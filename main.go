package main

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

type PostResponse struct {
	TotalCount      int     `json:"total_count"`
	DuplicatesCount int     `json:"duplicates_count"`
	TotalItems      int     `json:"total_items"`
	TotalCategories int     `json:"total_categories"`
	TotalPrice      float64 `json:"total_price"`
	Inserted        int     `json:"inserted"`
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func dsn() string {
	return "host=" + getenv("POSTGRES_HOST", "localhost") +
		" port=" + getenv("POSTGRES_PORT", "5432") +
		" user=" + getenv("POSTGRES_USER", "validator") +
		" password=" + getenv("POSTGRES_PASSWORD", "val1dat0r") +
		" dbname=" + getenv("POSTGRES_DB", "project-sem-1") +
		" sslmode=disable"
}

func connectWithRetry() (*sql.DB, error) {
	db, err := sql.Open("postgres", dsn())
	if err != nil {
		return nil, err
	}

	deadline := time.Now().Add(30 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err = db.PingContext(ctx)
		cancel()
		if err == nil {
			return db, nil
		}
		if time.Now().After(deadline) {
			_ = db.Close()
			return nil, err
		}
		time.Sleep(1 * time.Second)
	}
}

func mustInitSchema(db *sql.DB) {
	_, err := db.Exec(`
CREATE TABLE IF NOT EXISTS prices (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  category VARCHAR(255) NOT NULL,
  price DECIMAL(10,2) NOT NULL,
  create_date TIMESTAMP NOT NULL
);
`)
	if err != nil {
		log.Fatal(err)
	}
}

func readMultipartFile(r *http.Request) ([]byte, error) {
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		return nil, err
	}
	f, _, err := r.FormFile("file")
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(f)
}

func readCSVFromZip(b []byte) ([]byte, error) {
	zr, err := zip.NewReader(bytes.NewReader(b), int64(len(b)))
	if err != nil {
		return nil, err
	}
	if len(zr.File) == 0 {
		return nil, io.EOF
	}
	rc, err := zr.File[0].Open()
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

func readCSVFromTar(b []byte) ([]byte, error) {
	tr := tar.NewReader(bytes.NewReader(b))
	for {
		h, err := tr.Next()
		if err == io.EOF {
			return nil, io.EOF
		}
		if err != nil {
			return nil, err
		}
		if h.Typeflag != tar.TypeReg {
			continue
		}
		name := strings.TrimPrefix(h.Name, "./")
		if name == "test_data.csv" || name == "data.csv" {
			return io.ReadAll(tr)
		}
	}
}

func parseDate(s string) (time.Time, bool) {
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

func parsePrice(s string) (float64, bool) {
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false
	}
	if v <= 0 {
		return 0, false
	}
	return v, true
}

type rowData struct {
	name string
	cat  string
	pr   float64
	dt   time.Time
}

func validateCSV(csvBytes []byte) (totalCount int, valid []rowData) {
	cr := csv.NewReader(bytes.NewReader(csvBytes))
	rows, err := cr.ReadAll()
	if err != nil || len(rows) <= 1 {
		return 0, nil
	}

	valid = make([]rowData, 0, len(rows)-1)

	for i := 1; i < len(rows); i++ {
		totalCount++

		if len(rows[i]) < 5 {
			continue
		}

		// id из CSV проверяем на int, но в БД НЕ вставляем (id SERIAL)
		if _, err := strconv.Atoi(strings.TrimSpace(rows[i][0])); err != nil {
			continue
		}

		name := strings.TrimSpace(rows[i][1])
		cat := strings.TrimSpace(rows[i][2])
		if name == "" || cat == "" {
			continue
		}

		pr, ok := parsePrice(strings.TrimSpace(rows[i][3]))
		if !ok {
			continue
		}

		dt, ok := parseDate(strings.TrimSpace(rows[i][4]))
		if !ok {
			continue
		}

		valid = append(valid, rowData{name: name, cat: cat, pr: pr, dt: dt})
	}

	return totalCount, valid
}

func insertValidRowsTx(db *sql.DB, rows []rowData) (inserted int, err error) {
	tx, err := db.Begin()
	if err != nil {
		return 0, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	stmt, err := tx.Prepare(`INSERT INTO prices (name, category, price, create_date) VALUES ($1,$2,$3,$4)`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	for _, r := range rows {
		_, e := stmt.Exec(r.name, r.cat, r.pr, r.dt)
		if e != nil {
			err = e
			return 0, err
		}
		inserted++
	}

	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return inserted, nil
}

func statsAll(db *sql.DB) (items, cats int, sum float64, err error) {
	err = db.QueryRow(
		`SELECT COUNT(*), COUNT(DISTINCT category), COALESCE(SUM(price),0) FROM prices`,
	).Scan(&items, &cats, &sum)
	return
}

func duplicatesAll(db *sql.DB) (int, error) {
	var d int
	err := db.QueryRow(`
SELECT COALESCE(COUNT(*),0) FROM (
  SELECT name, category, price, create_date, COUNT(*) c
  FROM prices
  GROUP BY name, category, price, create_date
  HAVING COUNT(*) > 1
) t;
`).Scan(&d)
	return d, err
}

func handlePOST(db *sql.DB, w http.ResponseWriter, r *http.Request) {
	archiveType := r.URL.Query().Get("type")
	if archiveType == "" {
		archiveType = "zip"
	}

	fileBytes, err := readMultipartFile(r)
	if err != nil {
		http.Error(w, "no file", http.StatusBadRequest)
		return
	}

	var csvBytes []byte
	switch archiveType {
	case "zip":
		csvBytes, err = readCSVFromZip(fileBytes)
	case "tar":
		csvBytes, err = readCSVFromTar(fileBytes)
	default:
		http.Error(w, "bad type", http.StatusBadRequest)
		return
	}

	if err != nil {
		http.Error(w, "bad archive", http.StatusBadRequest)
		return
	}
	if len(csvBytes) == 0 {
		http.Error(w, "empty csv", http.StatusBadRequest)
		return
	}

	totalCount, validRows := validateCSV(csvBytes)

	inserted, err := insertValidRowsTx(db, validRows)
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}

	items, cats, sum, err := statsAll(db)
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}

	dup, err := duplicatesAll(db)
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(PostResponse{
		TotalCount:      totalCount,
		DuplicatesCount: dup,
		TotalItems:      items,
		TotalCategories: cats,
		TotalPrice:      sum,
		Inserted:        inserted,
	})
}

func handleGET(db *sql.DB, w http.ResponseWriter, r *http.Request) {
	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")
	minStr := r.URL.Query().Get("min")
	maxStr := r.URL.Query().Get("max")

	where := make([]string, 0, 4)
	args := make([]any, 0, 4)
	argN := 1

	if startStr != "" {
		if t, ok := parseDate(startStr); ok {
			where = append(where, "create_date >= $"+strconv.Itoa(argN))
			args = append(args, t)
			argN++
		}
	}
	if endStr != "" {
		if t, ok := parseDate(endStr); ok {
			t = t.Add(24*time.Hour - time.Nanosecond)
			where = append(where, "create_date <= $"+strconv.Itoa(argN))
			args = append(args, t)
			argN++
		}
	}
	if minStr != "" {
		if v, err := strconv.ParseFloat(minStr, 64); err == nil {
			where = append(where, "price >= $"+strconv.Itoa(argN))
			args = append(args, v)
			argN++
		}
	}
	if maxStr != "" {
		if v, err := strconv.ParseFloat(maxStr, 64); err == nil {
			where = append(where, "price <= $"+strconv.Itoa(argN))
			args = append(args, v)
			argN++
		}
	}

	q := "SELECT id, name, category, price, create_date FROM prices"
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	q += " ORDER BY id"

	rows, err := db.Query(q, args...)
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	type outRow struct {
		id   int
		name string
		cat  string
		pr   float64
		dt   time.Time
	}

	out := make([]outRow, 0, 256)
	for rows.Next() {
		var rr outRow
		if err := rows.Scan(&rr.id, &rr.name, &rr.cat, &rr.pr, &rr.dt); err != nil {
			continue
		}
		out = append(out, rr)
	}
	if err := rows.Err(); err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}

	var buf bytes.Buffer
	cw := csv.NewWriter(&buf)
	_ = cw.Write([]string{"id", "name", "category", "price", "create_date"})
	for _, rr := range out {
		priceStr := strconv.FormatFloat(rr.pr, 'f', -1, 64)
		_ = cw.Write([]string{
			strconv.Itoa(rr.id),
			rr.name,
			rr.cat,
			priceStr,
			rr.dt.Format("2006-01-02"),
		})
	}
	cw.Flush()
	if err := cw.Error(); err != nil {
		http.Error(w, "csv error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/zip")
	zw := zip.NewWriter(w)
	defer zw.Close()

	f, err := zw.Create("data.csv")
	if err != nil {
		http.Error(w, "zip error", http.StatusInternalServerError)
		return
	}
	if _, err := f.Write(buf.Bytes()); err != nil {
		http.Error(w, "zip error", http.StatusInternalServerError)
		return
	}
}

func main() {
	db, err := connectWithRetry()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	mustInitSchema(db)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v0/prices", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			handlePOST(db, w, r)
		case http.MethodGet:
			handleGET(db, w, r)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})

	srv := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Println("listening on :8080")
	err = srv.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}
