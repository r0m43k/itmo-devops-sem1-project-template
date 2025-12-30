package main

import (
	"archive/tar"
	"archive/zip"
	"bytes"
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

type Response struct {
	TotalCount      int `json:"total_count"`
	DuplicatesCount int `json:"duplicates_count"`
	TotalItems      int `json:"total_items"`
	TotalCategories int `json:"total_categories"`
	TotalPrice      int `json:"total_price"`
}

type Row struct {
	ProductID int
	Name      string
	Category  string
	Price     int
	Date      time.Time
	Key       string
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

func mustInitSchema(db *sql.DB) {
	_, err := db.Exec(`
CREATE TABLE IF NOT EXISTS prices (
  id SERIAL PRIMARY KEY,
  product_id INTEGER NOT NULL,
  name VARCHAR(255) NOT NULL,
  category VARCHAR(255) NOT NULL,
  price NUMERIC(10,2) NOT NULL,
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

func parseInt(s string) (int, bool) {
	v, err := strconv.Atoi(strings.TrimSpace(s))
	return v, err == nil
}

func parseDateYYYYMMDD(s string) (time.Time, bool) {
	t, err := time.Parse("2006-01-02", strings.TrimSpace(s))
	return t, err == nil
}

func makeKey(name, cat string, price int, d time.Time) string {
	return name + "\x1f" + cat + "\x1f" + strconv.Itoa(price) + "\x1f" + d.Format("2006-01-02")
}

func parseAndValidateCSV(csvBytes []byte) (totalCount int, rows []Row) {
	cr := csv.NewReader(bytes.NewReader(csvBytes))
	cr.FieldsPerRecord = -1

	all, err := cr.ReadAll()
	if err != nil || len(all) <= 1 {
		return 0, nil
	}

	for i := 1; i < len(all); i++ {
		totalCount++
		rec := all[i]
		if len(rec) < 5 {
			continue
		}

		pid, ok := parseInt(rec[0])
		if !ok {
			continue
		}

		d, ok := parseDateYYYYMMDD(rec[1])
		if !ok {
			continue
		}

		name := strings.TrimSpace(rec[2])
		cat := strings.TrimSpace(rec[3])

		price, ok := parseInt(rec[4])
		if !ok || price <= 0 {
			continue
		}

		if name == "" || cat == "" {
			continue
		}

		rows = append(rows, Row{
			ProductID: pid,
			Name:      name,
			Category:  cat,
			Price:     price,
			Date:      d,
			Key:       makeKey(name, cat, price, d),
		})
	}

	return totalCount, rows
}

func insertRowsTx(tx *sql.Tx, rows []Row) error {
	stmt, err := tx.Prepare(`
INSERT INTO prices (product_id, name, category, price, create_date)
VALUES ($1,$2,$3,$4,$5)
`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, r := range rows {
		if _, err := stmt.Exec(r.ProductID, r.Name, r.Category, r.Price, r.Date); err != nil {
			return err
		}
	}
	return nil
}

func duplicatesTx(tx *sql.Tx) (int, error) {
	var dup int
	err := tx.QueryRow(`
SELECT COALESCE(SUM(c-1),0) FROM (
  SELECT COUNT(*) c
  FROM prices
  GROUP BY name, category, price, create_date
  HAVING COUNT(*) > 1
) t;
`).Scan(&dup)
	return dup, err
}

func statsTx(tx *sql.Tx) (items, cats, sum int, err error) {
	err = tx.QueryRow(`
SELECT
  COUNT(*)::int,
  COUNT(DISTINCT category)::int,
  COALESCE(SUM(price)::bigint,0)::int
FROM prices
`).Scan(&items, &cats, &sum)
	return
}

func handlePOST(db *sql.DB, w http.ResponseWriter, r *http.Request) {
	archiveType := r.URL.Query().Get("type")
	if archiveType == "" {
		archiveType = "zip"
	}

	fileBytes, err := readMultipartFile(r)
	if err != nil {
		http.Error(w, "missing multipart file 'file'", http.StatusBadRequest)
		return
	}

	var csvBytes []byte
	switch archiveType {
	case "zip":
		csvBytes, err = readCSVFromZip(fileBytes)
	case "tar":
		csvBytes, err = readCSVFromTar(fileBytes)
	default:
		http.Error(w, "unsupported archive type", http.StatusBadRequest)
		return
	}

	if err != nil {
		http.Error(w, "cannot read archive", http.StatusBadRequest)
		return
	}
	if len(csvBytes) == 0 {
		http.Error(w, "empty csv", http.StatusBadRequest)
		return
	}

	totalCount, validRows := parseAndValidateCSV(csvBytes)

	tx, err := db.Begin()
	if err != nil {
		http.Error(w, "cannot start transaction", http.StatusInternalServerError)
		return
	}
	defer func() { _ = tx.Rollback() }()

	if err := insertRowsTx(tx, validRows); err != nil {
		http.Error(w, "db insert failed", http.StatusInternalServerError)
		return
	}

	dup, err := duplicatesTx(tx)
	if err != nil {
		http.Error(w, "duplicates query failed", http.StatusInternalServerError)
		return
	}

	items, cats, sum, err := statsTx(tx)
	if err != nil {
		http.Error(w, "stats query failed", http.StatusInternalServerError)
		return
	}

	if err := tx.Commit(); err != nil {
		http.Error(w, "commit failed", http.StatusInternalServerError)
		return
	}

	resp := Response{
		TotalCount:      totalCount,
		DuplicatesCount: dup,
		TotalItems:      items,
		TotalCategories: cats,
		TotalPrice:      sum,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func handleGET(db *sql.DB, w http.ResponseWriter, r *http.Request) {
	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")
	minStr := r.URL.Query().Get("min")
	maxStr := r.URL.Query().Get("max")

	where := []string{}
	args := []any{}
	argN := 1

	if startStr != "" {
		if _, ok := parseDateYYYYMMDD(startStr); ok {
			where = append(where, "create_date >= $"+strconv.Itoa(argN))
			args = append(args, startStr)
			argN++
		}
	}
	if endStr != "" {
		if _, ok := parseDateYYYYMMDD(endStr); ok {
			where = append(where, "create_date <= $"+strconv.Itoa(argN))
			args = append(args, endStr)
			argN++
		}
	}
	if minStr != "" {
		if v, ok := parseInt(minStr); ok && v > 0 {
			where = append(where, "price >= $"+strconv.Itoa(argN))
			args = append(args, v)
			argN++
		}
	}
	if maxStr != "" {
		if v, ok := parseInt(maxStr); ok && v > 0 {
			where = append(where, "price <= $"+strconv.Itoa(argN))
			args = append(args, v)
			argN++
		}
	}

	q := `
SELECT product_id AS id, create_date, name, category, price
FROM prices
`
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	q += " ORDER BY id"

	rows, err := db.Query(q, args...)
	if err != nil {
		http.Error(w, "db query failed", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var buf bytes.Buffer
	cw := csv.NewWriter(&buf)
	_ = cw.Write([]string{"id", "create_date", "name", "category", "price"})

	for rows.Next() {
		var pid int
		var d time.Time
		var name, cat string
		var priceStr string

		if err := rows.Scan(&pid, &d, &name, &cat, &priceStr); err != nil {
			continue
		}

		if strings.Contains(priceStr, ".") {
			priceStr = strings.TrimRight(strings.TrimRight(priceStr, "0"), ".")
		}

		_ = cw.Write([]string{
			strconv.Itoa(pid),
			d.Format("2006-01-02"),
			name,
			cat,
			priceStr,
		})
	}
	cw.Flush()

	if err := rows.Err(); err != nil {
		http.Error(w, "db rows error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/zip")
	zw := zip.NewWriter(w)
	defer func() { _ = zw.Close() }()

	f, err := zw.Create("data.csv")
	if err != nil {
		http.Error(w, "zip create failed", http.StatusInternalServerError)
		return
	}
	if _, err := f.Write(buf.Bytes()); err != nil {
		http.Error(w, "zip write failed", http.StatusInternalServerError)
		return
	}
}

func main() {
	db, err := sql.Open("postgres", dsn())
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	mustInitSchema(db)

	http.HandleFunc("/api/v0/prices", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			handlePOST(db, w, r)
		case http.MethodGet:
			handleGET(db, w, r)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})

	port := getenv("APP_PORT", "8080")
	log.Println("listening on :" + port)
	err = http.ListenAndServe(":"+port, nil)
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}
