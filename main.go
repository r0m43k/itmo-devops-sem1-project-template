package main

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

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
  id INTEGER NOT NULL,
  name TEXT NOT NULL,
  category TEXT NOT NULL,
  price INTEGER NOT NULL,
  create_date DATE NOT NULL
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
	if err != nil || len(zr.File) == 0 {
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
	v, err := strconv.Atoi(s)
	return v, err == nil
}

func parseDate(s string) (time.Time, bool) {
	t, err := time.Parse("2006-01-02", s)
	return t, err == nil
}


func insertRow(db *sql.DB, id int, name, cat string, price int, d time.Time) error {
	_, err := db.Exec(
		`INSERT INTO prices (id,name,category,price,create_date)
		 VALUES ($1,$2,$3,$4,$5)`,
		id, name, cat, price, d,
	)
	return err
}

func parseAndInsertCSV(db *sql.DB, csvBytes []byte) (totalCount int, inserted int) {
	cr := csv.NewReader(bytes.NewReader(csvBytes))
	rows, err := cr.ReadAll()
	if err != nil || len(rows) <= 1 {
		return
	}

	for i := 1; i < len(rows); i++ {
		totalCount++

		if len(rows[i]) < 5 {
			continue
		}

		id, ok := parseInt(rows[i][0])
		if !ok {
			continue
		}

		name := strings.TrimSpace(rows[i][1])
		cat := strings.TrimSpace(rows[i][2])

		price, ok := parseInt(rows[i][3])
		if !ok {
			continue
		}

		d, ok := parseDate(rows[i][4])
		if !ok {
			continue
		}

		if name == "" || cat == "" {
			continue
		}

		if insertRow(db, id, name, cat, price, d) == nil {
			inserted++
		}
	}
	return
}

func stats(db *sql.DB) (items, cats, sum int) {
	_ = db.QueryRow(
		`SELECT COUNT(*), COUNT(DISTINCT category), COALESCE(SUM(price),0) FROM prices`,
	).Scan(&items, &cats, &sum)
	return
}

func duplicates(db *sql.DB) int {
	var d int
	_ = db.QueryRow(`
SELECT COALESCE(COUNT(*),0) FROM (
  SELECT name, category, price, create_date, COUNT(*) c
  FROM prices
  GROUP BY name, category, price, create_date
  HAVING COUNT(*) > 1
) t;
`).Scan(&d)
	return d
}

func handlePOST(db *sql.DB, w http.ResponseWriter, r *http.Request) {
	archiveType := r.URL.Query().Get("type")
	if archiveType == "" {
		archiveType = "zip"
	}

	fileBytes, err := readMultipartFile(r)
	if err != nil {
		http.Error(w, "no file", 400)
		return
	}

	var csvBytes []byte
	if archiveType == "zip" {
		csvBytes, err = readCSVFromZip(fileBytes)
	} else if archiveType == "tar" {
		csvBytes, err = readCSVFromTar(fileBytes)
	} else {
		http.Error(w, "bad type", 400)
		return
	}

	if err != nil || len(csvBytes) == 0 {
		http.Error(w, "bad archive", 400)
		return
	}

	_, _ = db.Exec("TRUNCATE prices")

	totalCount, inserted := parseAndInsertCSV(db, csvBytes)
	items, cats, sum := stats(db)
	dup := duplicates(db)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]int{
		"total_count":      totalCount,
		"total_items":      items,
		"total_categories": cats,
		"total_price":      sum,
		"duplicates_count": dup,
		"inserted":         inserted,
	})
}

func handleGET(db *sql.DB, w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query(`SELECT id,name,category,price,create_date FROM prices ORDER BY id`)
	if err != nil {
		http.Error(w, "db error", 500)
		return
	}
	defer rows.Close()

	var buf bytes.Buffer
	cw := csv.NewWriter(&buf)
	cw.Write([]string{"id", "name", "category", "price", "create_date"})

	for rows.Next() {
		var id, price int
		var name, cat string
		var d time.Time
		if rows.Scan(&id, &name, &cat, &price, &d) == nil {
			cw.Write([]string{
				strconv.Itoa(id),
				name,
				cat,
				strconv.Itoa(price),
				d.Format("2006-01-02"),
			})
		}
	}
	cw.Flush()

	w.Header().Set("Content-Type", "application/zip")
	zw := zip.NewWriter(w)
	f, _ := zw.Create("data.csv")
	f.Write(buf.Bytes())
	zw.Close()
}

func main() {
	db, err := sql.Open("postgres", dsn())
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	mustInitSchema(db)

	http.HandleFunc("/api/v0/prices", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			handlePOST(db, w, r)
			return
		}
		if r.Method == http.MethodGet {
			handleGET(db, w, r)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})

	log.Println("listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
