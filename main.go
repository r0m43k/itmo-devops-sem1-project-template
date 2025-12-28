package main

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
	"os"

	_ "github.com/lib/pq"
)

func main() {
	dbHost := getenv("POSTGRES_HOST", "localhost")
	dbPort := getenv("POSTGRES_PORT", "5432")
	dbName := getenv("POSTGRES_DB", "project-sem-1")
	dbUser := getenv("POSTGRES_USER", "validator")
	dbPass := getenv("POSTGRES_PASSWORD", "val1dat0r")

	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		dbHost, dbPort, dbUser, dbPass, dbName)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	http.HandleFunc("/api/v0/prices", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			handlePost(db, w, r)
			return
		}
		if r.Method == http.MethodGet {
			handleGet(db, w, r)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})

	log.Println("listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func handlePost(db *sql.DB, w http.ResponseWriter, r *http.Request) {
	archiveType := r.URL.Query().Get("type")
	if archiveType == "" {
		archiveType = "zip"
	}

	_ = r.ParseMultipartForm(32 << 20)
	f, _, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "no file", 400)
		return
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		http.Error(w, "bad file", 400)
		return
	}

	csvData := readCSVFromArchive(data, archiveType)
	if len(csvData) == 0 {
		http.Error(w, "empty csv", 400)
		return
	}

	cr := csv.NewReader(bytes.NewReader(csvData))
	rows, err := cr.ReadAll()
	if err != nil || len(rows) == 0 {
		http.Error(w, "bad csv", 400)
		return
	}

	totalCount := 0
	duplicatesCount := 0
	seen := make(map[string]bool)

	for i := 1; i < len(rows); i++ {
		totalCount++

		if len(rows[i]) < 5 {
			continue
		}

		idStr := strings.TrimSpace(rows[i][0])
		name := strings.TrimSpace(rows[i][1])
		cat := strings.TrimSpace(rows[i][2])
		priceStr := strings.TrimSpace(rows[i][3])
		dateStr := strings.TrimSpace(rows[i][4])

		if idStr == "" || name == "" || cat == "" || priceStr == "" || dateStr == "" {
			continue
		}

		id, e1 := strconv.Atoi(idStr)
		price, e2 := strconv.Atoi(priceStr)
		dt, e3 := time.Parse("2006-01-02", dateStr)

		if e1 != nil || e2 != nil || e3 != nil || price <= 0 {
			continue
		}

		key := fmt.Sprintf("%s|%s|%d|%s", name, cat, price, dateStr)

		if seen[key] {
			duplicatesCount++
			continue
		}
		seen[key] = true

		var exists int
		_ = db.QueryRow(
			"SELECT 1 FROM prices WHERE name=$1 AND category=$2 AND price=$3 AND create_date=$4 LIMIT 1",
			name, cat, price, dt,
		).Scan(&exists)

		if exists == 1 {
			duplicatesCount++
			continue
		}

		_, _ = db.Exec(
			"INSERT INTO prices VALUES ($1,$2,$3,$4,$5)",
			id, name, cat, price, dt,
		)
	}

	var items, cats, sum int
	_ = db.QueryRow(
		"SELECT COUNT(*), COUNT(DISTINCT category), COALESCE(SUM(price),0) FROM prices",
	).Scan(&items, &cats, &sum)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]int{
		"total_count":      totalCount,
		"duplicates_count": duplicatesCount,
		"total_items":      items,
		"total_categories": cats,
		"total_price":      sum,
	})
}

func handleGet(db *sql.DB, w http.ResponseWriter, r *http.Request) {
	start := r.URL.Query().Get("start")
	end := r.URL.Query().Get("end")
	min := r.URL.Query().Get("min")
	max := r.URL.Query().Get("max")

	q := "SELECT id,name,category,price,create_date FROM prices"
	args := []interface{}{}
	where := []string{}
	n := 1

	if start != "" {
		where = append(where, fmt.Sprintf("create_date >= $%d", n))
		args = append(args, start)
		n++
	}
	if end != "" {
		where = append(where, fmt.Sprintf("create_date <= $%d", n))
		args = append(args, end)
		n++
	}
	if min != "" {
		where = append(where, fmt.Sprintf("price >= $%d", n))
		args = append(args, min)
		n++
	}
	if max != "" {
		where = append(where, fmt.Sprintf("price <= $%d", n))
		args = append(args, max)
		n++
	}
	if len(where) > 0 {
		q = q + " WHERE " + strings.Join(where, " AND ")
	}

	rows, err := db.Query(q, args...)
	if err != nil {
		http.Error(w, "db error", 500)
		return
	}
	defer rows.Close()

	var buf bytes.Buffer
	cw := csv.NewWriter(&buf)
	_ = cw.Write([]string{"id", "name", "category", "price", "create_date"})

	for rows.Next() {
		var id, price int
		var name, cat string
		var d time.Time
		if err := rows.Scan(&id, &name, &cat, &price, &d); err != nil {
			continue
		}
		_ = cw.Write([]string{
			strconv.Itoa(id),
			name,
			cat,
			strconv.Itoa(price),
			d.Format("2006-01-02"),
		})
	}
	cw.Flush()

	w.Header().Set("Content-Type", "application/zip")
	zw := zip.NewWriter(w)
	f, _ := zw.Create("data.csv")
	_, _ = f.Write(buf.Bytes())
	_ = zw.Close()
}

func readCSVFromArchive(data []byte, archiveType string) []byte {
	if archiveType == "zip" {
		zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
		if err != nil || len(zr.File) == 0 {
			return nil
		}
		rc, err := zr.File[0].Open()
		if err != nil {
			return nil
		}
		defer rc.Close()
		b, _ := io.ReadAll(rc)
		return b
	}

	if archiveType == "tar" {
		tr := tar.NewReader(bytes.NewReader(data))

		for {
			h, err := tr.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				break
			}
			if h.Typeflag != tar.TypeReg {
				continue
			}
			if h.Name == "test_data.csv" || h.Name == "./test_data.csv" || h.Name == "data.csv" || h.Name == "./data.csv" {
				b, _ := io.ReadAll(tr)
				return b
			}
		}
	}

	return nil
}

func getenv(k, def string) string {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def
	}
	return v
}