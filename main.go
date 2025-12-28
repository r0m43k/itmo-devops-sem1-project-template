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
	"strconv"
	"time"

	_ "github.com/lib/pq"
)

func main() {
	db, err := sql.Open(
		"postgres",
		"host=localhost port=5432 user=validator password=val1dat0r dbname=project-sem-1 sslmode=disable",
	)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	http.HandleFunc("/api/v0/prices", func(w http.ResponseWriter, r *http.Request) {

		if r.Method == http.MethodPost {

			archiveType := r.URL.Query().Get("type")
			if archiveType == "" {
				archiveType = "zip"
			}

			r.ParseMultipartForm(32 << 20)
			f, _, err := r.FormFile("file")
			if err != nil {
				http.Error(w, "no file", 400)
				return
			}
			defer f.Close()

			data, _ := io.ReadAll(f)
			var csvData []byte

			if archiveType == "zip" {
				zr, _ := zip.NewReader(bytes.NewReader(data), int64(len(data)))
				rc, _ := zr.File[0].Open()
				csvData, _ = io.ReadAll(rc)
				rc.Close()
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
						csvData, _ = io.ReadAll(tr)
						break
					}
				}
			}


			cr := csv.NewReader(bytes.NewReader(csvData))
			rows, _ := cr.ReadAll()

			for i := 1; i < len(rows); i++ {
				if len(rows[i]) < 5 {
					continue
				}

				id, _ := strconv.Atoi(rows[i][0])
				price, _ := strconv.Atoi(rows[i][3])
				date, _ := time.Parse("2006-01-02", rows[i][4])

				db.Exec(
					"INSERT INTO prices VALUES ($1,$2,$3,$4,$5)",
					id, rows[i][1], rows[i][2], price, date,
				)
			}


			var items, cats, sum int
			db.QueryRow(
				"SELECT COUNT(*), COUNT(DISTINCT category), COALESCE(SUM(price),0) FROM prices",
			).Scan(&items, &cats, &sum)
			
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]int{
				"total_items":      items,
				"total_categories": cats,
				"total_price":      sum,
			})
			return
		}

		if r.Method == http.MethodGet {

			rows, _ := db.Query("SELECT * FROM prices")

			var buf bytes.Buffer
			cw := csv.NewWriter(&buf)
			cw.Write([]string{"id", "name", "category", "price", "create_date"})

			for rows.Next() {
				var id, price int
				var name, cat string
				var d time.Time
				rows.Scan(&id, &name, &cat, &price, &d)

				cw.Write([]string{
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
			f.Write(buf.Bytes())
			zw.Close()
			return
		}

		w.WriteHeader(http.StatusMethodNotAllowed)
	})

	log.Println("listening on :8080")
	http.ListenAndServe(":8080", nil)
}
