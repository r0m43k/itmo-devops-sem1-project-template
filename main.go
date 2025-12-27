package main

import (
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

		if r.Method == "POST" {

			r.ParseMultipartForm(32 << 20)
			file, _, _ := r.FormFile("file")
			defer file.Close()

			zipBytes, _ := io.ReadAll(file)

			zr, _ := zip.NewReader(bytes.NewReader(zipBytes), int64(len(zipBytes)))
			zf, _ := zr.File[0].Open()
			csvBytes, _ := io.ReadAll(zf)
			zf.Close()

			cr := csv.NewReader(bytes.NewReader(csvBytes))
			rows, _ := cr.ReadAll()

			for i := 1; i < len(rows); i++ {
				id, _ := strconv.Atoi(rows[i][0])
				price, _ := strconv.Atoi(rows[i][3])
				date, _ := time.Parse("2006-01-02", rows[i][4])

				db.Exec(
					"INSERT INTO prices VALUES ($1,$2,$3,$4,$5)",
					id, rows[i][1], rows[i][2], price, date,
				)
			}

			var totalItems, totalCategories, totalPrice int
			db.QueryRow(
				"SELECT COUNT(*), COUNT(DISTINCT category), COALESCE(SUM(price),0) FROM prices",
			).Scan(&totalItems, &totalCategories, &totalPrice)

			json.NewEncoder(w).Encode(map[string]int{
				"total_items":      totalItems,
				"total_categories": totalCategories,
				"total_price":      totalPrice,
			})
			return
		}

		if r.Method == "GET" {

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
