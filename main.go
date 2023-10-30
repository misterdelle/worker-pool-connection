package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
	"io"
	"log"
	"math"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	dbMaxIdleConns = 4
	dbMaxConns     = 100
	totalWorker    = 100
	csvFile        = "majestic_million.csv"
)

var (
	dbUsername   = os.Getenv("DB_USERNAME")
	dbName       = os.Getenv("DB_NAME")
	dbConnString = fmt.Sprintf("%s@tcp(localhost:8080)/%s", dbUsername, dbName)
	dataHeaders  []string
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal(err.Error())
	}

	start := time.Now()

	db, err := OpenDBConnection()
	if err != nil {
		log.Fatal(err.Error())
	}
	defer db.Close()

	csvReader, csvFile, err := OpenCSVFile()
	if err != nil {
		log.Fatal(err.Error())
	}
	defer csvFile.Close()

	jobs := make(chan []interface{}, 0)
	var wg sync.WaitGroup

	go DispatchWorkers(db, jobs, &wg)
	ProcessCSVFileWithWorker(csvReader, jobs, &wg)

	wg.Wait()

	duration := time.Since(start)
	log.Printf("Done in %d seconds", int(math.Ceil(duration.Seconds())))
}

func OpenDBConnection() (*sql.DB, error) {
	log.Println("Open DB connection")

	db, err := sql.Open("mysql", dbConnString)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(dbMaxConns)
	db.SetMaxIdleConns(dbMaxIdleConns)

	return db, nil
}

func OpenCSVFile() (*csv.Reader, *os.File, error) {
	log.Println("Open CSV file")

	file, err := os.Open(csvFile)
	if err != nil {
		return nil, nil, err
	}

	reader := csv.NewReader(file)
	return reader, file, nil
}

func DispatchWorkers(db *sql.DB, jobs <-chan []interface{}, wg *sync.WaitGroup) {
	for workerIndex := 0; workerIndex <= totalWorker; workerIndex++ {
		go func(workerIndex int, db *sql.DB, jobs <-chan []interface{}, wg *sync.WaitGroup) {
			counter := 0

			for job := range jobs {
				DoTheJob(workerIndex, counter, db, job)
				wg.Done()
				counter++
			}

		}(workerIndex, db, jobs, wg)
	}
}

func ProcessCSVFileWithWorker(reader *csv.Reader, jobs chan<- []interface{}, wg *sync.WaitGroup) {
	for {
		row, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}

		if len(dataHeaders) == 0 {
			dataHeaders = row
			continue
		}

		rowOrdered := make([]interface{}, len(row))
		for i, v := range row {
			rowOrdered[i] = v
		}

		wg.Add(1)
		jobs <- rowOrdered
	}
	close(jobs)
}

func DoTheJob(workerIndex, counter int, db *sql.DB, values []interface{}) {
	for {
		conn, err := db.Conn(context.Background())
		query := fmt.Sprintf("INSERT INTO domain (%s) VALUES (%s)",
			strings.Join(dataHeaders, ","),
			strings.Join(generateQuestionsMark(len(dataHeaders)), ","),
		)

		_, err = conn.ExecContext(context.Background(), query, values...)
		if err != nil {
			log.Fatal(err.Error())
		}

		err = conn.Close()
		if err != nil {
			log.Fatal(err.Error())
		}

		if counter%100 == 0 {
			log.Printf("=> Worker %d inserted %d data\n", workerIndex, counter)
		}
	}
}

func generateQuestionsMark(n int) []string {
	return make([]string, n)
}
