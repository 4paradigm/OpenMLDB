package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	// MySQL database connection parameters
	dbUser := "root"         // Replace with your MySQL username
	dbPass := "root"         // Replace with your MySQL password
	dbName := "db1"    // Replace with your MySQL database name
	dbHost := "localhost:3307"        // Replace with your MySQL host address
	dbCharset := "utf8mb4"            // Replace with your MySQL charset

	// Create a database connection
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=%s", dbUser, dbPass, dbHost, dbName, dbCharset))
	if err != nil {
		log.Fatalf("Error connecting to the database: %v", err)
	}
	defer db.Close()

	// Perform a simple query
	rows, err := db.Query("SELECT id, name FROM db1.t1")
	if err != nil {
		log.Fatalf("Error executing query: %v", err)
	}
	defer rows.Close()

	// Iterate over the result set
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			log.Fatalf("Error scanning row: %v", err)
		}
		fmt.Printf("ID: %d, Name: %s\n", id, name)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("Error iterating over result set: %v", err)
	}
}
