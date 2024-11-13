package database

import (
	"log"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// DB is the database instance
var DB *gorm.DB

// Connect initializes the database connection
func Connect() error {
	var err error
	// Adjust the DSN string with your actual database credentials and details
	dsn := "host=data-platform-db.fortress-asya.com user=postgres password=P@ssw0rd1qaz2wsx3edc dbname=rbi_streamingdb port=5432 sslmode=disable" // Update this line

	// Open a connection to the database
	DB, err = gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatal("Failed to connect to the database:", err)
		return err // Return the error for further handling if needed
	}

	log.Println("Database connection established successfully!")
	return nil
}
