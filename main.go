package main

import (
	"log"
	database "rbi_backend/db"
	handlers "rbi_backend/handlers/AO"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
)

func main() {
	app := fiber.New()

	// Enable CORS for all routes
	app.Use(cors.New(cors.Config{
		AllowOrigins: "*",
		AllowMethods: "GET,POST,PUT,DELETE",
	}))

	// Connect to the database
	if err := database.Connect(); err != nil {
		log.Fatalf("Could not connect to the database: %v", err)
	}

	// Group for dashboard routes
	dashboardRoutes := app.Group("/AO-dashboard")
	dashboardRoutes.Get("/", handlers.GetTotalCountsClient)
	dashboardRoutes.Get("/Total-loans", handlers.GetLoanAccountTotals)
	dashboardRoutes.Get("/age-group", handlers.GetAgeGroupCounts)
	dashboardRoutes.Get("/capital", handlers.GetCapitalBuildUp)
	dashboardRoutes.Get("/products-count", handlers.GetProductCounts)
	dashboardRoutes.Get("/center-summary", handlers.GetCenterSummary)
	dashboardRoutes.Get("/weekly-client-count", handlers.GetWeeklyCustomerCount)
	dashboardRoutes.Get("/weekly-capital-build", handlers.GetWeeklyCapitalBuildUp)
	dashboardRoutes.Get("/clients-report", handlers.GetClients)

	// Start the server
	if err := app.Listen(":8080"); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
