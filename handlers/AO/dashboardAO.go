package handlers

import (
	"fmt"
	"log"
	database "rbi_backend/db"

	"github.com/gofiber/fiber/v2"
)

// Result represents the output format
type Result struct {
	Particulars string `json:"particulars"`
	Count       int    `json:"count"`
}

// GetTotalValues handles the request to get the counts and totals of customer information
func GetTotalCountsClient(c *fiber.Ctx) error {
	var results []Result

	accountOfficer := c.Query("account_officer")
	startDate := c.Query("start_date")
	endDate := c.Query("end_date")

	if accountOfficer == "" || startDate == "" || endDate == "" {
		return c.Status(400).JSON(fiber.Map{"error": "Missing required parameters"})
	}

	query := `
		SELECT 
			ci.member_status AS "Particulars",
			count(ci.t_id) AS "Count"
		FROM 
			public.customer_info ci 
		WHERE 
			ci.account_officer = ? AND 
			ci.l_date_recog BETWEEN ? AND ?
		GROUP BY 
			ci.member_status 
		UNION ALL
		SELECT 
			'Total Client' AS "Particulars",
			Count(*) AS "Count"
		FROM 
			public.customer_info ci 
		WHERE 
			ci.account_officer = ? AND 
			ci.l_date_recog BETWEEN ? AND ?
	`

	err := database.DB.Raw(query, accountOfficer, startDate, endDate, accountOfficer, startDate, endDate).Scan(&results).Error
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to get total values"})
	}

	return c.JSON(results)
}

// LoanAccountResult represents the output format for the loan account query
type LoanAccountResult struct {
	Particulars string  `json:"particulars"`
	Count       int     `json:"count"`
	Amount      float64 `json:"amount"`
}

// GetLoanAccountTotals handles the request to get loan account details for a specified officer and date range
func GetLoanAccountTotals(c *fiber.Ctx) error {
	var results []LoanAccountResult

	accountOfficer := c.Query("account_officer")
	startDate := c.Query("start_date")
	endDate := c.Query("end_date")

	if accountOfficer == "" || startDate == "" || endDate == "" {
		return c.Status(400).JSON(fiber.Map{"error": "Missing required parameters"})
	}

	query := `
		SELECT 
			la.bill_type AS "Particulars",
			count(la.bill_type) AS "Count",
			sum(la.online_actual_bal::numeric) * -1 AS "Amount"
		FROM 
			public.loan_acct la
		WHERE 
			la.account_officer = ? AND 
			la.opening_date::date BETWEEN ? AND ?
		GROUP BY 
			la.bill_type
	`

	err := database.DB.Raw(query, accountOfficer, startDate, endDate).Scan(&results).Error
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to get loan account totals"})
	}

	return c.JSON(results)
}

// CapitalBuildUpResult represents the output format for the capital build-up query
type CapitalBuildUpResult struct {
	Title        string  `json:"title"`
	TotalCapital float64 `json:"total_capital"`
}

// GetCapitalBuildUp handles the request to get the capital build-up total for a specified officer and date range
func GetCapitalBuildUp(c *fiber.Ctx) error {
	var result []CapitalBuildUpResult

	accountOfficer := c.Query("account_officer")
	startDate := c.Query("start_date")
	endDate := c.Query("end_date")

	if accountOfficer == "" || startDate == "" || endDate == "" {
		return c.Status(400).JSON(fiber.Map{"error": "Missing required parameters"})
	}

	query := `SELECT * FROM get_capital_build_up(?, ?, ?);`

	// Execute the query with the parameters
	err := database.DB.Debug().Raw(query, accountOfficer, startDate, endDate).Find(&result).Error
	if err != nil {
		log.Printf("SQL Error: %v", err)
		return c.Status(500).JSON(fiber.Map{"error": "Failed to get capital build-up total"})
	}

	return c.JSON(result)
}

// AgeGroupCount represents the output format for the age group counts query
type AgeGroupCount struct {
	Age18_29  int `json:"age_18_29"`
	Age30_39  int `json:"age_30_39"`
	Age40_49  int `json:"age_40_49"`
	Age50_59  int `json:"age_50_59"`
	Age60_69  int `json:"age_60_69"`
	Age70_79  int `json:"age_70_79"`
	Age80Plus int `json:"age_80_plus"`
	Total     int `json:"total"`
}

// GetAgeGroupCounts handles the request to get age group counts for a specified officer and date range
func GetAgeGroupCounts(c *fiber.Ctx) error {
	var result AgeGroupCount

	accountOfficer := c.Query("account_officer")
	startDate := c.Query("start_date")
	endDate := c.Query("end_date")

	if accountOfficer == "" || startDate == "" || endDate == "" {
		return c.Status(400).JSON(fiber.Map{"error": "Missing required parameters"})
	}

	fmt.Printf("Parameters: accountOfficer=%s, startDate=%s, endDate=%s\n", accountOfficer, startDate, endDate)

	query := `
		SELECT 
			COUNT(CASE WHEN EXTRACT(YEAR FROM AGE(ci.date_of_birth)) BETWEEN 18 AND 29 THEN 1 END) AS "Age 18-29",
			COUNT(CASE WHEN EXTRACT(YEAR FROM AGE(ci.date_of_birth)) BETWEEN 30 AND 39 THEN 1 END) AS "Age 30-39",
			COUNT(CASE WHEN EXTRACT(YEAR FROM AGE(ci.date_of_birth)) BETWEEN 40 AND 49 THEN 1 END) AS "Age 40-49",
			COUNT(CASE WHEN EXTRACT(YEAR FROM AGE(ci.date_of_birth)) BETWEEN 50 AND 59 THEN 1 END) AS "Age 50-59",
			COUNT(CASE WHEN EXTRACT(YEAR FROM AGE(ci.date_of_birth)) BETWEEN 60 AND 69 THEN 1 END) AS "Age 60-69",
			COUNT(CASE WHEN EXTRACT(YEAR FROM AGE(ci.date_of_birth)) BETWEEN 70 AND 79 THEN 1 END) AS "Age 70-79",
			COUNT(CASE WHEN EXTRACT(YEAR FROM AGE(ci.date_of_birth)) >= 80 THEN 1 END) AS "Age 80+",
			COUNT(*) AS "TOTAL"
		FROM 
			public.customer_info ci 
		WHERE 
			ci.account_officer = ? AND 
			ci.l_date_recog BETWEEN ? AND ?
	`

	row := database.DB.Raw(query, accountOfficer, startDate, endDate).Row()
	err := row.Scan(&result.Age18_29, &result.Age30_39, &result.Age40_49, &result.Age50_59, &result.Age60_69, &result.Age70_79, &result.Age80Plus, &result.Total)
	if err != nil {
		log.Printf("SQL Error: %v", err)
		return c.Status(500).JSON(fiber.Map{"error": "Failed to get age group counts"})
	}

	return c.JSON(result)
}

// ProductCount represents the output format for the product count query
type ProductCount struct {
	ProductName string `json:"product_name"`
	Count       int    `json:"count"`
}

// GetProductCounts handles the request to get loan product counts for a specified officer and date range
func GetProductCounts(c *fiber.Ctx) error {
	var results []ProductCount

	accountOfficer := c.Query("account_officer")
	startDate := c.Query("start_date")
	endDate := c.Query("end_date")

	if accountOfficer == "" || startDate == "" || endDate == "" {
		return c.Status(400).JSON(fiber.Map{"error": "Missing required parameters"})
	}

	fmt.Printf("Parameters: accountOfficer=%s, startDate=%s, endDate=%s\n", accountOfficer, startDate, endDate)

	query := `
		SELECT 
			account_title_1 AS "Product Name",
			COUNT(account_title_1) AS "Count"
		FROM 
			public.loan_acct la 
		WHERE 
			la.account_officer = ? AND 
			la.opening_date::date BETWEEN ? AND ?
		GROUP BY 
			account_title_1
	`

	rows, err := database.DB.Raw(query, accountOfficer, startDate, endDate).Rows()
	if err != nil {
		log.Printf("SQL Error: %v", err)
		return c.Status(500).JSON(fiber.Map{"error": "Failed to get product counts"})
	}
	defer rows.Close()

	for rows.Next() {
		var productCount ProductCount
		if err := rows.Scan(&productCount.ProductName, &productCount.Count); err != nil {
			log.Printf("Row Scan Error: %v", err)
			return c.Status(500).JSON(fiber.Map{"error": "Failed to parse product counts"})
		}
		results = append(results, productCount)
	}

	return c.JSON(results)
}

// CenterSummary represents the output format for the center summary query
type CenterSummary struct {
	CenterName   string `json:"center_name"`
	NoOfClients  int    `json:"no_of_clients"`
	WithLoans    int    `json:"with_loans"`
	WithoutLoans int    `json:"without_loans"`
	PastDue      int    `json:"past_due"`
}

// GetCenterSummary handles the request to get a summary of clients by center for a specified officer and date range
func GetCenterSummary(c *fiber.Ctx) error {
	var results []CenterSummary

	accountOfficer := c.Query("account_officer")
	startDate := c.Query("start_date")
	endDate := c.Query("end_date")

	if accountOfficer == "" || startDate == "" || endDate == "" {
		return c.Status(400).JSON(fiber.Map{"error": "Missing required parameters"})
	}

	fmt.Printf("Parameters: accountOfficer=%s, startDate=%s, endDate=%s\n", accountOfficer, startDate, endDate)

	query := `
		SELECT 
			ci.center_name AS "Center Name",
			COUNT(DISTINCT ci.t_id) AS "No of Clients",
			COUNT(DISTINCT CASE WHEN la.customer IS NOT NULL THEN ci.t_id END) AS "w/ Loans",
			COUNT(DISTINCT CASE WHEN la.customer IS NULL THEN ci.t_id END) AS "w/o Loans",
			COUNT(DISTINCT CASE WHEN la.bill_status = 'DUE' THEN ci.t_id END) AS "Past Due"
		FROM 
			public.customer_info ci 
		LEFT JOIN 
			public.loan_acct la ON ci.t_id = la.customer 
		WHERE 
			ci.account_officer = ? 
			AND ci.l_date_recog BETWEEN ? AND ?
		GROUP BY 
			ci.center_name
		UNION ALL  
		SELECT
			'Total Centers' AS "Center Name",
			COUNT(DISTINCT ci.t_id) AS "No of Clients",
			COUNT(DISTINCT CASE WHEN la.customer IS NOT NULL THEN ci.t_id END) AS "w/ Loans",
			COUNT(DISTINCT CASE WHEN la.customer IS NULL THEN ci.t_id END) AS "w/o Loans",
			COUNT(DISTINCT CASE WHEN la.bill_status = 'DUE' THEN ci.t_id END) AS "Past Due"
		FROM 
			public.customer_info ci 
		LEFT JOIN 
			public.loan_acct la ON ci.t_id = la.customer 
		WHERE 
			ci.account_officer = ? 
			AND ci.l_date_recog BETWEEN ? AND ?
	`

	rows, err := database.DB.Raw(query, accountOfficer, startDate, endDate, accountOfficer, startDate, endDate).Rows()
	if err != nil {
		log.Printf("SQL Error: %v", err)
		return c.Status(500).JSON(fiber.Map{"error": "Failed to get center summary"})
	}
	defer rows.Close()

	for rows.Next() {
		var centerSummary CenterSummary
		if err := rows.Scan(&centerSummary.CenterName, &centerSummary.NoOfClients, &centerSummary.WithLoans, &centerSummary.WithoutLoans, &centerSummary.PastDue); err != nil {
			log.Printf("Row Scan Error: %v", err)
			return c.Status(500).JSON(fiber.Map{"error": "Failed to parse center summary"})
		}
		results = append(results, centerSummary)
	}

	return c.JSON(results)
}

// WeeklyCount represents the output format for the weekly customer count query
type WeeklyCount struct {
	Particulars string `json:"particulars"`
	Week        string `json:"week"`
	Count       int    `json:"count"`
}

// GetWeeklyCustomerCount handles the request to get the customer count by week for a specified officer and date range
func GetWeeklyCustomerCount(c *fiber.Ctx) error {
	var results []WeeklyCount

	accountOfficer := c.Query("account_officer")
	startDate := c.Query("start_date")
	endDate := c.Query("end_date")

	if accountOfficer == "" || startDate == "" || endDate == "" {
		return c.Status(400).JSON(fiber.Map{"error": "Missing required parameters"})
	}

	fmt.Printf("Parameters: accountOfficer=%s, startDate=%s, endDate=%s\n", accountOfficer, startDate, endDate)

	query := `
		SELECT 
			ci.member_status AS "Particulars",
			CASE 
				WHEN ci.l_date_recog >= DATE_TRUNC('month', ci.l_date_recog) 
					 AND ci.l_date_recog < DATE_TRUNC('month', ci.l_date_recog) + INTERVAL '1 week' THEN 'Week 1'
				WHEN ci.l_date_recog >= DATE_TRUNC('month', ci.l_date_recog) + INTERVAL '1 week' 
					 AND ci.l_date_recog < DATE_TRUNC('month', ci.l_date_recog) + INTERVAL '2 weeks' THEN 'Week 2'
				WHEN ci.l_date_recog >= DATE_TRUNC('month', ci.l_date_recog) + INTERVAL '2 weeks' 
					 AND ci.l_date_recog < DATE_TRUNC('month', ci.l_date_recog) + INTERVAL '3 weeks' THEN 'Week 3'
				WHEN ci.l_date_recog >= DATE_TRUNC('month', ci.l_date_recog) + INTERVAL '3 weeks' 
					 AND ci.l_date_recog < DATE_TRUNC('month', ci.l_date_recog) + INTERVAL '4 weeks' THEN 'Week 4'
				ELSE 'Week 5'
			END AS "Week",
			COUNT(ci.t_id) AS "Count"
		FROM 
			public.customer_info ci 
		WHERE 
			ci.account_officer = ? 
			AND ci.l_date_recog BETWEEN ? AND ?
		GROUP BY 
			ci.member_status, "Week"
		UNION ALL
		SELECT 
			'Total Client' AS "Particulars",
			CASE 
				WHEN ci.l_date_recog >= DATE_TRUNC('month', ci.l_date_recog) 
					 AND ci.l_date_recog < DATE_TRUNC('month', ci.l_date_recog) + INTERVAL '1 week' THEN 'Week 1'
				WHEN ci.l_date_recog >= DATE_TRUNC('month', ci.l_date_recog) + INTERVAL '1 week' 
					 AND ci.l_date_recog < DATE_TRUNC('month', ci.l_date_recog) + INTERVAL '2 weeks' THEN 'Week 2'
				WHEN ci.l_date_recog >= DATE_TRUNC('month', ci.l_date_recog) + INTERVAL '2 weeks' 
					 AND ci.l_date_recog < DATE_TRUNC('month', ci.l_date_recog) + INTERVAL '3 weeks' THEN 'Week 3'
				WHEN ci.l_date_recog >= DATE_TRUNC('month', ci.l_date_recog) + INTERVAL '3 weeks' 
					 AND ci.l_date_recog < DATE_TRUNC('month', ci.l_date_recog) + INTERVAL '4 weeks' THEN 'Week 4'
				ELSE 'Week 5'
			END AS "Week",
			COUNT(*) AS "Count"
		FROM 
			public.customer_info ci 
		WHERE 
			ci.account_officer = ? 
			AND ci.l_date_recog BETWEEN ? AND ?
		GROUP BY 
			"Week"
		ORDER BY 
			"Particulars", "Week"
	`

	rows, err := database.DB.Raw(query, accountOfficer, startDate, endDate, accountOfficer, startDate, endDate).Rows()
	if err != nil {
		log.Printf("SQL Error: %v", err)
		return c.Status(500).JSON(fiber.Map{"error": "Failed to get weekly customer count"})
	}
	defer rows.Close()

	for rows.Next() {
		var weeklyCount WeeklyCount
		if err := rows.Scan(&weeklyCount.Particulars, &weeklyCount.Week, &weeklyCount.Count); err != nil {
			log.Printf("Row Scan Error: %v", err)
			return c.Status(500).JSON(fiber.Map{"error": "Failed to parse weekly customer count"})
		}
		results = append(results, weeklyCount)
	}

	return c.JSON(results)
}

// WeeklyCapitalBuildUp represents the output format for the weekly capital build-up query
type WeeklyCapitalBuildUp struct {
	Title        string  `json:"title"`
	Week         string  `json:"week"`
	TotalCapital float64 `json:"total_capital"`
}

// GetWeeklyCapitalBuildUp handles the request to get the weekly capital build-up total for a specified officer and date range
func GetWeeklyCapitalBuildUp(c *fiber.Ctx) error {
	var results []WeeklyCapitalBuildUp

	accountOfficer := c.Query("account_officer")
	startDate := c.Query("start_date")
	endDate := c.Query("end_date")

	if accountOfficer == "" || startDate == "" || endDate == "" {
		return c.Status(400).JSON(fiber.Map{"error": "Missing required parameters"})
	}

	fmt.Printf("Parameters: accountOfficer=%s, startDate=%s, endDate=%s\n", accountOfficer, startDate, endDate)

	query := `
		SELECT 
			'Capital Build Up' AS "Title",
			CASE 
				WHEN la.opening_date::date >= DATE_TRUNC('month', la.opening_date::date) 
					 AND la.opening_date::date < DATE_TRUNC('month', la.opening_date::date) + INTERVAL '1 week' THEN 'Week 1'
				WHEN la.opening_date::date >= DATE_TRUNC('month', la.opening_date::date) + INTERVAL '1 week' 
					 AND la.opening_date::date < DATE_TRUNC('month', la.opening_date::date) + INTERVAL '2 weeks' THEN 'Week 2'
				WHEN la.opening_date::date >= DATE_TRUNC('month', la.opening_date::date) + INTERVAL '2 weeks' 
					 AND la.opening_date::date < DATE_TRUNC('month', la.opening_date::date) + INTERVAL '3 weeks' THEN 'Week 3'
				WHEN la.opening_date::date >= DATE_TRUNC('month', la.opening_date::date) + INTERVAL '3 weeks' 
					 AND la.opening_date::date < DATE_TRUNC('month', la.opening_date::date) + INTERVAL '4 weeks' THEN 'Week 4'
				ELSE 'Week 5'
			END AS "Week",
			SUM(la.online_actual_bal::NUMERIC) * -1 AS "Total Capital"
		FROM 
			public.loan_acct la 
		WHERE 
			la.account_officer = ? 
			AND la.bill_type IS NOT NULL
			AND la.opening_date::DATE BETWEEN ? AND ?
		GROUP BY 
			"Week"
		ORDER BY 
			"Week"
	`

	rows, err := database.DB.Raw(query, accountOfficer, startDate, endDate).Rows()
	if err != nil {
		log.Printf("SQL Error: %v", err)
		return c.Status(500).JSON(fiber.Map{"error": "Failed to get weekly capital build-up total"})
	}
	defer rows.Close()

	for rows.Next() {
		var capitalBuildUp WeeklyCapitalBuildUp
		if err := rows.Scan(&capitalBuildUp.Title, &capitalBuildUp.Week, &capitalBuildUp.TotalCapital); err != nil {
			log.Printf("Row Scan Error: %v", err)
			return c.Status(500).JSON(fiber.Map{"error": "Failed to parse weekly capital build-up total"})
		}
		results = append(results, capitalBuildUp)
	}

	return c.JSON(results)
}

// ActiveClientInfo represents the output format for the active client query
type ActiveClientInfo struct {
	UnitName       string `json:"unit_name"`
	CenterName     string `json:"center_name"`
	CID            string `json:"cid"`
	ClientName     string `json:"client_name"`
	DateRecognized string `json:"date_recognized"`
	MemberStatus   string `json:"member_status"`
}

// GetActiveClients handles the request to get information of clients for a specified officer, date range, and member status
func GetClients(c *fiber.Ctx) error {
	var results []ActiveClientInfo

	accountOfficer := c.Query("account_officer")
	startDate := c.Query("start_date")
	endDate := c.Query("end_date")
	memberStatus := c.Query("member_status", "Active")

	if accountOfficer == "" || startDate == "" || endDate == "" {
		return c.Status(400).JSON(fiber.Map{"error": "Missing required parameters"})
	}

	fmt.Printf("Parameters: accountOfficer=%s, startDate=%s, endDate=%s, memberStatus=%s\n", accountOfficer, startDate, endDate, memberStatus)

	query := `
		SELECT 
			ci.unit_name AS "Unit Name",
			ci.center_name AS "Center Name",
			ci.t_id AS "CID",
			ci.customer_name AS "Client Name",
			to_char(ci.l_date_recog, 'Mon. DD, YYYY') AS "Date Recognized",
			ci.member_status AS "Member Status"
		FROM 
			public.customer_info ci 
		WHERE 
			ci.account_officer = ? 
			AND ci.l_date_recog BETWEEN ? AND ?
			AND ci.member_status = ?
	`

	rows, err := database.DB.Raw(query, accountOfficer, startDate, endDate, memberStatus).Rows()
	if err != nil {
		log.Printf("SQL Error: %v", err)
		return c.Status(500).JSON(fiber.Map{"error": "Failed to get active clients"})
	}
	defer rows.Close()

	for rows.Next() {
		var clientInfo ActiveClientInfo
		if err := rows.Scan(&clientInfo.UnitName, &clientInfo.CenterName, &clientInfo.CID, &clientInfo.ClientName, &clientInfo.DateRecognized, &clientInfo.MemberStatus); err != nil {
			log.Printf("Row Scan Error: %v", err)
			return c.Status(500).JSON(fiber.Map{"error": "Failed to parse active client information"})
		}
		results = append(results, clientInfo)
	}

	return c.JSON(results)
}
