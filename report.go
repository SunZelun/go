package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"

	_ "github.com/go-sql-driver/mysql"
)

type topBuyer struct {
	name         string
	wechat_name  string
	spent_in_rmb float64
	spent_in_sgd float64
}

func main() {
	db, err := sql.Open("mysql", "root:@/daigou")
	checkErr(err)

	defer db.Close()

	// query
	// rows, err := db.Query("SELECT id, customer_id, cost_in_sgd FROM orders")
	// checkErr(err)

	// update
	rows, err := db.Query(`SELECT c.name, c.wechat_name, SUM(o.revenue_in_rmb) as spent_in_rmb, 
	SUM(o.revenue_in_sgd) as spent_in_sgd FROM customers as c 
	LEFT JOIN orders as o ON o.customer_id = c.id AND o.status = 1 AND o.order_date 
	WHERE YEAR(order_date) = YEAR(CURRENT_DATE - INTERVAL 1 MONTH)
	AND MONTH(order_date) = MONTH(CURRENT_DATE - INTERVAL 1 MONTH)
	GROUP BY c.id HAVING spent_in_rmb > 0 OR spent_in_sgd > 0 
	ORDER BY spent_in_rmb DESC`)
	checkErr(err)

	buyers := []topBuyer{}

	for rows.Next() {
		var tempBuyer = topBuyer{}
		err = rows.Scan(&tempBuyer.name, &tempBuyer.wechat_name, &tempBuyer.spent_in_rmb, &tempBuyer.spent_in_sgd)
		checkErr(err)

		buyers = append(buyers, tempBuyer)
	}

	if len(buyers) > 0 {
		file, err := os.Create("last_month_report.csv")
		checkErr(err)
		defer file.Close()

		writer := csv.NewWriter(file)
		defer writer.Flush()

		header := []string{"排名", "客户姓名", "微信名字", "RMB总花费", "SGD总花费"}
		err = writer.Write(header)

		for key, value := range buyers {
			index := key + 1
			arr := []string{
				strconv.Itoa(index), value.name, value.wechat_name, strconv.FormatFloat(value.spent_in_rmb, 'f', -1, 32), strconv.FormatFloat(value.spent_in_sgd, 'f', -1, 32),
			}
			err := writer.Write(arr)
			checkErr(err)
		}
	}

	fmt.Print(buyers)
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
