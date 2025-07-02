package main

import (
	"database/sql"
	"fmt"
	"github.com/jsuserapp/ju"
	"github.com/jsuserapp/judb"
)

const (
	caCertFile = "D:/Dev/Documents/cert/ca/ca-cert.pem"
)

func main() {
	//testMysqlSSL()
	testPostgreSSL()
}
func testMysqlSSL() {
	clientFolder := "D:/Dev/Documents/cert/mysql/client/"
	var db judb.Db
	db.OpenMysqlSSL("39.100.100.226", "3306", "tool", "tool", "IJ/XpB6L3zUl+p3hepyTyJFERnQ4ZQKz",
		clientFolder+"client-key.pem",
		clientFolder+"client-cert.pem",
		caCertFile)
	db.Close()
}
func testPostgreSSL() {
	clientFolder := "D:/Dev/Documents/cert/postgre/client/tool/"
	var db judb.Db
	db.OpenPostgreSSL("39.100.100.226", "5432", "tool", "tool", "JVy0a+TPfGM2K/wHfpY3/T/40mN6NZ/O",
		clientFolder+"client-key.pem",
		clientFolder+"client-cert.pem",
		caCertFile)
	db.OutputConnectInfo()
	db.Close()
}
func testPostgreSSLRoot() {
	clientFolder := "D:/Dev/Documents/cert/postgre/client/postgres/"
	var db judb.Db
	db.OpenPostgreSSL("39.100.100.226", "5432", "postgres", "postgres", "a3BzV7L0PG9ThsrvHOZScZ7bc4Vfxh",
		clientFolder+"client-key.pem",
		clientFolder+"client-cert.pem",
		caCertFile)
	db.OutputConnectInfo()
	db.Close()
}
func testMysql() {
	var db judb.Db
	if db.OpenMysql("127.0.0.1:3308", "tool", "tool", "IJ/XpB6L3zUl+p3hepyTyJFERnQ4ZQKz") {
		judb.SetLogDb(judb.LogDbTypeMysql, db.GetDb(), true)
		judb.SetLogLimit("", -1)
	}
}
func testSQLite() {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	var version string
	err = db.QueryRow(`SELECT sqlite_version()`).Scan(&version)
	if err != nil {
		panic(err)
	}

	ju.LogGreen(fmt.Sprintf("SQLite Version: %s\n", version))
}
