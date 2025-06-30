package main

import (
	"database/sql"
	"github.com/jsuserapp/ju"
	"github.com/jsuserapp/judb"
)

func main() {

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

	ju.LogGreenF("SQLite Version: %s\n", version)
}
