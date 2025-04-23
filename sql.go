package judb

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"github.com/jsuserapp/ju"
	"github.com/lib/pq"
	"github.com/mattn/go-sqlite3"
)

type Db struct {
	db *sql.DB
}

// Open 和 Close 函数没有处理异步的情况，也就是它不支持并发
// 当然，一般这两个函数在初始化和结束时调用后，不会频繁调用。
func (db *Db) Open(driverName, dataSourceName string) bool {
	if db.db != nil {
		return true
	}
	d, err := sql.Open(driverName, dataSourceName)
	db.db = d
	return ju.CheckSuccess(err)
}

// OpenSqlite3 支持多线程写入, 这会稍微降低性能, 但是大多数场景很难避免多线程写入, 如果不启用这个特性,
// 写入时候有概率触发表被锁定提示.
// dbname: example ./data/log.db
func (db *Db) OpenSqlite3(dbname string) bool {
	if db.db != nil {
		return true
	}
	d, err := sql.Open("sqlite3", "file:"+dbname+"?_mutex=full&_journal_mode=WAL")
	db.db = d
	return ju.CheckSuccess(err)
}
func (db *Db) OpenMysql(host, dbname, user, pass string) bool {
	if db.db != nil {
		return true
	}
	d, err := sql.Open("mysql", user+":"+pass+"@tcp("+host+")/"+dbname+"?charset=utf8")
	db.db = d
	return ju.CheckSuccess(err)
}

// OpenPostgreSQL 打开 PostgreSQL 数据库, 这里只提供了基本参数
func (db *Db) OpenPostgreSQL(host, port, dbname, user, pass string) bool {
	if db.db != nil {
		return true
	}
	if port == "" {
		port = "5432"
	}
	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, pass, dbname)
	d, err := sql.Open("postgres", psqlInfo)
	if err == nil {
		_, er := d.Exec("SET client_encoding = 'UTF8';")
		ju.CheckError(er)
	}
	db.db = d
	return ju.CheckSuccess(err)
}
func (db *Db) Close() {
	if db.db != nil {
		_ = db.db.Close()
	}
}

type SqlResult struct {
	Result sql.Result
	Code   string
	Error  string
}

func (mr *SqlResult) Failure() bool {
	return mr.Error != ""
}
func (mr *SqlResult) SetError(err error) {
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		mr.Code = fmt.Sprintf("%d", mysqlErr.Number)
		mr.Error = mysqlErr.Message
	}
	var pgErr *pq.Error
	if errors.As(err, &pgErr) {
		mr.Code = string(pgErr.Code)
		mr.Error = pgErr.Message
	}
	var sqErr sqlite3.Error
	if errors.As(err, &sqErr) {
		mr.Code = fmt.Sprintf("%d", sqErr.Code)
		mr.Error = sqErr.Error()
	}
}

type QueryCall func(rows *sql.Rows)
type QueryFunc func(string, QueryCall, ...interface{}) SqlResult

// Query 查询需要返回数据的语句，数据从回调函数的 rows 里获取，无需执行 rows 的 Close 函数，
// 这个设计的目的是减少遗忘 Close 的可能，因为遗忘 Close 不会对程序有立即的影响，直到 Mysql
// 资源被耗尽，对于海量的查询语句来说，定位哪里忘记 Close 是非常困难的。
// noinspection GoUnusedExportedFunction
func (db *Db) Query(sqlCase string, qc QueryCall, v ...interface{}) SqlResult {
	var mr SqlResult
	if db.db == nil {
		mr.Code = "-1"
		mr.Error = "数据库对象不可用 nil"
		ju.OutputColor(1, "red", mr.Error)
		return mr
	}
	rows, err := db.db.Query(sqlCase, v...)
	if ju.CheckTrace(err, 1) {
		mr.SetError(err)
	} else {
		qc(rows)
		_ = rows.Close()
	}
	return mr
}
func (db *Db) Exec(sqlCase string, v ...interface{}) SqlResult {
	var mr SqlResult
	if db.db == nil {
		mr.Code = "-1"
		mr.Error = "数据库对象为 nil"
		ju.OutputColor(1, "red", mr.Error)
		return mr
	}
	rst, err := db.db.Exec(sqlCase, v...)
	if ju.CheckTrace(err, 1) {
		mr.SetError(err)
	} else {
		mr.Result = rst
	}
	return mr
}
func (db *Db) Begin() (*sql.Tx, SqlResult) {
	var mr SqlResult
	tx, err := db.db.Begin()
	if ju.CheckTrace(err, 1) {
		mr.SetError(err)
		return nil, mr
	}
	return tx, mr
}
func (db *Db) GetDb() *sql.DB {
	return db.db
}
