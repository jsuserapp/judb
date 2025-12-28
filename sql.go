package judb

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jsuserapp/ju"
	"github.com/jsuserapp/judb/postgres"
	"github.com/mattn/go-sqlite3"
)

const (
	DatabaseTypeMysql    = "mysql"
	DatabaseTypeSqlite   = "sqlite"
	DatabaseTypePostgres = "postgres"
)

type Db struct {
	dbType string
	db     *sql.DB
}

var errSkip = 1

// SetErrorSkip 设置错误信息输出的栈层次，默认是 1，在调用者位置，0 在库的位置
func SetErrorSkip(skip int) {
	if skip < 0 {
		return
	}
	errSkip = skip
}

// OpenSqlite3 支持多线程写入, 这会稍微降低性能, 但是大多数场景很难避免多线程写入, 如果不启用这个特性,
// 写入时候有概率触发表被锁定提示.
//
// dbpath: example ./data/log.db
//
// params: 如果不需要修改参数，可以设置为空串，此时它的值是 _mutex=full&_journal_mode=WAL
func (db *Db) OpenSqlite3(dbpath, params string) bool {
	if db.db != nil {
		return true
	}
	if params == "" {
		params = "_mutex=full&_journal_mode=WAL"
	}
	d, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?%s", dbpath, params))
	db.db = d
	db.dbType = DatabaseTypeSqlite
	return !ju.LogErrorTrace(err, errSkip)
}

// MakeTLSConfig Mysql 使用证书的方式和 PostgreSQL 不太一样，需要单独注册
// 另外，如果有多个连接，需要注册不同的名字："judb-tls-config1"，"judb-tls-config2"
func MakeTLSConfig(clientKeyPath, clientCertPath, caCertPath, serverName string) (cfg *tls.Config) {
	// 加载 CA 根证书
	rootCAPool := x509.NewCertPool()
	caCert, err := os.ReadFile(caCertPath)
	if ju.OutputErrorTrace(err, 1) {
		//ju.LogRed(fmt.Sprintf("无法读取 CA 证书文件: %v", err))
		return
	}
	if ok := rootCAPool.AppendCertsFromPEM(caCert); !ok {
		ju.LogRed("添加 CA 证书到证书池失败")
		return
	}

	// 加载客户端证书和私钥
	clientCerts, err := tls.LoadX509KeyPair(clientCertPath, clientKeyPath)
	if ju.OutputErrorTrace(err, 1) {
		//ju.LogRed(fmt.Sprintf("无法加载客户端证书或密钥: %v", err))
		return
	}

	// 创建 tls.Config
	cfg = &tls.Config{
		// RootCAs 用于验证服务器证书的颁发机构
		RootCAs: rootCAPool,
		// Certificates 是我们的客户端证书，用于向服务器证明自己的身份
		Certificates: []tls.Certificate{clientCerts},
		// ServerName 如果设置，会用于验证服务器证书上的主机名 (CN/SAN)。
		// 最好设置为你的数据库服务器域名。
		ServerName: serverName,
	}
	return cfg
}

// MakeMysqlConfig 这个函数是为了简化 Config 的构造
func MakeMysqlConfig(host, port, dbname, user, pass string) *mysql.Config {
	cfg := &mysql.Config{
		Net:    "tcp",
		User:   user,
		Passwd: pass,
		DBName: dbname,
		Addr:   fmt.Sprintf("%s:%s", host, port),
	}
	return cfg
}

// MakeMysqlSSLConfig 如果 TLS 注册失败，函数会返回 nil，这个函数是为了简化 Config 的构造
// tlsName 如果是多连接需要不同名称，否则会产生冲突，单联接时可以传入空串，相当于使用默认名称 "judb-tls-config"
func MakeMysqlSSLConfig(host, port, dbname, user, pass, tlsName, clientKeyPath, clientCertPath, caCertPath string) *mysql.Config {
	if tlsName == "" {
		tlsName = "judb-tls-config"
	}
	tlsCfg := MakeTLSConfig(clientKeyPath, clientCertPath, caCertPath, host)
	err := mysql.RegisterTLSConfig(tlsName, tlsCfg)
	if ju.LogFail(err) {
		ju.LogRed(fmt.Sprintf("注册自定义 TLS 配置失败: %v", err))
		return nil
	}

	cfg := &mysql.Config{
		Net:    "tcp",
		User:   user,
		Passwd: pass,
		DBName: dbname,
		Addr:   fmt.Sprintf("%s:%s", host, port),
		Loc:    time.UTC,
		Params: map[string]string{
			"tls": tlsName,
		},
	}
	return cfg
}
func (db *Db) OpenMysql(cfg *mysql.Config) bool {
	if db.db != nil {
		return true
	}
	d, err := sql.Open("mysql", cfg.FormatDSN())
	db.db = d
	db.dbType = DatabaseTypeMysql
	return !ju.LogErrorTrace(err, errSkip)
}

type name struct {
}

func (db *Db) OpenPostgres(cfg *postgres.Config) bool {
	dsn := cfg.FormatDSN()
	d, err := sql.Open("pgx", dsn)
	db.db = d
	db.dbType = DatabaseTypePostgres
	return !ju.LogErrorTrace(err, errSkip)
}
func (db *Db) OutputConnectInfo() bool {
	// sql.Open 不会立即建立连接，Ping() 会
	err := db.db.Ping()
	if ju.LogErrorTrace(err, 1) {
		return false
	}

	sqlCase := "SELECT VERSION()"
	if db.dbType == DatabaseTypeSqlite {
		sqlCase = "SELECT sqlite_version()"
	}
	// 现在可以执行查询了
	var version string
	err = db.QueryRow(sqlCase).Scan(&version)
	if ju.LogErrorTrace(err, 1) {
		return false
	}
	if db.dbType == DatabaseTypeSqlite {
		ju.OutputColor(1, "green", fmt.Sprintf("SQLite Version %s", version))
	} else if db.dbType == DatabaseTypeMysql {
		ju.OutputColor(1, "green", fmt.Sprintf("MySQL Version %s", version))
	} else if db.dbType == DatabaseTypePostgres {
		ju.OutputColor(1, "green", version)
	}
	return true
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

func NewSqlResult(err error) (rst SqlResult) {
	rst.SetError(err)
	return
}
func (mr *SqlResult) Fail() bool {
	return mr.Error != ""
}
func (mr *SqlResult) SetError(err error) {
	if err == nil {
		return
	}
	mr.Error = err.Error()
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		mr.Code = fmt.Sprintf("%d", mysqlErr.Number)
		mr.Error = mysqlErr.Message
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		mr.Code = pgErr.Code
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
		ju.OutputColor(errSkip, "red", mr.Error)
		return mr
	}
	rows, err := db.db.Query(sqlCase, v...)
	if ju.LogErrorTrace(err, errSkip) {
		mr.SetError(err)
	} else {
		qc(rows)
		_ = rows.Close()
	}
	return mr
}
func (db *Db) QueryRow(sqlCase string, v ...interface{}) *sql.Row {
	return db.db.QueryRow(sqlCase, v...)
}
func (db *Db) Exec(sqlCase string, v ...interface{}) SqlResult {
	var mr SqlResult
	if db.db == nil {
		mr.Code = "-1"
		mr.Error = "数据库对象为 nil"
		ju.OutputColor(errSkip, "red", mr.Error)
		return mr
	}
	rst, err := db.db.Exec(sqlCase, v...)
	if ju.LogErrorTrace(err, errSkip) {
		mr.SetError(err)
	} else {
		mr.Result = rst
	}
	return mr
}
func (db *Db) Begin() (*sql.Tx, SqlResult) {
	var mr SqlResult
	tx, err := db.db.Begin()
	if ju.LogErrorTrace(err, errSkip) {
		mr.SetError(err)
		return nil, mr
	}
	return tx, mr
}
func (db *Db) GetDb() *sql.DB {
	return db.db
}
