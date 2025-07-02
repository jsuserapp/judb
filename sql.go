package judb

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"errors"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"github.com/jsuserapp/ju"
	"github.com/lib/pq"
	"github.com/mattn/go-sqlite3"
	"os"
)

type Db struct {
	db *sql.DB
}

var errSkip = 1

// SetErrorSkip 设置错误信息输出的栈层次，默认是 1，在调用者位置，0 在库的位置
func SetErrorSkip(skip int) {
	if skip < 0 {
		return
	}
	errSkip = skip
}

// Open 和 Close 函数没有处理异步的情况，也就是它不支持并发
// 当然，一般这两个函数在初始化和结束时调用后，不会频繁调用。
func (db *Db) Open(driverName, dataSourceName string) bool {
	if db.db != nil {
		return true
	}
	d, err := sql.Open(driverName, dataSourceName)
	db.db = d
	return !ju.LogErrorTrace(err, errSkip)
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
	return !ju.LogErrorTrace(err, errSkip)
}
func (db *Db) OpenMysql(host, port, dbname, user, pass string) bool {
	if db.db != nil {
		return true
	}
	d, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8", user, pass, host, port, dbname))
	db.db = d
	return !ju.LogErrorTrace(err, errSkip)
}

func (db *Db) OpenMysqlSSL(host, port, database, user, password, clientKeyPath, clientCertPath, caCertPath string) (rst bool) {
	// --- 2. 创建自定义的 TLS 配置 ---
	// 定义一个独一无二的名字，用于在 DSN 中引用这个配置
	tlsConfigName := "judb-tls-config"

	// 加载 CA 根证书
	rootCAPool := x509.NewCertPool()
	caCert, err := os.ReadFile(caCertPath)
	if ju.LogFail(err) {
		ju.LogRed(fmt.Sprintf("无法读取 CA 证书文件: %v", err))
		return
	}
	if ok := rootCAPool.AppendCertsFromPEM(caCert); !ok {
		ju.LogRed("添加 CA 证书到证书池失败")
		return
	}

	// 加载客户端证书和私钥
	clientCerts, err := tls.LoadX509KeyPair(clientCertPath, clientKeyPath)
	if ju.LogFail(err) {
		ju.LogRed(fmt.Sprintf("无法加载客户端证书或密钥: %v", err))
		return
	}

	// 创建 tls.Config
	tlsConfig := &tls.Config{
		// RootCAs 用于验证服务器证书的颁发机构
		RootCAs: rootCAPool,
		// Certificates 是我们的客户端证书，用于向服务器证明自己的身份
		Certificates: []tls.Certificate{clientCerts},
		// ServerName 如果设置，会用于验证服务器证书上的主机名 (CN/SAN)。
		// 最好设置为你的数据库服务器域名。
		// ServerName: "your.mysql.server.com",
	}

	// --- 3. 向 MySQL 驱动注册这个 TLS 配置 ---
	// 这是最关键的一步！
	err = mysql.RegisterTLSConfig(tlsConfigName, tlsConfig)
	if ju.LogFail(err) {
		ju.LogRed(fmt.Sprintf("注册自定义 TLS 配置失败: %v", err))
		return
	}

	// --- 4. 构建数据库连接字符串 (DSN) ---
	// 格式: user:password@tcp(host:port)/dbname?tls=your_config_name
	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/%s?tls=%s",
		user,
		password,
		host,
		port,
		database,
		tlsConfigName, // 在这里使用我们注册的 TLS 配置名
	)

	// --- 5. 打开数据库连接并测试 ---
	d, err := sql.Open("mysql", dsn)
	if ju.LogFail(err) {
		ju.LogRed(fmt.Sprintf("打开数据库连接失败: %v", err))
		return
	}
	db.db = d

	return true
}
func (db *Db) OpenPostgreSSL(host, port, database, user, password, clientKeyPath, clientCertPath, caCertPath string) (rst bool) {
	// --- 2. 创建自定义的 TLS 配置 ---
	// 定义一个独一无二的名字，用于在 DSN 中引用这个配置
	tlsConfigName := "judb-tls-config"

	// 加载 CA 根证书
	rootCAPool := x509.NewCertPool()
	caCert, err := os.ReadFile(caCertPath)
	if ju.LogFail(err) {
		ju.LogRed(fmt.Sprintf("无法读取 CA 证书文件: %v", err))
		return
	}
	if ok := rootCAPool.AppendCertsFromPEM(caCert); !ok {
		ju.LogRed("添加 CA 证书到证书池失败")
		return
	}

	// 加载客户端证书和私钥
	clientCerts, err := tls.LoadX509KeyPair(clientCertPath, clientKeyPath)
	if ju.LogFail(err) {
		ju.LogRed(fmt.Sprintf("无法加载客户端证书或密钥: %v", err))
		return
	}

	// 创建 tls.Config
	tlsConfig := &tls.Config{
		// RootCAs 用于验证服务器证书的颁发机构
		RootCAs: rootCAPool,
		// Certificates 是我们的客户端证书，用于向服务器证明自己的身份
		Certificates: []tls.Certificate{clientCerts},
		// ServerName 如果设置，会用于验证服务器证书上的主机名 (CN/SAN)。
		// 最好设置为你的数据库服务器域名。
		// ServerName: "your.mysql.server.com",
	}

	// --- 3. 向 MySQL 驱动注册这个 TLS 配置 ---
	// 这是最关键的一步！
	err = mysql.RegisterTLSConfig(tlsConfigName, tlsConfig)
	if ju.LogFail(err) {
		ju.LogRed(fmt.Sprintf("注册自定义 TLS 配置失败: %v", err))
		return
	}

	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s sslrootcert=%s sslcert=%s sslkey=%s",
		host,
		port,
		user, // 这个用户名需要和客户端证书的CN匹配
		password,
		database,
		"verify-full",
		caCertPath,
		clientCertPath,
		clientKeyPath,
	)

	d, err := sql.Open("postgres", dsn)
	if ju.LogFail(err) {
		ju.LogRed(fmt.Sprintf("打开数据库连接失败: %v", err))
		return
	}
	db.db = d
	return true
}
func (db *Db) OutputConnectInfo() {
	// sql.Open 不会立即建立连接，Ping() 会
	err := db.db.Ping()
	if ju.LogErrorTrace(err, 1) {
		return
	}

	ju.LogGreen("🎉 成功连接到 数据库!")

	// 现在可以执行查询了
	var version string
	err = db.QueryRow("SELECT VERSION()").Scan(&version)
	if ju.LogErrorTrace(err, 1) {
		return
	}
	ju.LogGreen(fmt.Sprintf("PostgreSQL 版本: %s\n", version))
}

// OpenPostgreSQL 打开 PostgreSQL 数据库, 这里只提供了基本参数
func (db *Db) OpenPostgreSQL(host, port, dbname, user, pass string) bool {
	if db.db != nil {
		return true
	}
	if port == "" {
		port = "5432"
	}
	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=require",
		host, port, user, pass, dbname)
	d, err := sql.Open("postgres", psqlInfo)
	if err == nil {
		_, er := d.Exec("SET client_encoding = 'UTF8';")
		ju.LogError(er)
	}
	db.db = d
	return !ju.LogErrorTrace(err, errSkip)
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
