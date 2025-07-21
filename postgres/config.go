package postgres

import (
	"fmt"
	"github.com/jsuserapp/ju"
	"path/filepath"
	"strconv"
	"strings"
)

type Config struct {
	Host           string
	Port           uint16
	User           string
	Password       string
	Database       string
	TimeZone       string
	ClientEncode   string
	ClientKeyPath  string
	ClientCertPath string
	CaCertPath     string
	ReadOnly       bool
	ConnParam      map[string]string //额外的连接参数
}

func (cfg *Config) FormatDSN() string {
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s",
		cfg.Host,
		cfg.Port,
		cfg.User,
		EscapeDSNValue(cfg.Password),
		cfg.Database,
	)
	if cfg.CaCertPath != "" {
		// 如果提供了证书，则使用最安全的 verify-full 模式
		// 并将所有证书路径附加到 DSN 中。
		// 使用 filepath.ToSlash 确保路径分隔符的跨平台兼容性。
		dsn += fmt.Sprintf(" sslmode=verify-full sslrootcert=%s sslcert=%s sslkey=%s",
			filepath.ToSlash(cfg.CaCertPath),
			filepath.ToSlash(cfg.ClientCertPath),
			filepath.ToSlash(cfg.ClientKeyPath),
		)
	} else {
		// 如果没有提供证书，则明确禁用 SSL
		dsn += " sslmode=disable"
	}

	// 附加时区参数，这是一个非常好的实践
	if cfg.TimeZone == "" {
		// 如果未指定，默认为 UTC
		cfg.TimeZone = "UTC"
	}
	dsn += fmt.Sprintf(" timezone=%s", cfg.TimeZone)
	if cfg.ClientEncode == "" {
		cfg.ClientEncode = "utf8"
	}
	dsn += fmt.Sprintf(" client_encoding=%s", cfg.ClientEncode)

	if cfg.ReadOnly {
		dsn += " default_transaction_read_only=true"
	}

	for key, val := range cfg.ConnParam {
		dsn += fmt.Sprintf(" %s=%s", key, val)
	}

	return dsn
}
func (cfg *Config) MakeConfig(host, port, database, user, password string) bool {
	cfg.Host = host
	iPort, err := strconv.Atoi(port)
	if ju.OutputErrorTrace(err, 1) {
		return false
	}
	cfg.Port = uint16(iPort)
	cfg.User = user
	cfg.Password = password
	cfg.Database = database
	return true
}
func (cfg *Config) MakeSSLConfig(host, port, database, user, password, clientKeyPath, clientCertPath, caCertPath string) bool {
	if !cfg.MakeConfig(host, port, database, user, password) {
		return false
	}
	if !ju.FileExist(clientKeyPath) {
		ju.OutputColor(1, "red", "client key file not exists")
		return false
	}
	if !ju.FileExist(clientCertPath) {
		ju.OutputColor(1, "red", "client cert file not exists")
		return false
	}
	if !ju.FileExist(caCertPath) {
		ju.OutputColor(1, "red", "ca cert file not exists")
		return false
	}
	cfg.ClientKeyPath = clientKeyPath
	cfg.ClientCertPath = clientCertPath
	cfg.CaCertPath = caCertPath
	return true
}

// EscapeDSNValue 对 DSN 中的值进行安全转义。
// 如果值包含空格，它会用单引号包裹，并转义内部的单引号和反斜杠。
func EscapeDSNValue(value string) string {
	// 检查是否需要用单引号包裹
	if strings.Contains(value, " ") {
		// 替换反斜杠为 \\
		value = strings.ReplaceAll(value, `\`, `\\`)
		// 替换单引号为 ''
		value = strings.ReplaceAll(value, `'`, `''`)
		return `'` + value + `'`
	}
	return value
}
