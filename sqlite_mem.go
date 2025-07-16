package judb

import (
	"database/sql"
	"fmt"
	"github.com/jsuserapp/ju"
)

// SQLiteMemDb SQLite 内存数据库, 它的函数并不支持并发, 但并不是说不能多线程调用, 而是不能频繁调用
// 普通的确认没有线程竞争的情况下是可以多线程调用的. 内存模式的数据库, 无论设置什么参数, 都不支持多线程
// 同时写
type SQLiteMemDb struct {
	Db *sql.DB
}

func (mdb *SQLiteMemDb) Open() bool {
	if mdb.Db != nil {
		return true
	}
	source := "file:shared_mem?mode=memory&cache=shared"
	db, err := sql.Open("sqlite3", source)
	if err != nil {
		ju.OutputColor(0, ju.ColorRed, err.Error())
		return false
	}
	mdb.Db = db
	return true
}
func (mdb *SQLiteMemDb) Close() {
	if mdb.Db != nil {
		_ = mdb.Db.Close()
		mdb.Db = nil
	}
}

// LoadFromFile 加载数据库内容到内存数据库
func (mdb *SQLiteMemDb) LoadFromFile(fileDB string) bool {
	if !mdb.Open() {
		return false
	}

	// 附加文件数据库
	_, err := mdb.Db.Exec(fmt.Sprintf("ATTACH DATABASE '%s' AS filedb", fileDB))
	if err != nil {
		ju.OutputColor(0, ju.ColorRed, err.Error())
		return false
	}
	defer func() {
		_, err = mdb.Db.Exec("DETACH DATABASE filedb")
		if err != nil {
			ju.OutputColor(0, ju.ColorRed, err.Error())
		}
	}()

	// 查询文件数据库中的表结构
	rows, err := mdb.Db.Query("SELECT name,`sql` FROM filedb.sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
	if err != nil {
		ju.OutputColor(0, ju.ColorRed, err.Error())
		return false
	}
	rows, err = mdb.Db.Query("SELECT * FROM filedb.users")
	defer rows.Close()

	// 遍历所有表并复制数据
	for rows.Next() {
		var name, sqlCase string
		if err := rows.Scan(&name, &sqlCase); err != nil {
			ju.OutputColor(0, ju.ColorRed, err.Error())
			return false
		}

		// 在内存数据库中创建表
		_, err = mdb.Db.Exec(sqlCase)
		if err != nil {
			// 回滚事务
			mdb.Db.Exec(`ROLLBACK;`)
			ju.OutputColor(0, ju.ColorRed, err.Error())
			return false
		}

		// 开始事务
		_, err := mdb.Db.Exec(`BEGIN TRANSACTION;`)
		if err != nil {
			ju.OutputColor(0, ju.ColorRed, err.Error())
			continue
		}
		// 将数据从文件数据库复制到内存数据库
		_, err = mdb.Db.Exec(fmt.Sprintf("INSERT INTO %s SELECT * FROM filedb.%s", name, name))
		if err != nil {
			// 回滚事务
			mdb.Db.Exec(`ROLLBACK;`)
			ju.OutputColor(0, ju.ColorRed, err.Error())
			return false
		}

		// 提交事务
		_, err = mdb.Db.Exec(`COMMIT;`)
		if err != nil {
			ju.OutputColor(0, ju.ColorRed, err.Error())
			return false
		}
	}

	return true
}

// SaveToFile 这会把内存数据保存到指定的文件, 如果文件存在, 会被覆盖
func (mdb *SQLiteMemDb) SaveToFile(fileDB string) bool {
	if mdb.Db == nil {
		return false
	}
	_, err := mdb.Db.Exec(fmt.Sprintf("VACUUM INTO '%s'", fileDB))
	if err != nil {
		ju.OutputColor(0, ju.ColorRed, err.Error())
		return false
	}
	return true
}
