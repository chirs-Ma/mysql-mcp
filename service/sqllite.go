package service

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	_ "github.com/mattn/go-sqlite3"
)

var dbName = "schema.db" // 修改为不带路径前缀的文件名
var dbTable = "mysql_tables"
var sqliteDB *sql.DB
var sqliteOnce sync.Once
var sqliteInitErr error

// InitSQLite 初始化SQLite数据库连接
func InitSQLite() error {
	currentDir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		return fmt.Errorf("获取当前工作目录失败: %v", err)
	}

	dbPath := filepath.Join(currentDir, dbName)

	sqliteOnce.Do(func() {
		// 确保数据库文件所在目录存在
		if _, err := os.Stat(dbPath); os.IsNotExist(err) {
			f, err := os.Create(dbPath)
			if err != nil {
				sqliteInitErr = fmt.Errorf("创建空数据库文件失败: %v", err)
				return
			}
			f.Close()
			Logger.Infow("创建空数据库文件成功", "path", dbPath)
		}

		var db *sql.DB
		db, sqliteInitErr = sql.Open("sqlite3", dbPath)
		if sqliteInitErr != nil {
			sqliteInitErr = fmt.Errorf("打开SQLite数据库失败: %v", sqliteInitErr)
			return
		}

		// 测试连接
		sqliteInitErr = db.Ping()
		if sqliteInitErr != nil {
			return
		}

		// 创建表（如果不存在）
		_, sqliteInitErr = db.Exec(fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				table_name TEXT NOT NULL UNIQUE
			)`, dbTable))
		if sqliteInitErr != nil {
			sqliteInitErr = fmt.Errorf("创建表失败: %v", sqliteInitErr)
			return
		}

		sqliteDB = db
		Logger.Info("SQLite数据库初始化成功")
	})

	return sqliteInitErr
}

func SaveToSQLite(rows []string) (bool, error) {
	if err := InitSQLite(); err != nil {
		return false, fmt.Errorf("SQLite初始化失败: %v", err)
	}

	if len(rows) == 0 {
		Logger.Debug("没有数据需要保存到SQLite")
		return true, nil
	}

	placeholders := make([]string, len(rows))
	args := make([]any, len(rows))
	for i, row := range rows {
		placeholders[i] = "(?)"
		args[i] = row
	}

	insertSQL := fmt.Sprintf("INSERT INTO %s (table_name) VALUES %s",
		dbTable, strings.Join(placeholders, ","))

	_, err := sqliteDB.Exec(insertSQL, args...)
	if err != nil {
		return false, fmt.Errorf("批量插入数据失败: %v", err)
	}
	Logger.Infow("成功保存数据到SQLite", "SQL:", insertSQL)
	return true, nil
}

func CheckRowExist(row []string) []string {
	res := []string{}
	if err := InitSQLite(); err != nil {
		Logger.Errorw("检查行存在时SQLite初始化失败", "error", err)
		return res
	}

	if len(row) == 0 {
		Logger.Debug("检查行存在时输入为空")
		return res
	}

	// 构建查询，获取存在的表
	placeholders := make([]string, len(row))
	args := make([]any, len(row))
	for i, r := range row {
		placeholders[i] = "?"
		args[i] = r
	}

	querySQL := fmt.Sprintf("SELECT table_name FROM %s WHERE table_name IN (%s)",
		dbTable, strings.Join(placeholders, ","))

	// 查询存在的表
	rows, err := sqliteDB.Query(querySQL, args...)
	if err != nil {
		Logger.Errorw("查询表是否存在失败", "error", err)
		return res
	}
	defer rows.Close()

	// 将存在的表添加到 map 中，方便查找
	existingTables := make(map[string]bool)
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			Logger.Warnw("扫描表名失败", "error", err)
			continue
		}
		existingTables[tableName] = true
	}

	// 找出不存在的表
	for _, tableName := range row {
		if !existingTables[tableName] {
			res = append(res, tableName)
		}
	}

	Logger.Infow("检查表存在结果", "totalChecked", len(row), "notExist", len(res))
	return res
}

// CloseSQLite 关闭SQLite数据库连接
func CloseSQLite() {
	if sqliteDB != nil {
		Logger.Info("关闭SQLite数据库连接")
		sqliteDB.Close()
		sqliteDB = nil
	}
}
