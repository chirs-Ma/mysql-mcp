package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

func Execute(ctx context.Context, db *sql.DB, sql string) (string, error) {
	// 检查数据库连接是否可用
	if db == nil {
		return "", fmt.Errorf("database connection not initialized")
	}

	// 判断SQL语句类型（简单判断，实际应用中可能需要更复杂的解析）
	queryLower := strings.ToLower(strings.TrimSpace(sql))
	isQuery := strings.HasPrefix(queryLower, "select") || strings.HasPrefix(queryLower, "show") ||
		strings.HasPrefix(queryLower, "describe") || strings.HasPrefix(queryLower, "explain")

	// 如果是查询语句
	if isQuery {
		// 执行查询
		rows, err := db.QueryContext(ctx, sql)
		if err != nil {
			return "", fmt.Errorf("query execution failed: %v", err)
		}
		defer rows.Close()

		// 获取列名
		columns, err := rows.Columns()
		if err != nil {
			return "", fmt.Errorf("failed to get column names: %v", err)
		}

		// 准备结果集
		resultSet := make([]map[string]interface{}, 0)
		colValues := make([]interface{}, len(columns))
		colPointers := make([]interface{}, len(columns))

		// 创建指针切片以接收数据
		for i := range colValues {
			colPointers[i] = &colValues[i]
		}

		// 遍历结果集
		for rows.Next() {
			err = rows.Scan(colPointers...)
			if err != nil {
				return "", fmt.Errorf("failed to scan row: %v", err)
			}

			// 创建行数据映射
			rowData := make(map[string]interface{})
			for i, colName := range columns {
				val := colPointers[i].(*interface{})
				// 处理特殊类型，如时间和二进制数据
				switch v := (*val).(type) {
				case []byte:
					// 尝试将[]byte转换为字符串
					rowData[colName] = string(v)
				default:
					rowData[colName] = *val
				}
			}

			resultSet = append(resultSet, rowData)
		}

		// 检查遍历过程中是否有错误
		if err = rows.Err(); err != nil {
			return "", fmt.Errorf("error during row iteration: %v", err)
		}

		// 将结果转换为JSON
		resultJSON, err := json.MarshalIndent(resultSet, "", "  ")
		if err != nil {
			return "", fmt.Errorf("failed to marshal result to JSON: %v", err)
		}
		return string(resultJSON), nil
	} else {
		// 执行非查询语句（如INSERT, UPDATE, DELETE等）
		result, err := db.ExecContext(ctx, sql)
		if err != nil {
			return "", fmt.Errorf("non-query execution failed: %v", err)
		}

		rowsAffected, _ := result.RowsAffected()
		lastInsertID, _ := result.LastInsertId()

		response := fmt.Sprintf("Query executed successfully. Rows affected: %d", rowsAffected)
		if lastInsertID > 0 {
			response += fmt.Sprintf(", Last insert ID: %d", lastInsertID)
		}

		return response, nil
	}
}

func GetAllTableSchema(ctx context.Context, db *sql.DB) ([]string, error) {
	if db == nil {
		return nil, fmt.Errorf("database connection not initialized")
	}
	rows, err := db.QueryContext(ctx, "show tables")
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %v", err)
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var table string
		if err = rows.Scan(&table); err != nil {
			return nil, fmt.Errorf("failed to scan row: %v", err)
		}
		tables = append(tables, table)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error during row iteration: %v", err)
	}
	var columns []string
	for _, table := range tables {
		// 使用 QueryRow 直接获取单个结果
		var tableName, createTableStmt string
		err := db.QueryRowContext(ctx, "SHOW CREATE TABLE "+table).Scan(&tableName, &createTableStmt)
		if err != nil {
			// 处理错误，例如表不存在
			fmt.Printf("警告: 无法获取表 %s 的结构: %v\n", table, err)
			continue
		}
		columns = append(columns, createTableStmt)
	}
	return columns, nil

}
