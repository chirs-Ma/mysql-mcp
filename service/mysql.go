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

func GetAllTableSchema(ctx context.Context, db *sql.DB, ch chan map[string]string) {
	defer close(ch) // 确保函数结束时关闭通道

	if db == nil {
		Logger.Error("数据库连接未初始化")
		return
	}

	// 查询所有表
	rows, err := db.QueryContext(ctx, "show tables")
	if err != nil {
		Logger.Errorw("查询表失败", "error", err)
		return
	}

	// 立即使用一个函数处理结果，而不是延迟关闭
	tables, err := scanTables(rows)
	rows.Close() // 立即关闭，不使用defer

	if err != nil {
		Logger.Errorw("扫描表失败", "error", err)
		return
	}

	// 处理每个表的结构
	for _, table := range tables {
		select {
		case <-ctx.Done():
			Logger.Info("上下文取消，停止获取表结构")
			return
		default:
			// 在每次循环中创建新的查询
			tableRows, err := db.QueryContext(ctx, "show create table "+table)
			if err != nil {
				// 记录错误但继续处理其他表
				Logger.Warnw("无法获取表结构", "table", table, "error", err)
				continue
			}

			// 使用闭包立即处理结果并关闭资源
			func() {
				defer tableRows.Close() // 在闭包内使用defer是安全的

				if tableRows.Next() {
					var tableName, createTableStmt string
					if err = tableRows.Scan(&tableName, &createTableStmt); err != nil {
						Logger.Warnw("无法扫描表结构", "table", table, "error", err)
						return
					}
					tableMap := map[string]string{
						tableName: createTableStmt,
					}

					select {
					case ch <- tableMap:
						// 成功发送到通道
					case <-ctx.Done():
						Logger.Info("上下文取消，停止发送表结构")
						return
					}
				}
			}()
		}
	}

	Logger.Info("所有表结构获取完成")
}

// 辅助函数：扫描表名
func scanTables(rows *sql.Rows) ([]string, error) {
	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, fmt.Errorf("failed to scan row: %v", err)
		}
		tables = append(tables, table)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error during row iteration: %v", err)
	}

	return tables, nil
}
