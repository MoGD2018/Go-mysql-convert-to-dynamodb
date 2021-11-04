package Mysql

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
)

// Field 定义一个表字段信息结构体
type Field struct {
	Fname     string
	ColumnKey string
	dataType  string
}
// ConnectDB 初始化MySQL连接
// return: *sql.DB 数据库连接
func ConnectDB() *sql.DB {
	// DSN:Data Source Name
	// 数据源语法："用户名:密码@[连接方式](主机名:端口号)/数据库名"
	dsn := "admin:admin123456@tcp(mogd-test.c5dkdeacqtlg.ap-southeast-1.rds.amazonaws.com:3306)/purchase?charset=utf8mb4&parseTime=True"
	// 不会校验账号密码是否正确
	// 注意！！！这里不要使用:=，我们是给全局变量赋值，然后在main函数中使用全局变量db
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		fmt.Printf("open db error!err:%v\n", err)
		return nil
		//panic(err)
	}
	// 尝试与数据库建立连接（校验dsn是否正确）
	err = db.Ping()
	if err != nil {
		fmt.Printf("ping db error!err:%v\n", err)
		return nil
	}
	return db
}
// DatabaseInfo 查询所有的数据库
// db: *sql.DB 数据库连接
// return: []string 返回一个数据表数组
func DatabaseInfo(db *sql.DB) []string {
	sqlStr := `SELECT table_schema databaseName
			FROM INFORMATION_SCHEMA.TABLES 
			WHERE UPPER(table_type)='BASE TABLE'
			AND table_schema NOT IN ('mysql','performance_schema','sys')
			GROUP BY table_schema
			ORDER BY table_schema asc`
	
	rows, err := db.Query(sqlStr)
	// 关闭查询
	defer rows.Close()
	if err != nil {
		fmt.Printf("query table name error!err:%v\n", err)
		return nil
		//panic(err)
	}
	var result []string
	for rows.Next() {
		var tableName string
		err = rows.Scan(&tableName)
		if err != nil {
			fmt.Printf("scan table name error!err:%v\n", err)
			return nil
		}
		result = append(result, tableName)
	}
	return result

}
// TableInfo 查询数据库中表结构
// db: *sql.DB 数据库连接
// dbName: string 数据库名
// return: []string 返回一个数据表数组
func TableInfo(db *sql.DB, dbName string) []string {
	sqlStr := `SELECT table_name tableName
			FROM INFORMATION_SCHEMA.TABLES 
			WHERE UPPER(table_type)='BASE TABLE'
			AND LOWER(table_schema) = ? 
			GROUP BY table_name
			ORDER BY table_name asc`

	var result []string

	rows, err := db.Query(sqlStr,dbName)
	if err != nil {
		fmt.Printf("query table name error!err:%v\n", err)
		return nil
		//panic(err)
	}
	// 关闭查询
	defer rows.Close()
	for rows.Next() {
		var tableName string
		err = rows.Scan(&tableName)
		if err != nil {
			fmt.Printf("scan table name error!err:%v\n", err)
			return nil
		}
		result = append(result, tableName)
	}
	return result
}
// TableFiledInfo 获取数据表结构
// db: *sql.DB 数据库连接
// database: string 数据库
// table: string 数据表
// return: 返回一个表字段信息结构体
func TableFiledInfo(db *sql.DB, database, table string) [] Field {
	sqlStr := `SELECT COLUMN_NAME fName,COLUMN_KEY columnKey,DATA_TYPE dataType
			FROM information_schema.columns 
			WHERE table_schema = ? AND table_name = ?`
	rows, err := db.Query(sqlStr, database, table)
	if err != nil {
		fmt.Printf("Failed to query a field in a data table！err:%v\n", err)
		return nil
	}
	defer rows.Close()

	var result []Field
	for rows.Next() {
		var f Field
		err = rows.Scan(&f.Fname, &f.ColumnKey, &f.dataType)
		if err != nil {
			fmt.Printf("Failed to scan a field in a table!err:%v\n", err)
			return nil
		}
		result = append(result, f)
	}
	return result
}
 // TableData 获取数据表的所有数据
 // db: *sql.DB 数据库连接
 // field: []Field 数据表字段
 // database: string 数据库
 // table: string 数据表
 // return: map[string][]string 返回每一列的数据
func TableData(db *sql.DB,field []Field,database, table string) (map[string][]string, int) {
	result := make(map[string][]string)
	var rowsLength int
	for i := 0; i < len(field); i++ {
		sqlStr := "SELECT " + field[i].Fname + " from " + database + "." + table
		//fmt.Println(sqlStr)
		rows, err := db.Query(sqlStr)
		if err != nil {
			fmt.Printf("Failed to query table! error: %v\n", err)
			return nil, 0
		}
		defer rows.Close()
		var columnValue string
		var oneResult []string
		for rows.Next() {
			err = rows.Scan(&columnValue)
			if err != nil {
				fmt.Printf("Failed to scan a field in a table!err:%v\n", err)
				return nil, 0
			}
			oneResult = append(oneResult, columnValue)
		}
		if len(oneResult) == 0 {
			fmt.Printf("%v.%v not data!\n", database, table)
			return nil, 0
		}
		result[field[i].Fname] = oneResult
		rowsLength = len(oneResult)
	}
	return result, rowsLength
}