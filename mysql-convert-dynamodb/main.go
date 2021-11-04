package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"mysql-convert-dynamodb/DynamoDB"
	"mysql-convert-dynamodb/Mysql"
)

func main()  {
	/*
	* 0 初始数据库连接和DynamoDB client
	* 1 读取MySQL数据库
	* 2 获取每个数据库中MySQL数据表
	* 3 将数据表结构转为DynamoDB结构（字段，类型）
	*   3.1 获取表字段并判断表字段是否为主键
	* 4 创建DynamoDB表：
	*   4.1 定义DynamoDB表名称：mysql数据库名_数据表
	*   4.2 创建DynamoDB表
	* 5 循环获取MySQL数据表的数据，加载到DynamoDB
	*   5.1 获取所有列数据信息，以及行数
	*   5.2 读取数据表的数据
	*   5.3 把数据写入DynamoDB
	 */
	// 初始化数据库连接
	db := Mysql.ConnectDB()
	if db == nil {
		fmt.Printf("init db failed!\n")
		return
	}
	// 初始DynamoDB client
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("ap-southeast-1"))
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}
	// Using the Config value, create the DynamoDB client
	svc := dynamodb.NewFromConfig(cfg)

	// 1、读取数据库
	database := Mysql.DatabaseInfo(db)
	if database == nil {
		fmt.Println("Failed to obtain database information!")
	}

	// 遍历数据库
	for i := 0; i < len(database); i++ {
		// 2、读取数据表
		table := Mysql.TableInfo(db, database[i])
		if table == nil {
			fmt.Println("Failed to obtain table information!")
		}

		// 2.1、遍历数据表
		for j := 0; j < len(table); j++ {
			// 3 将数据表结构转为DynamoDB结构
			// 3.1、获取表字段并判断表字段是否为主键
			field := Mysql.TableFiledInfo(db, database[i], table[j])

			// 4 创建DynamoDB表名
			// 4.1 定义DynamoDB表名称：mysql数据库名_数据表
			tableName := "mogd-" + database[i] + "_" + table[j]
			// 4.2 创建DynamoDB表
			createDynamoDBResult := DynamoDB.CreateDynamoDB(svc, field, tableName)
			if createDynamoDBResult != nil {
				fmt.Println("create DynamoDB succeed！")
			} else {
				panic(createDynamoDBResult)
			}

			// 5 循环获取MySQL数据表的数据，加载到DynamoDB
 			// 5.1 获取所有列数据信息，以及行数
			tableData, rowLength := Mysql.TableData(db, field, database[i], table[j])
			if tableData == nil {
				continue
			}
			// 5.2 读取数据表的数据
			for k := 0; k < rowLength; k++ {
				itemMap := make(map[string]types.AttributeValue)
				for itemName, item := range tableData {
					itemMap[itemName] =  &types.AttributeValueMemberS{Value: item[k]}
				}
				// 5.3 把数据写入DynamoDB
				putItemReuslt := DynamoDB.PutItemDynamoDB(svc , itemMap, tableName)
				if putItemReuslt != nil {
					fmt.Println("put Item succeed！")
				} else {
					panic(putItemReuslt)
				}
			}
		}
	}

	defer db.Close()  // 关闭数据库连接
	return
}
