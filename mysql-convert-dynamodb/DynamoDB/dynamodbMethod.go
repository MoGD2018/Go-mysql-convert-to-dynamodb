package DynamoDB

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"mysql-convert-dynamodb/Mysql"
	"time"
)

// CreateDynamoDB 创建DynamoDB表
// svc: *dynamodb.Client dynamoDB客户端
// field: [] Mysql.Field 数据表字段
// tableName: string DynamoDB数据表名
// return: *dynamodb.CreateTableOutput 返回一个DynamoDB创建表输出
func CreateDynamoDB(svc *dynamodb.Client, field [] Mysql.Field, tableName string) *dynamodb.CreateTableOutput {
	var attributeDefinitions []types.AttributeDefinition
	var keySchema []types.KeySchemaElement
	for i :=0; i < len(field); i++ {
		if (field[i].ColumnKey == "PRI") && (len(attributeDefinitions) < 1)  {
			// 第一个主键作为分区键
			Attribute := []types.AttributeDefinition{
				{
					AttributeName: aws.String(field[i].Fname),
					AttributeType: types.ScalarAttributeTypeS,
				},
			}
			schemaElement := []types.KeySchemaElement{
				{
					AttributeName: aws.String(field[i].Fname),
					KeyType:       types.KeyTypeHash,
				},
			}
			attributeDefinitions = append(attributeDefinitions, Attribute...)
			keySchema = append(keySchema, schemaElement...)
		} else if (field[i].ColumnKey == "PRI") && (len(attributeDefinitions) >= 1) {
			// 第二个主键作为排序键
			Attribute := []types.AttributeDefinition{
				{
					AttributeName: aws.String(field[i].Fname),
					AttributeType: types.ScalarAttributeTypeS,
				},
			}
			schemaElement := []types.KeySchemaElement{
				{
					AttributeName: aws.String(field[i].Fname),
					KeyType:       types.KeyTypeRange,
				},
			}
			attributeDefinitions = append(attributeDefinitions, Attribute...)
			keySchema = append(keySchema, schemaElement...)
		}
		// 当存在多于两个主键时，只选择前两个主键
		if len(attributeDefinitions) >= 2 {
			fmt.Printf("The database primary key is greater than or equal to 2！tableName:%v\n", tableName)
			break
		}
	}
	// 如果不存在主键，以第一个表字段为DynamoDB的分区键
	if len(attributeDefinitions) == 0 {
		attributeDefinitions = []types.AttributeDefinition{
			{
				AttributeName: aws.String(field[0].Fname),
				AttributeType: types.ScalarAttributeTypeS,
			},
		}
		keySchema = []types.KeySchemaElement{
			{
				AttributeName: aws.String(field[0].Fname),
				KeyType:       types.KeyTypeHash,
			},
		}
		fmt.Printf("No primary key exists in the database！tableName:%v\n", tableName)
	}
	//fmt.Println(attributeDefinitions[1].AttributeName)
	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: attributeDefinitions,
		KeySchema: keySchema,
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
		TableName: aws.String(tableName),
	}

	result, err := svc.CreateTable(context.TODO(),input)
	if err != nil {
		fmt.Printf("Failed to create DynamoDB! error: %v\n", err)
		return nil
	}
	// CreateTable为异步操作，需要等待一定时间，继续下一步
	time.Sleep(time.Second * 5)

	return result
}

// PutItemDynamoDB 添加DynamoDB表的Item
// svc: *dynamodb.Client DynamoDB客户端
// itemMap: map[string]types.AttributeValue 数据Item
// tableName: string 数据表名称
// return: 返回PutIntem的输出
func PutItemDynamoDB(svc *dynamodb.Client, itemMap map[string]types.AttributeValue, tableName string) *dynamodb.PutItemOutput{

	input := &dynamodb.PutItemInput{
		Item: itemMap,
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
		TableName:              aws.String(tableName),
	}
	result, err := svc.PutItem(context.TODO(),input)
	if err != nil {
		fmt.Printf("Failed to put Item! error: %v\n", err)
		return nil
	}

	return result
}