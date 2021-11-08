# Go-mysql-convert-to-dynamodb
## GO语言——实现从MySQL数据库迁移到Amazon DynamoDB
博客地址：https://blog.csdn.net/weixin_41335923/article/details/121209000
## 思路与函数

### 思路

0. 初始数据库连接和DynamoDB client

1. 读取MySQL数据库

2. 获取每个数据库中MySQL数据表

3. 将数据表结构转为DynamoDB结构（字段，类型）

   - 获取表字段并判断表字段是否为主键

4. 创建DynamoDB表：

   - 定义DynamoDB表名称：mysql数据库名_数据表

   - 创建DynamoDB表

5. 循环获取MySQL数据表的数据，加载到DynamoDB

   - 获取所有列数据信息，以及行数

   - 读取数据表的数据

   - 把数据写入DynamoDB


## MySQL函数

### 查询数据库、数据表和字段信息

在MySQL数据库中，`INFORMATION_SCHEMA.TABLES`表存储了MySQL数据库的元数据。

元数据信息主要包括数据库中表信息以及表字段信息，可以从`INFORMATION_SCHEMA.TABLES`表中查询数据库信息：


### 查询表所有信息 TableData

在go语言原生的`github.com/go-sql-driver/mysql`中的查询，需要指定与查询结果同样数量的变量才能把查询结果输出。

官方文档说明：`https://pkg.go.dev/database/sql#Row.Scan`

因此博主通过上个函数获取到的表字段，一个个字段查询，汇总为一个按列查询结构的map类型数据。

1. 遍历字段数组，通过`Query`查询出，数据表中该列的值
2. 利用`rows.Next`遍历查询结果，`rows.Scan`获取列值，追加到数组中
3. 使用map类型，以key(字段名):value[值数组]的方式存储一个表的所有数据

## Amazon DynamoDB

### 创建DynamoDB表

在DynamoDB的设计中，只有一个分区键和一个排序键。当然Amazon DynamoDB中，还可以添加全局索引和本地索引，这个方式复杂，在这里只是使用了分区键和排序键

因为DynamoDB只有两个键，并且必须指定一个分区键。

而在MySQL数据库中可能会存在两个以上或无主键的情况，面对这两种情况，博主通过判断前面获取的字段属性值。

如果存在两个主键，就以查询结果前面的两个主键分别作为分区键和排序键；若不存在主键，以查询结果第一个列为分区键。

<font color="red" size="5">在这里最好的方式应该是写成一个接口，按实际生产来修改每个数据表转换为DynamoDB后的格式</font>

<font color="red" size=5>另外创建表，默认都是String类型，并没有判断原字段的格式。博主理解最好的方法应该是使用Go语言的反射机制来判断转换的DynamoDB字段类型</font>


### 插入数据

通过遍历获取到MySQL数据表的所有数据，将数据添加到符合DynamoDB格式的map中，调用`PutItemInput`接口添加数据

<font color="red" size=5>添加的数据类型都是String类型，并没有判断原字段的格式。博主理解最好的方法应该是使用Go语言的反射机制来判断转换的DynamoDB字段类型</font>


## 附录：

### 参考

MySQL Driver：https://github.com/Go-SQL-Driver/MySQL/

AWS Go DynamoDB SDKv2：https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb#Client.PutItem
