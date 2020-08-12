import pyodbc
import json
import argparse
from pyspark import SparkContext
from pyspark.sql import SparkSession

"""
This code extracts a spark dataframe's schema (csv,avro,parquet) and convert it to mysql or
sql server ddl (Data Defenition Language) to create a table based on your data automatically.

"""
sc = SparkContext()
spark = SparkSession(sc)

parser = argparse.ArgumentParser(description='convert a spark dataframe schema to a create table ddl.')
parser.add_argument('--dbname', type=str, help='database name')
parser.add_argument('--tname', type=str, help='table name')
parser.add_argument('--pkey', type=str, help='primary key of table')
parser.add_argument('--data_path', type=str, help='data path')
parser.add_argument('--data_type', type=str, default='parquet', help='It supports parquet,avro and csv')
parser.add_argument('--db_type', type=str, default='mssql', help='It supports mysql,mssql')
parser.add_argument('--server', type=str, default='localhost', help='Ip or hostname of database')
parser.add_argument('--port', type=str, default='mssql', help='Port to connect to your database')
parser.add_argument('--username', type=str, help='username to connect to your database')
parser.add_argument('--password', type=str, help='password to connect tyour database')
args = parser.parse_args()

dbname = vars(args).get('dbname')
tableName = vars(args).get('tname')
p_key = vars(args).get('pkey')

data_path = vars(args).get('data_path')
data_type = vars(args).get('data_type')

db_type = vars(args).get('db_type')
server = vars(args).get('server')
port = vars(args).get('port')
user = vars(args).get('username')
pas = vars(args).get('password')


# ---------------------------------------------------------------------------------------
def readData(dtype, ddir):
    if dtype == 'parquet':
        df = spark.read.format("parquet").option("inferSchema", 'true').load(ddir)
    elif dtype == 'avro':
        df = spark.read.format("avro").option("inferSchema", 'true').load(ddir)
    elif dtype == 'csv':
        df = spark.read.format("csv").option("inferSchema", 'true').load(ddir)
    else:
        df = None
        print("Your data type is not valid!")
    return df


# ---------------------------------------------------------------------------------------
def getColSchema(fields):
    columnschema = ''
    for field in fields:
        if field.name == p_key:
            if db_type == 'mssql':
                columnschema = columnschema + "{} {} {}, ".format(p_key, convTypeMssql(str(field.dataType)), 'NOT NULL')
            elif db_type == 'mysql':
                columnschema = columnschema + "{} {}, ".format(p_key, convTypeMssql(str(field.dataType)))
        else:
            columnschema = columnschema + "{} {} {}, ".format(field.name, convTypeMssql(str(field.dataType)),isNullable(field.nullable))

    return columnschema


# ---------------------------------------------------------------------------------------
def convTypeMssql(dtype):
    mysqlDataTypes = {
        'StringType': 'VARCHAR(255)',
        'IntegerType': 'INT',
        'DoubleType': 'DOUBLE',
        'LongType': 'BIGINT',
        'FloatType': 'FLOAT'
    }
    return mysqlDataTypes[dtype] if dtype in mysqlDataTypes else 'VARCHAR(255)'


# ----------------------------------------------------------------------------------------
def isNullable(res):
    if res == True:
        return 'NULL'
    else:
        return 'NOT NULL'


# ----------------------------------------------------------------------------------------
def createsqlTable(tableName, column_schema, primary_key):
    ddl = "CREATE TABLE {} ({}PRIMARY KEY ({}))".format(tableName, column_schema, primary_key)
    return ddl


# ----------------------------------------------------------------------------------------
def sqlExecute(dbtype, server, port, dbname, user, pas, fields):

    if dbtype == 'mssql':
        # connection = pyodbc.connect('DRIVER=FreeTDS;SERVER=node4;PORT=1433;DATABASE=test;
        # UID=SA;PWD=Dl123456;TDS_Version=8.0;')
        connection = pyodbc.connect(
            "DRIVER=FreeTDS;SERVER={};PORT={};DATABASE={};UID={};PWD={};TDS_Version=8.0;".format(
                server, port, dbname,user, pas))

    elif dbtype == 'mysql':
        # connection = pyodbc.connect("DRIVER=MySQL;User ID={root};Password={Dl123456};
        # Server={node1};Database={test};Port={3306};String Types=Unicode"
        connection = pyodbc.connect(
            "DRIVER=MySQL;User ID={};Password={};Server={};Database={};Port={};String Types=Unicode".format(
                user, pas,server,dbname,port))

    else:
        connection = None
        print("Your dataBase type is not valid!")

    cursor = connection.cursor()
    column_schema = getColSchema(fields)
    ddl = createsqlTable(tableName, column_schema, p_key)
    cursor.execute(ddl)
    connection.commit()
    cursor.close()
    connection.close()


# ----------------------------------------------------------------------------------------

if __name__ == '__main__':
    df = readData(data_type, data_path)
    df.show(50)
    schema = df.schema
    fields = schema.fields
    sqlExecute(db_type, server, port, dbname, user, pas, fields)
