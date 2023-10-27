""""
This module is going to write data back to mysql database
"""
from pyspark.sql import SparkSession

#MySQL Connection details
MYSQL_JDBC_DRIVER = "com.mysql.jdbc.Driver"
MYSQL_SERVER = "localhost"
MYSQL_DBNAME = "sales"
MYSQL_USERNAME = "shxiao"
MYSQL_PASSWORD = "password"

URL = f"jdbc:mysql://{MYSQL_SERVER}/{MYSQL_DBNAME}"

#Table details
TABLE_EMPLOYEE = "Employees"

def write_to_mysql(_df):
    """
    This method is to write data back to mysql
    """
    _df.write \
        .format("jdbc") \
        .option("driver", MYSQL_JDBC_DRIVER) \
        .option("url", URL) \
        .option("dbtable", TABLE_EMPLOYEE) \
        .option("user", MYSQL_USERNAME) \
        .option("password", MYSQL_PASSWORD) \
        .save()

def read_from_mysql(_spark):
    """
    This method is going to read data from mysql database
    """
    return _spark.read.format("jdbc") \
        .option("driver", MYSQL_JDBC_DRIVER) \
        .option("url", URL) \
        .option("dbtable", TABLE_EMPLOYEE) \
        .option("user", MYSQL_USERNAME) \
        .option("password", MYSQL_PASSWORD) \
        .load()

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("mysql demo") \
        .getOrCreate()

    df_employees = spark.read \
        .json("C:\\Spark\\spark-2.4.6-bin-hadoop2.7\\examples\\src\\main\\resources\\employees.json")
    df_employees.show()

    df_emps = read_from_mysql(spark)
    df_emps.show()
