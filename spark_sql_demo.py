'''
This module is to read data from sql, transfer data and write to sql server database
'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

#Connection details
CONNECTOR_TYPE = "com.microsoft.sqlserver.jdbc.spark"
SQL_USERNAME = "sa"
SQL_PASSWORD = "yourStrong(!)Password"
SQL_DBNAME = "AdventureWorksLT2019"
SQL_SERVERNAME = "localhost"

#Table name
SALES_HEADER = "SalesLT.SalesOrderHeader"
SALES_SUBTOTAL = "mydb.dbo.SalesSubTotal"

if __name__ == "__main__":

    spark = SparkSession.builder \
        .appName("spark sql demo") \
        .getOrCreate()
    url = f"jdbc:sqlserver://{SQL_SERVERNAME};databaseName={SQL_DBNAME};"

    df_sales_header = spark.read \
        .format(CONNECTOR_TYPE) \
        .option("url", url) \
        .option("dbtable", SALES_HEADER) \
        .option("user", SQL_USERNAME) \
        .option("password", SQL_PASSWORD) \
        .load()

    # df_sales_header.show(2)

    df_sale_subtotal = df_sales_header.groupBy("SalesOrderNumber") \
        .sum("SubTotal") \
        .select("SalesOrderNumber", col("sum(SubTotal)").alias("SubTotal"))

    try:
        df_sale_subtotal.write \
            .format(CONNECTOR_TYPE) \
            .mode("append") \
            .option("url", url) \
            .option("dbtable", SALES_SUBTOTAL) \
            .option("user", SQL_USERNAME) \
            .option("password", SQL_PASSWORD) \
            .save()

    except ValueError as error:
        print("Connector write failed", error)
