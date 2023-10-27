from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = SparkSession.builder.appName("my first Spark app").getOrCreate()
    df_employees = spark.read.json("C:\\Spark\\spark-2.4.6-bin-hadoop2.7\\examples\\src\\main\\resources\\employees.json")
    df_employees.show()


    df_users = spark.read.parquet("C:\\Spark\\spark-2.4.6-bin-hadoop2.7\\examples\\src\\main\\resources\\users.parquet")
    df_users.show()

    df_people = spark.read.text("C:\\Spark\\spark-2.4.6-bin-hadoop2.7\\examples\\src\\main\\resources\\people.txt")
    df_people.show()










