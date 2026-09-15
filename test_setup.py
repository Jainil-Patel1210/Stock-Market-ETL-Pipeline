from pyspark.sql import SparkSession
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

# Test Spark
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
print("Spark version:", spark.version)
spark.stop()

# Test Postgres connection
conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)
print("Postgres connected:", conn.status == 1)
conn.close()