from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
import logging

# Setup basic configuration for logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def createSparkSession():
    """
    Create a Spark session configured for Iceberg and S3 (MinIO).
    """
    logging.info("Creating Spark session...")
    spark = SparkSession.builder \
        .appName("Iceberg with MinIO Example") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
        .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://warehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", True) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()
    
    logging.info("Spark session created successfully.")
    return spark

def defineSchema():
    """
    Define the schema used for the lineitem data.
    """
    logging.info("Defining data schema...")
    schema = StructType([
        StructField("l_orderkey", IntegerType(), True),
        StructField("l_partkey", IntegerType(), True),
        StructField("l_suppkey", IntegerType(), True),
        StructField("l_linenumber", IntegerType(), True),
        StructField("l_quantity", DoubleType(), True),
        StructField("l_extendedprice", DoubleType(), True),
        StructField("l_discount", DoubleType(), True),
        StructField("l_tax", DoubleType(), True),
        StructField("l_returnflag", StringType(), True),
        StructField("l_linestatus", StringType(), True),
        StructField("l_shipdate", DateType(), True),
        StructField("l_commitdate", DateType(), True),
        StructField("l_receiptdate", DateType(), True),
        StructField("l_shipinstruct", StringType(), True),
        StructField("l_shipmode", StringType(), True),
        StructField("l_comment", StringType(), True)
    ])
    logging.info("Schema defined.")
    return schema
    
def createIcebergTable(spark):
    logging.info("Creating/checking Iceberg table...")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS spark_catalog.default.lineitem (
            l_orderkey integer,
            l_partkey integer,
            l_suppkey integer,
            l_linenumber integer,
            l_quantity double,
            l_extendedprice double,
            l_discount double,
            l_tax double,
            l_returnflag string,
            l_linestatus string,
            l_shipdate date,
            l_commitdate date,
            l_receiptdate date,
            l_shipinstruct string,
            l_shipmode string,
            l_comment string
        ) USING iceberg
        OPTIONS (
            format='orc'
        )
        LOCATION 's3a://warehouse/default/lineitem'
        TBLPROPERTIES (
            'write.format.default'='orc'
        )
    """)
    logging.info("Iceberg table is set up.")

def readAndProcessData(spark, schema):
    """
    Load data from a CSV file using a predefined schema, process it with Spark and write to Iceberg table.
    """
    logging.info("Loading data from CSV...")
    df = spark.read.schema(schema).csv('/home/spark/scripts/lineitem.csv', header=True)
    count = df.count()
    logging.info(f"Number of rows to write: {count}")

    if count > 0:
        logging.info("Writing data to Iceberg table...")
        df.write.format("iceberg") \
            .option("write.format", "orc") \
            .mode("append") \
            .save("spark_catalog.default.lineitem")
        logging.info("Data written to Iceberg table successfully in ORC format.")
    else:
        logging.warning("No data to write.")


def main():
    spark = createSparkSession()
    schema = defineSchema()
    createIcebergTable(spark)  # Ensure table exists
    readAndProcessData(spark, schema)
    spark.stop()

if __name__ == "__main__":
    main()
