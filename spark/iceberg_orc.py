import time

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, DecimalType
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

def defineSchema(table_name: str):
    """
    Define the schema used for the lineitem data.
    """
    logging.info("Defining data schema...")
    if table_name == "lineitem":
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
    elif table_name == "customer":
        schema = StructType([
            StructField("c_custkey", IntegerType(), True),
            StructField("c_name", StringType(), True),
            StructField("c_address", StringType(), True),
            StructField("c_nationkey", IntegerType(), True),
            StructField("c_phone", StringType(), True),
            StructField("c_acctbal", DecimalType(15, 2), True),
            StructField("c_mktsegment", StringType(), True),
            StructField("c_comment", StringType(), True)
        ])
    elif table_name == "order":
        schema = StructType([
            StructField("o_orderkey", IntegerType(), True),
            StructField("o_custkey", IntegerType(), True),
            StructField("o_orderstatus", StringType(), True),
            StructField("o_totalprice", DecimalType(15, 2), True),
            StructField("o_orderdate", DateType(), True),
            StructField("o_orderpriority", StringType(), True),
            StructField("o_clerk", StringType(), True),
            StructField("o_shippriority", IntegerType(), True),
            StructField("o_comment", StringType(), True)
        ])
    else:
        raise Exception("Table does not exist.")
    logging.info("Schema defined.")
    return schema
def creat_table(table_name, spark):
    if table_name == "lineitem":
        createIcebergTable_orders(spark)
    elif table_name == "customer":
        createIcebergTable_customer(spark)
    elif table_name == "order":
        createIcebergTable_orders(spark)
    else:
        raise Exception("Table does not exist.")

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

def createIcebergTable_orders(spark):
    logging.info("Creating/checking Iceberg table...")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS spark_catalog.default.orders (
            o_orderkey       INTEGER NOT NULL,
            o_custkey        INTEGER NOT NULL,
            o_orderstatus    CHAR(1) NOT NULL,
            o_totalprice     DECIMAL(15,2) NOT NULL,
            o_orderdate      DATE NOT NULL,
            o_orderpriority  CHAR(15) NOT NULL,
            o_clerk          CHAR(15) NOT NULL,
            o_shippriority   INTEGER NOT NULL,
            o_comment        VARCHAR(79) NOT NULL
        ) USING iceberg
        OPTIONS (
            format='orc'
        )
        LOCATION 's3a://warehouse/default/orders'
        TBLPROPERTIES (
            'write.format.default'='orc'
        )
    """)
    logging.info("Iceberg table is set up.")

def createIcebergTable_customer(spark):
    logging.info("Creating/checking Iceberg table...")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS spark_catalog.default.customer (
            c_custkey     INTEGER NOT NULL,
            c_name        VARCHAR(25) NOT NULL,
            c_address     VARCHAR(40) NOT NULL,
            c_nationkey   INTEGER NOT NULL,
            c_phone       CHAR(15) NOT NULL,
            c_acctbal     DECIMAL(15,2)   NOT NULL,
            c_mktsegment  CHAR(10) NOT NULL,
            c_comment     VARCHAR(117) NOT NULL
        ) USING iceberg
        OPTIONS (
            format='orc'
        )
        LOCATION 's3a://warehouse/default/customer'
        TBLPROPERTIES (
            'write.format.default'='orc'
        )
    """)
    logging.info("customer Iceberg table is set up.")



def readAndProcessData(spark, schema, table_name):
    """
    Load data from a CSV file using a predefined schema, process it with Spark and write to Iceberg table.
    """
    iceberg_table_name = f"spark_catalog.default.{table_name}"
    logging.info("Loading data from CSV...")
    df = spark.read.schema(schema).csv(f'/home/spark/scripts/{table_name}.csv', header=True)
    count = df.count()
    logging.info(f"Number of rows to write: {count}")

    if count > 0:
        logging.info("Writing data to Iceberg table...")
        df.write.format("iceberg") \
            .option("write.format", "orc") \
            .mode("append") \
            .save(iceberg_table_name)
        logging.info("Data written to Iceberg table successfully in ORC format.")
    else:
        logging.warning("No data to write.")

def query3_performance(spark):
    st_time = time.time()
    query3 = """
            select
              l_orderkey,
              sum(l_extendedprice * (1 - l_discount)) as revenue,
              o_orderdate,
              o_shippriority
            from customer, orders, lineitem
            where c_custkey = o_custkey
              and l_orderkey = o_orderkey
              and c_mktsegment = 'BUILDING' and o_orderdate < date '1995-03-15' and l_shipdate > date '1995-03-15'
            group by l_orderkey, o_orderdate, o_shippriority
            order by revenue desc, o_orderdate
            limit 10
            """
    results = spark.sql(query3)
    results.show()  # This will print the DataFrame contents to the console.
    print(f"Baseline Execution time for query: {time.time() - st_time}")

def main():
    spark = createSparkSession()
    tables = ["lineitem", "orders", "customer"]
    for table in tables:
        schema = defineSchema(table)
        creat_table(table_name=table, spark=spark)  # Ensure table exists
        schema = defineSchema(table_name=table)
        readAndProcessData(spark, schema, table_name=table)
    query3_performance(spark)
    spark.stop()

if __name__ == "__main__":
    main()
