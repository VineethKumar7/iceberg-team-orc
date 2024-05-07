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
    elif table_name == "orders":
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
        createIcebergTable_lineitem(spark)
    elif table_name == "customer":
        createIcebergTable_customer(spark)
    elif table_name == "orders":
        createIcebergTable_orders(spark)
    else:
        raise Exception("Table does not exist.")

def createIcebergTable_lineitem(spark):
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
    # Check if table exists
    try:
        spark.sql("SELECT * FROM spark_catalog.default.lineitem LIMIT 1")
        logging.info("Iceberg table 'lineitem' is set up and accessible.")
    except Exception as e:
        logging.error(f"Failed to access table 'lineitem'. Error: {e}")
        raise Exception("Table 'lineitem' does not exist or is not accessible.")

# Changed to Nullable for all columns since the data has missing values
def createIcebergTable_orders(spark):
    logging.info("Creating/checking Iceberg table...")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS spark_catalog.default.orders (
            o_orderkey       INTEGER,
            o_custkey        INTEGER,
            o_orderstatus    CHAR(1),
            o_totalprice     DECIMAL(15,2),
            o_orderdate      DATE,
            o_orderpriority  CHAR(15),
            o_clerk          CHAR(15),
            o_shippriority   INTEGER,
            o_comment        VARCHAR(79)
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


# Changed to Nullable for all columns since the data has missing values
def createIcebergTable_customer(spark):
    logging.info("Creating/checking Iceberg table...")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS spark_catalog.default.customer (
            c_custkey     INTEGER,
            c_name        VARCHAR(25),
            c_address     VARCHAR(40),
            c_nationkey   INTEGER,
            c_phone       CHAR(15),
            c_acctbal     DECIMAL(15,2),
            c_mktsegment  CHAR(10),
            c_comment     VARCHAR(117)
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
    logging.info("readAndProcessData...")
    iceberg_table_name = f"spark_catalog.default.{table_name}"
    logging.info("Loading data from CSV...")
    df = spark.read.schema(schema).csv(f'/home/spark/scripts/{table_name}.csv', header=True)
    count = df.count()
    logging.info(f"Number of rows to write: {count}")

    if count > 0:
        try:
            logging.info(f"Writing data to Iceberg table {iceberg_table_name}...")
            df.write.format("iceberg") \
                .option("write.format", "orc") \
                .mode("append") \
                .save(iceberg_table_name)
            logging.info("Data written to Iceberg table successfully in ORC format.")
        except Exception as e:
            logging.error(f"Failed to write data to {iceberg_table_name}: {e}")
    else:
        logging.warning("No data to write.")


def drop_table_if_exists(spark, table_name):
    logging.info("drop_table_if_exists...")
    logging.info(f"Attempting to drop table if it exists: {table_name}")
    drop_command = f"DROP TABLE IF EXISTS {table_name}"
    try:
        spark.sql(drop_command)
        logging.info(f"Table {table_name} dropped successfully or did not exist.")
    except Exception as e:
        logging.error(f"Failed to drop table {table_name}. Error: {e}")


def query3_performance(spark):
    logging.info("query3_performance...")
    st_time = time.time()
    query3 = """
            SELECT
                l_orderkey,
                SUM(l_extendedprice * (1 - l_discount)) AS revenue,
                o_orderdate,
                o_shippriority
            FROM
                spark_catalog.default.customer AS customer,
                spark_catalog.default.orders AS orders,
                spark_catalog.default.lineitem AS lineitem
            WHERE
                customer.c_custkey = orders.o_custkey AND
                lineitem.l_orderkey = orders.o_orderkey AND
                customer.c_mktsegment = 'BUILDING' AND
                orders.o_orderdate < DATE '1995-03-15' AND
                lineitem.l_shipdate > DATE '1995-03-15'
            GROUP BY
                l_orderkey, o_orderdate, o_shippriority
            ORDER BY
                revenue DESC, o_orderdate
            LIMIT 10
            """
    try:
        results = spark.sql(query3)
        results.show()  # This will print the DataFrame contents to the console.
        # column_sum = results.agg({"revenue": "sum"}).collect()[0][0]
        # print("sum of revenue: ", column_sum)
        # print("type of results: ", type(results))
        print(f"Baseline Execution time for query3: {time.time() - st_time}")
    except Exception as e:
        logging.error(f"Failed to execute query3: {e}")

def query6_performance(spark):
    logging.info("query6_performance...")
    st_time = time.time()
    query6 = """
        select
          sum(l_extendedprice * l_discount) as revenue
        from
          lineitem
        where
          l_shipdate >= date '1994-01-01'
          and l_shipdate < date '1994-01-01' + interval '1' year
          and l_discount between 0.06 - 0.01 and 0.06 + 0.01
          and l_quantity < 24
        """
    try:
        results = spark.sql(query6)
        results.show()  # This will print the DataFrame contents to the console.
        # column_sum = results.agg({"revenue": "sum"}).collect()[0][0]
        # print("sum of revenue: ", column_sum)
        # print("type of results: ", type(results))
        print(f"Baseline Execution time for query6: {time.time() - st_time}")
    except Exception as e:
        logging.error(f"Failed to execute query6: {e}")

def main():
    spark = createSparkSession()

    # # Drop existing tables to prevent conflicts
    # drop_table_if_exists(spark, "spark_catalog.default.lineitem")
    # drop_table_if_exists(spark, "spark_catalog.default.orders")
    # drop_table_if_exists(spark, "spark_catalog.default.customer")
    #
    # tables = ["lineitem", "orders", "customer"]
    # for table in tables:
    #     logging.info("Creating table %s", table)
    #     schema = defineSchema(table)
    #     creat_table(table_name=table, spark=spark)  # Ensure table exists
    #     readAndProcessData(spark, schema, table_name=table)

    query3_performance(spark)
    query6_performance(spark)
    spark.stop()

if __name__ == "__main__":
    main()
