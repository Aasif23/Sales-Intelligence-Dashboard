import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, month, year, to_date, lit, round, desc, to_timestamp, greatest
from pyspark.sql.types import DoubleType, StringType, IntegerType, DateType, StructType, StructField
import kagglehub
from kagglehub import KaggleDatasetAdapter
import sqlite3
import logging

# Configure logging for better visibility and debugging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- 1. Initialize Spark Session ---
def initialize_spark_session():
    """Initializes and returns a SparkSession with appropriate configurations."""
    logging.info("Initializing Spark Session...")
    spark = (
        SparkSession.builder.appName("SalesAnalyticsPipeline")
        .config("spark.memory.offHeap.enabled", "true") # Enable off-heap memory for larger datasets
        .config("spark.memory.offHeap.size", "2g")     # Set off-heap memory size
        .config("spark.driver.memory", "4g")           # Increased driver memory for better performance
        .getOrCreate()
    )
    logging.info("Spark Session initialized.")
    return spark

# --- 2. Ingest Raw CSV Data ---
def ingest_data(spark_session, kaggle_dataset_ref, csv_file_name, schema):
    """
    Loads raw CSV data from KaggleHub into a PySpark DataFrame using a Pandas intermediate.
    
    Args:
        spark_session: The active SparkSession.
        kaggle_dataset_ref (str): Kaggle dataset reference (e.g., "owner/dataset-name").
        csv_file_name (str): The name of the CSV file within the dataset.
        schema: The explicit schema to apply for type safety and performance.
        
    Returns:
        pyspark.sql.DataFrame: The raw PySpark DataFrame with applied schema.
    """
    logging.info(f"Attempting to load dataset '{csv_file_name}' from KaggleHub...")
    try:
        # Use KaggleHub to load the dataset into a Pandas DataFrame first
        pandas_df = kagglehub.load_dataset(
            KaggleDatasetAdapter.PANDAS,
            kaggle_dataset_ref,
            csv_file_name,
            pandas_kwargs={"encoding": "latin1"} # Specify encoding to handle potential character issues
        )
        logging.info("Dataset loaded successfully into Pandas DataFrame.")

        # Convert Pandas DataFrame to PySpark DataFrame, applying the explicit schema
        raw_df = spark_session.createDataFrame(pandas_df, schema=schema)
        logging.info("Converted Pandas DataFrame to PySpark DataFrame with explicit schema.")
        return raw_df
    except Exception as e:
        logging.error(f"Error loading dataset from KaggleHub: {e}", exc_info=True)
        raise # Re-raise the exception to propagate it up the call stack

# --- 3. Define Explicit Schema ---
def define_schema():
    """
    Defines the explicit schema for the sales data.
    This helps in type inference and ensures data quality from the start.
    """
    logging.info("Defining explicit schema for the sales data.")
    schema = StructType([
        StructField("ORDERNUMBER", IntegerType(), True),
        StructField("QUANTITYORDERED", IntegerType(), True),
        StructField("PRICEEACH", DoubleType(), True),
        StructField("ORDERLINENUMBER", IntegerType(), True),
        StructField("SALES", DoubleType(), True),
        StructField("ORDERDATE", StringType(), True), # Read as String initially for safe parsing
        StructField("STATUS", StringType(), True),
        StructField("QTR_ID", IntegerType(), True),
        StructField("MONTH_ID", IntegerType(), True),
        StructField("YEAR_ID", IntegerType(), True),
        StructField("PRODUCTLINE", StringType(), True),
        StructField("MSRP", IntegerType(), True),
        StructField("PRODUCTCODE", StringType(), True),
        StructField("CUSTOMERNAME", StringType(), True),
        StructField("PHONE", StringType(), True),
        StructField("ADDRESSLINE1", StringType(), True),
        StructField("ADDRESSLINE2", StringType(), True),
        StructField("CITY", StringType(), True),
        StructField("STATE", StringType(), True),
        StructField("POSTALCODE", StringType(), True),
        StructField("COUNTRY", StringType(), True),
        StructField("TERRITORY", StringType(), True),
        StructField("CONTACTLASTNAME", StringType(), True),
        StructField("CONTACTFIRSTNAME", StringType(), True),
        StructField("DEALSIZE", StringType(), True)
    ])
    return schema

# --- 4. Data Cleaning and Validation ---
def clean_and_validate_data(raw_df, profit_margin_rate):
    """
    Performs data cleaning, validation, and enriches the DataFrame with calculated fields.
    
    Args:
        raw_df (pyspark.sql.DataFrame): The raw DataFrame ingested from the CSV.
        profit_margin_rate (float): The assumed profit margin rate to calculate profit.

    Returns:
        pyspark.sql.DataFrame: The cleaned, validated, and enriched DataFrame.
    """
    logging.info("Starting data cleaning and validation...")

    # Identify and drop columns that are entirely null (e.g., if CSV has empty columns)
    null_columns_check = [
        c for c, count in raw_df.select([sum(col(c).isNull().cast("int")).alias(c) 
        for c in raw_df.columns]).collect()[0].asDict().items() if count == raw_df.count()
    ]
    if null_columns_check:
        logging.info(f"Dropping entirely null columns: {null_columns_check}")
        raw_df = raw_df.drop(*null_columns_check)
    
    # Select and cast columns to their correct types, and rename for consistency
    cleaned_df = raw_df.select(
        col("ORDERNUMBER").cast(IntegerType()).alias("OrderNumber"),
        # Convert ORDERDATE string to timestamp, then to date
        to_timestamp(col("ORDERDATE"), "M/d/yyyy H:mm").cast(DateType()).alias("OrderDate"),
        col("STATUS").cast(StringType()).alias("Status"),
        col("PRODUCTLINE").cast(StringType()).alias("ProductLine"),
        col("QUANTITYORDERED").cast(IntegerType()).alias("QuantityOrdered"),
        col("PRICEEACH").cast(DoubleType()).alias("PriceEach"),
        col("SALES").cast(DoubleType()).alias("Sales"),
        col("DEALSIZE").cast(StringType()).alias("DealSize"),
        col("MSRP").cast(IntegerType()).alias("MSRP"),
        col("PRODUCTCODE").cast(StringType()).alias("ProductCode"),
        col("CUSTOMERNAME").cast(StringType()).alias("CustomerName"),
        col("PHONE").cast(StringType()).alias("Phone"),
        col("ADDRESSLINE1").cast(StringType()).alias("AddressLine1"),
        col("ADDRESSLINE2").cast(StringType()).alias("AddressLine2"),
        col("CITY").cast(StringType()).alias("City"),
        col("STATE").cast(StringType()).alias("State"),
        col("POSTALCODE").cast(StringType()).alias("PostalCode"),
        col("COUNTRY").cast(StringType()).alias("Country"),
        col("TERRITORY").cast(StringType()).alias("Territory"),
        col("CONTACTLASTNAME").cast(StringType()).alias("ContactLastName"),
        col("CONTACTFIRSTNAME").cast(StringType()).alias("ContactFirstName"),
        col("QTR_ID").cast(IntegerType()).alias("QTR_ID"),
        col("MONTH_ID").cast(IntegerType()).alias("MONTH_ID"),
        col("YEAR_ID").cast(IntegerType()).alias("YEAR_ID"),
        col("ORDERLINENUMBER").cast(IntegerType()).alias("OrderLineNumber")
    )

    # Define columns for null value imputation
    numeric_cols = ["QuantityOrdered", "PriceEach", "Sales", "MSRP", "QTR_ID", "MONTH_ID", "YEAR_ID", "OrderLineNumber"]
    string_cols = ["Status", "ProductLine", "DealSize", "CustomerName", "Phone", "AddressLine1",
                   "AddressLine2", "City", "State", "PostalCode", "Country", "Territory",
                   "ContactLastName", "ContactFirstName", "ProductCode"]

    # Fill null numeric columns with 0
    for col_name in numeric_cols:
        if col_name in cleaned_df.columns:
            cleaned_df = cleaned_df.na.fill(0, subset=[col_name])

    # Fill null string columns with "N/A"
    for col_name in string_cols:
        if col_name in cleaned_df.columns:
            cleaned_df = cleaned_df.na.fill("N/A", subset=[col_name])
            
    logging.info("Checking for and removing duplicate records based on OrderNumber, OrderLineNumber, ProductCode...")
    initial_row_count = cleaned_df.count()
    cleaned_df = cleaned_df.dropDuplicates(["OrderNumber", "OrderLineNumber", "ProductCode"])
    deduplicated_row_count = cleaned_df.count()
    if initial_row_count != deduplicated_row_count:
        logging.info(f"Removed {initial_row_count - deduplicated_row_count} duplicate rows.")
    else:
        logging.info("No duplicate rows found.")

    # Filter out invalid records based on critical data points
    cleaned_df = cleaned_df.filter(
        (col("OrderDate").isNotNull()) &
        (col("Sales").isNotNull()) & (col("Sales") >= 0) & # Sales must be non-negative
        (col("QuantityOrdered").isNotNull()) & (col("QuantityOrdered") > 0) & # Quantity must be positive
        (col("MSRP").isNotNull()) & (col("MSRP") > 0) # MSRP must be positive
    )
    logging.info(f"Filtered out records with invalid dates or critical missing/invalid numeric values. Remaining records: {cleaned_df.count()}")

    # Calculate 'DISCOUNT' if not present. Assuming discount is (MSRP * Quantity - Sales) / (MSRP * Quantity)
    if "DISCOUNT" not in cleaned_df.columns:
        logging.info("Calculating 'DISCOUNT' column from 'SALES', 'QUANTITYORDERED', and 'MSRP'.")
        # Ensure division by zero is handled; if denominator is 0, discount will be 1 (100%)
        # Using lit(1) - (sales / (qty * msrp)) to get the discount rate
        cleaned_df = cleaned_df.withColumn(
            "DISCOUNT",
            lit(1) - (col("SALES") / (col("QUANTITYORDERED") * col("MSRP")))
        ).withColumn(
            "DISCOUNT",
            (col("DISCOUNT")).cast(DoubleType()) # Cast to DoubleType explicitly
        )
        # Ensure discount is not negative (e.g., if sales > MSRP * QTY due to errors)
        cleaned_df = cleaned_df.withColumn("DISCOUNT", greatest(lit(0.0), col("DISCOUNT")))
        
    # Add 'COMMISSION' column if not present (defaulting to 0.0)
    if "COMMISSION" not in cleaned_df.columns:
        logging.warning("'COMMISSION' column not found. Adding with default value 0.0.")
        cleaned_df = cleaned_df.withColumn("COMMISSION", lit(0.0).cast(DoubleType()))

    # Calculate 'Profit' based on sales, discount, and an assumed profit margin rate
    logging.info(f"Adding 'Profit' column with assumed profit margin rate of {profit_margin_rate * 100}%.")
    cleaned_df = cleaned_df.withColumn("Profit", col("Sales") * (1 - col("DISCOUNT")) * lit(profit_margin_rate))
    
    logging.info("Data cleaning and validation complete. Added 'Profit' and calculated 'DISCOUNT' column.")

    # Cache the cleaned DataFrame for faster subsequent operations
    cleaned_df.cache()
    logging.info("Cleaned DataFrame cached.")
    return cleaned_df

# --- 5. Generate Meaningful Aggregations ---
def generate_aggregations(cleaned_df):
    """
    Generates meaningful aggregated datasets from the cleaned DataFrame.
    
    Args:
        cleaned_df (pyspark.sql.DataFrame): The cleaned and enriched DataFrame.

    Returns:
        dict: A dictionary containing various aggregated DataFrames.
    """
    logging.info("Starting generation of meaningful aggregations...")

    # Monthly Sales by Region
    monthly_sales_by_region = cleaned_df.withColumn("SalesMonth", month(col("OrderDate"))) \
                                        .withColumn("SalesYear", year(col("OrderDate"))) \
                                        .groupBy("SalesYear", "SalesMonth", "Country") \
                                        .agg(round(sum("Sales"), 2).alias("TotalSales")) \
                                        .orderBy("SalesYear", "SalesMonth", desc("TotalSales"))
    logging.info("Monthly Sales by Region calculated.")

    # Top 10 Customers by Profit
    top_10_customers_by_profit = cleaned_df.groupBy("CustomerName") \
                                           .agg(round(sum("Profit"), 2).alias("TotalProfit")) \
                                           .orderBy(desc("TotalProfit")) \
                                           .limit(10)
    logging.info("Top 10 Customers by Profit calculated.")
    # top_10_customers_by_profit.show() # Debug: Show content before saving
    
    # Category-wise Average Discount
    category_wise_avg_discount = cleaned_df.groupBy("ProductLine") \
                                           .agg(round(avg("DISCOUNT"), 4).alias("AverageDiscount")) \
                                           .orderBy(desc("AverageDiscount"))
    logging.info("Category-wise Average Discount calculated.")
    # category_wise_avg_discount.show() # Debug: Show content before saving

    return {
        "monthly_sales_by_region": monthly_sales_by_region,
        "top_10_customers_by_profit": top_10_customers_by_profit,
        "category_wise_avg_discount": category_wise_avg_discount
    }

# --- 6. Run Reporting Queries (using Spark SQL) ---
def run_reporting_queries(cleaned_df, sales_threshold):
    """
    Runs various reporting queries using Spark SQL on the cleaned DataFrame.
    
    Args:
        cleaned_df (pyspark.sql.DataFrame): The cleaned and enriched DataFrame.
        sales_threshold (float): The sales value threshold for high-value orders.

    Returns:
        dict: A dictionary containing DataFrames resulting from the reporting queries.
    """
    logging.info("Starting execution of reporting queries...")

    # Register the cleaned DataFrame as a temporary view for Spark SQL queries
    cleaned_df.createOrReplaceTempView("sales_data")
    logging.info("Cleaned data registered as temporary view 'sales_data' for Spark SQL.")

    # Sales above a specified threshold
    sales_above_threshold = spark.sql(f"""
        SELECT
            OrderNumber,
            OrderDate,
            CustomerName,
            ProductLine,
            Sales
        FROM
            sales_data
        WHERE
            Sales > {sales_threshold}
        ORDER BY
            Sales DESC
    """)
    logging.info(f"Sales above {sales_threshold} query executed.")

    # Profitable Categories (categories with total profit > 0)
    profitable_categories = spark.sql("""
        SELECT
            ProductLine,
            ROUND(SUM(Profit), 2) AS TotalProfit
        FROM
            sales_data
        GROUP BY
            ProductLine
        HAVING
            SUM(Profit) > 0
        ORDER BY
            TotalProfit DESC
    """)
    logging.info("Profitable Categories query executed.")

    # Customers with high discount usage (average discount > 10%)
    customers_high_discount = spark.sql("""
        SELECT
            CustomerName,
            ROUND(AVG(DISCOUNT), 4) AS AverageDiscountUsed,
            ROUND(SUM(Sales), 2) AS TotalSales
        FROM
            sales_data
        GROUP BY
            CustomerName
        HAVING
            AVG(DISCOUNT) > 0.10
        ORDER BY
            AverageDiscountUsed DESC
    """)
    logging.info("Customers with High Discount Usage query executed.")

    return {
        "sales_above_threshold": sales_above_threshold,
        "profitable_categories": profitable_categories,
        "customers_high_discount": customers_high_discount
    }

# --- 7. Store Transformed Output in SQLite ---
def store_output_to_sqlite(dataframes_to_store, db_name):
    """
    Stores a dictionary of PySpark DataFrames (converted to Pandas) as tables
    in a local SQLite database.
    
    Args:
        dataframes_to_store (dict): A dictionary where keys are table names
                                    and values are PySpark DataFrames.
        db_name (str): The name of the SQLite database file.
    """
    logging.info(f"Storing transformed outputs to SQLite database: {db_name}...")
    # Remove existing DB file to ensure a clean slate
    if os.path.exists(db_name):
        os.remove(db_name)
        logging.info(f"Removed existing database file: {db_name}")

    conn = None
    try:
        conn = sqlite3.connect(db_name)
        for table_name, df in dataframes_to_store.items():
            logging.info(f"Storing '{table_name}' table...")
            # Convert PySpark DataFrame to Pandas DataFrame before storing to SQLite
            df.toPandas().to_sql(table_name, conn, if_exists="replace", index=False)
            logging.info(f"Stored '{table_name}' table.")
        logging.info("All transformed outputs stored successfully in SQLite.")
    except Exception as e:
        logging.error(f"Error storing data to SQLite: {e}", exc_info=True)
    finally:
        if conn:
            conn.close() # Ensure the connection is closed
            logging.info("SQLite connection closed.")

# --- 8. Store Transformed Output as Partitioned Parquet Files ---
def store_output_to_parquet(dataframes_to_store, output_base_path="output/parquet"):
    """
    Stores a dictionary of PySpark DataFrames as partitioned Parquet files.
    
    Args:
        dataframes_to_store (dict): A dictionary where keys are directory names
                                    and values are PySpark DataFrames.
        output_base_path (str): The base directory to store the Parquet files.
    """
    logging.info(f"Storing transformed outputs as partitioned Parquet files to '{output_base_path}'...")
    
    os.makedirs(output_base_path, exist_ok=True) # Ensure the output directory exists

    for table_name, df in dataframes_to_store.items():
        output_path = os.path.join(output_base_path, table_name)
        logging.info(f"Storing '{table_name}' to Parquet at '{output_path}'...")
        try:
            # Partition by year and month if 'OrderDate' or 'SalesYear'/'SalesMonth' columns exist
            if "OrderDate" in df.columns:
                df_with_ym = df.withColumn("year", year(col("OrderDate"))) \
                               .withColumn("month", month(col("OrderDate")))
                df_with_ym.write.mode("overwrite").partitionBy("year", "month").parquet(output_path)
            elif "SalesYear" in df.columns and "SalesMonth" in df.columns:
                df.write.mode("overwrite").partitionBy("SalesYear", "SalesMonth").parquet(output_path)
            else:
                df.write.mode("overwrite").parquet(output_path) # Store without partitioning
            logging.info(f"Stored '{table_name}' to Parquet successfully.")
        except Exception as e:
            logging.error(f"Error storing '{table_name}' to Parquet: {e}", exc_info=True)

# --- Main Execution ---
if __name__ == "__main__":
    # Configuration parameters
    KAGGLE_DATASET_REF = "kyanyoga/sample-sales-data"
    CSV_FILE_NAME = "sales_data_sample.csv"
    DB_NAME = "sales_analytics.db"
    PARQUET_OUTPUT_DIR = "output/parquet"
    SALES_THRESHOLD = 5000 # Threshold for high-value orders
    PROFIT_MARGIN_RATE = 0.30 # Assumed profit margin for calculations

    spark = None # Initialize spark to None
    try:
        spark = initialize_spark_session()

        # Ingest raw data from Kaggle
        raw_df = ingest_data(spark, KAGGLE_DATASET_REF, CSV_FILE_NAME, define_schema())
        
        # Exit if raw DataFrame is not loaded successfully
        if raw_df is None:
            logging.error("Raw DataFrame is None. Exiting pipeline.")
            exit(1)

        # Clean and validate the raw data
        cleaned_df = clean_and_validate_data(raw_df, PROFIT_MARGIN_RATE)

        # Generate aggregated datasets
        aggregations = generate_aggregations(cleaned_df)
        
        # Run reporting queries
        reporting_queries = run_reporting_queries(cleaned_df, SALES_THRESHOLD)

        # Combine all DataFrames for storage
        all_output_dfs = {**aggregations, **reporting_queries}

        # Store the transformed data in SQLite for the Flask API
        store_output_to_sqlite(all_output_dfs, DB_NAME)

        # Store the transformed data as Parquet files for potential future use/analysis
        store_output_to_parquet(all_output_dfs, PARQUET_OUTPUT_DIR)

    except Exception as e:
        # Catch any unhandled exceptions during pipeline execution
        logging.critical(f"An unhandled error occurred in the main pipeline execution: {e}", exc_info=True)
    finally:
        # Ensure Spark Session is stopped even if errors occur
        if spark:
            spark.stop()
            logging.info("Spark Session stopped.")
