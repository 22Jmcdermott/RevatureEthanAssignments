
# Import the SparkSession class, which is the entry point for working with Spark.
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, BooleanType

# Create and configure a Spark session.
spark = (
    SparkSession.builder

    # Set a name for the Spark application (shows up in Spark UI/logs).
    .appName("Project_1")

    # Enable Apache Iceberg SQL extensions so Spark understands
    # Iceberg-specific SQL commands and table operations.
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )

    # Register a catalog named "glue_catalog".
    # Spark will use this catalog whenever tables are referenced with
    # the prefix "glue_catalog".
    .config(
        "spark.sql.catalog.glue_catalog",
        "org.apache.iceberg.spark.SparkCatalog"
    )

    # Tell Spark that this catalog should use AWS Glue
    # as the metadata store for Iceberg tables.
    .config(
        "spark.sql.catalog.glue_catalog.catalog-impl",
        "org.apache.iceberg.aws.glue.GlueCatalog"
    )

    # Specify the S3 warehouse location where Iceberg table data
    # and metadata files will be stored.
    .config(
        "spark.sql.catalog.glue_catalog.warehouse",
        "s3://test-bucket-484056256290-us-east-1-an/iceberg/"
    )

    # Configure Iceberg to use the S3FileIO implementation
    # for reading and writing data in Amazon S3.
    .config(
        "spark.sql.catalog.glue_catalog.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO"
    )

    # Dirty source files contain mixed/invalid date patterns; disable ANSI
    # parsing exceptions so bad casts become null and can be filtered.
    .config("spark.sql.ansi.enabled", "false")

    # Create the Spark session with all of the above settings.
    .getOrCreate()
)

orders_schema = StructType([
    StructField('order_id', StringType()),
    StructField('customer_id', StringType()),
    StructField('product_id', StringType()),
    StructField('order_date', StringType()),
    StructField('ship_date', StringType()),
    StructField('quantity', StringType()),
    StructField('unit_price', StringType()),
    StructField('discount_pct', StringType()),
    StructField('total_amount', StringType()),
    StructField('payment_method', StringType()),
    StructField('order_status', StringType())
])

products_schema = StructType([
    StructField('product_id', StringType()),
    StructField('product_name', StringType()),
    StructField('category', StringType()),
    StructField('brand', StringType()),
    StructField('price', StringType()),
    StructField('cost', StringType()),
    StructField('stock_quantity', StringType()),
    StructField('weight_kg', StringType()),
    StructField('created_date', StringType()),
    StructField('is_active', StringType())
])

customers_schema = StructType([
    StructField('customer_id', IntegerType()),
    StructField('first_name', StringType()),
    StructField('last_name', StringType()),
    StructField('email', StringType()),
    StructField('phone', StringType()),
    StructField('signup_date', DateType()),
    StructField('country', StringType()),
    StructField('state', StringType()),
    StructField('postal_code', StringType()),
    StructField('is_active', BooleanType()),
    StructField('loyalty_points', IntegerType())
])


# Read the dataset from the csv file stored in S3
# and load it into a Spark DataFrame.
orders_df = (
    spark.read
    .option("header", True)
    .schema(orders_schema)
    .csv("s3://test-bucket-484056256290-us-east-1-an/orders.csv")
)

products_df = (
    spark.read
    .option("header", True)
    .schema(products_schema)
    .csv("s3://test-bucket-484056256290-us-east-1-an/products.csv")
)

customers_df = (
    spark.read
    .option("header", True)
    .schema(customers_schema)
    .csv("s3://test-bucket-484056256290-us-east-1-an/customers.csv")
)

# Display the DataFrame's schema (column names and data types)
# to verify the data was loaded correctly.
orders_df.printSchema()
products_df.printSchema()
customers_df.printSchema()


# Create an Iceberg database (namespace) in AWS Glue if it
# doesn't already exist.
spark.sql("""
CREATE DATABASE IF NOT EXISTS glue_catalog.iceberg_catalog_db
""")

# Remove duplicate order IDs
orders_df_clean = orders_df.dropDuplicates(["order_id"])

# Trim all string columns and convert common null-like values to null
orders_null_tokens = ["", "null", "n/a", "na", "none"]
for field in orders_df_clean.schema.fields:
    if isinstance(field.dataType, StringType):
        trimmed_value = F.trim(F.col(field.name))

        orders_df_clean = orders_df_clean.withColumn(
            field.name,
            F.when(
                F.lower(trimmed_value).isin(orders_null_tokens),
                None
            ).otherwise(trimmed_value)
        )

# Keep only numeric customer IDs and normalize order/product identifiers
orders_df_clean = orders_df_clean.withColumn(
    "customer_id",
    F.when(F.col("customer_id").rlike(r"^\d+$"), F.col("customer_id"))
)

orders_df_clean = orders_df_clean.withColumn(
    "order_id",
    F.when(F.col("order_id").rlike(r"^\d+$"), F.col("order_id"))
)

orders_df_clean = orders_df_clean.withColumn(
    "product_id",
    F.when(F.col("product_id").rlike(r"^P\d+$"), F.upper(F.col("product_id")))
)

# Parse multiple date formats into Spark DateType
orders_df_clean = orders_df_clean.withColumn(
    "order_date",
    F.coalesce(
        F.to_date(F.col("order_date"), "yyyy-MM-dd"),
        F.to_date(F.col("order_date"), "yyyy/MM/dd"),
        F.to_date(F.col("order_date"), "MM-dd-yyyy")
    )
)

orders_df_clean = orders_df_clean.withColumn(
    "ship_date",
    F.coalesce(
        F.to_date(F.col("ship_date"), "yyyy-MM-dd"),
        F.to_date(F.col("ship_date"), "yyyy/MM/dd"),
        F.to_date(F.col("ship_date"), "MM-dd-yyyy")
    )
)

# Remove currency symbols and cast numeric columns
orders_df_clean = orders_df_clean.withColumn(
    "unit_price",
    F.regexp_replace(F.col("unit_price"), r"[^0-9.\-]", "").cast(FloatType())
)

orders_df_clean = orders_df_clean.withColumn(
    "discount_pct",
    F.regexp_replace(F.col("discount_pct"), r"[^0-9.\-]", "").cast(FloatType())
)

orders_df_clean = orders_df_clean.withColumn(
    "total_amount",
    F.regexp_replace(F.col("total_amount"), r"[^0-9.\-]", "").cast(FloatType())
)

orders_df_clean = orders_df_clean.withColumn(
    "quantity",
    F.regexp_replace(F.col("quantity"), r"[^0-9\-]", "").cast(IntegerType())
)

# Apply basic business rules and normalize labels
orders_df_clean = orders_df_clean.withColumn(
    "quantity",
    F.when(F.col("quantity") > 0, F.col("quantity"))
)

orders_df_clean = orders_df_clean.withColumn(
    "unit_price",
    F.when(F.col("unit_price") >= 0, F.col("unit_price"))
)

orders_df_clean = orders_df_clean.withColumn(
    "discount_pct",
    F.when(F.col("discount_pct").between(0, 100), F.col("discount_pct")).otherwise(F.lit(0.0))
)

orders_df_clean = orders_df_clean.withColumn(
    "total_amount",
    F.when(F.col("total_amount") >= 0, F.col("total_amount"))
)

orders_df_clean = orders_df_clean.withColumn(
    "payment_method",
    F.initcap(F.lower(F.col("payment_method")))
)

orders_df_clean = orders_df_clean.withColumn(
    "order_status",
    F.initcap(F.lower(F.col("order_status")))
)

# Keep records with required order fields present
orders_df_clean = orders_df_clean.dropna(
    subset=[
        "order_id",
        "customer_id",
        "product_id",
        "order_date",
        "quantity",
        "unit_price",
        "total_amount"
    ]
)


# Remove duplicate product IDs
products_df_clean = products_df.dropDuplicates(["product_id"])

# Trim all string columns and convert common null-like values to null
products_null_tokens = ["", "null", "n/a", "na", "none", "?"]
for field in products_df_clean.schema.fields:
    if isinstance(field.dataType, StringType):
        trimmed_value = F.trim(F.col(field.name))

        products_df_clean = products_df_clean.withColumn(
            field.name,
            F.when(
                F.lower(trimmed_value).isin(products_null_tokens),
                None
            ).otherwise(trimmed_value)
        )

# Standardize product IDs and text labels
products_df_clean = products_df_clean.withColumn(
    "product_id",
    F.when(F.col("product_id").rlike(r"^P\d+$"), F.upper(F.col("product_id")))
)

products_df_clean = products_df_clean.withColumn("category", F.initcap(F.lower(F.col("category"))))
products_df_clean = products_df_clean.withColumn("brand", F.initcap(F.lower(F.col("brand"))))

# Normalize numeric-like strings and cast to numeric types
products_df_clean = products_df_clean.withColumn(
    "price",
    F.regexp_replace(F.col("price"), ",", ".")
)

products_df_clean = products_df_clean.withColumn(
    "cost",
    F.regexp_replace(F.col("cost"), ",", ".")
)

products_df_clean = products_df_clean.withColumn(
    "weight_kg",
    F.regexp_replace(F.col("weight_kg"), ",", ".")
)

products_df_clean = products_df_clean.withColumn(
    "price",
    F.regexp_replace(F.col("price"), r"[^0-9.\-]", "").cast(FloatType())
)

products_df_clean = products_df_clean.withColumn(
    "cost",
    F.regexp_replace(F.col("cost"), r"[^0-9.\-]", "").cast(FloatType())
)

products_df_clean = products_df_clean.withColumn(
    "stock_quantity",
    F.regexp_replace(F.col("stock_quantity"), r"[^0-9\-]", "").cast(IntegerType())
)

products_df_clean = products_df_clean.withColumn(
    "weight_kg",
    F.regexp_replace(F.col("weight_kg"), r"[^0-9.\-]", "").cast(FloatType())
)

# Parse created date and normalize boolean flags
products_df_clean = products_df_clean.withColumn(
    "created_date",
    F.coalesce(
        F.to_date(F.col("created_date"), "yyyy-MM-dd"),
        F.to_date(F.col("created_date"), "yyyy/MM/dd")
    )
)

products_df_clean = products_df_clean.withColumn(
    "is_active",
    F.when(F.lower(F.col("is_active")).isin("true", "1", "yes", "y"), F.lit(True))
    .when(F.lower(F.col("is_active")).isin("false", "0", "no", "n"), F.lit(False))
)

# Apply basic business rules
products_df_clean = products_df_clean.withColumn(
    "price",
    F.when(F.col("price") >= 0, F.col("price"))
)

products_df_clean = products_df_clean.withColumn(
    "cost",
    F.when(F.col("cost") >= 0, F.col("cost"))
)

products_df_clean = products_df_clean.withColumn(
    "stock_quantity",
    F.when(F.col("stock_quantity") >= 0, F.col("stock_quantity"))
)

products_df_clean = products_df_clean.withColumn(
    "weight_kg",
    F.when(F.col("weight_kg") > 0, F.col("weight_kg"))
)

# Keep records with required product fields present
products_df_clean = products_df_clean.dropna(
    subset=[
        "product_id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "created_date"
    ]
)

# Remove duplicate customer IDs
customers_df_clean = customers_df.dropDuplicates(["customer_id"])

# Trim all string columns and convert blank strings to null
for field in customers_df_clean.schema.fields:
    if isinstance(field.dataType, StringType):
        trimmed_value = F.trim(F.col(field.name))

        customers_df_clean = customers_df_clean.withColumn(
            field.name,
            F.when(
                trimmed_value == "",
                None
            ).otherwise(trimmed_value)
        )

# Validate and normalize email addresses
email_pattern = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"

customers_df_clean = customers_df_clean.withColumn(
    "email",
    F.when(
        F.col("email").rlike(email_pattern),
        F.lower(F.col("email"))
    )
)

# Remove non-digit characters from phone numbers
customers_df_clean = customers_df_clean.withColumn(
    "phone",
    F.regexp_replace(F.col("phone"), r"\D", "")
)

# Keep phone numbers containing between 10 and 15 digits
customers_df_clean = customers_df_clean.withColumn(
    "phone",
    F.when(
        F.length(F.col("phone")).between(10, 15),
        F.col("phone")
    )
)

# Replace negative loyalty points with zero
customers_df_clean = customers_df_clean.withColumn(
    "loyalty_points",
    F.when(
        F.col("loyalty_points") < 0,
        F.lit(0)
    ).otherwise(F.col("loyalty_points"))
)

# Remove records missing required customer information
customers_df_clean = customers_df_clean.dropna(
    subset=[
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "signup_date"
    ]
)

# Keep only orders with valid foreign keys in curated customer/product datasets
valid_customer_keys = customers_df_clean.select(
    F.col("customer_id").cast(StringType()).alias("customer_id")
).distinct()

valid_product_keys = products_df_clean.select("product_id").distinct()

orders_df_clean = orders_df_clean.join(valid_customer_keys, ["customer_id"], "inner")
orders_df_clean = orders_df_clean.join(valid_product_keys, ["product_id"], "inner")

# Create a curated joined dataset with calculated sales fields
sales_enriched_df = (
    orders_df_clean.alias("o")
    .join(
        customers_df_clean.alias("c"),
        F.col("o.customer_id") == F.col("c.customer_id").cast(StringType()),
        "inner"
    )
    .join(
        products_df_clean.alias("p"),
        F.col("o.product_id") == F.col("p.product_id"),
        "inner"
    )
    .select(
        F.col("o.order_id"),
        F.col("o.order_date"),
        F.col("o.ship_date"),
        F.col("o.customer_id"),
        F.col("c.first_name"),
        F.col("c.last_name"),
        F.col("c.email"),
        F.col("c.country"),
        F.col("c.state"),
        F.col("o.product_id"),
        F.col("p.product_name"),
        F.col("p.category"),
        F.col("p.brand"),
        F.col("o.quantity"),
        F.col("o.unit_price"),
        F.col("o.discount_pct"),
        F.col("o.total_amount"),
        F.col("o.payment_method"),
        F.col("o.order_status")
    )
    .withColumn("gross_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
    .withColumn("discount_amount", F.round(F.col("gross_amount") * (F.col("discount_pct") / F.lit(100.0)), 2))
    .withColumn("net_amount", F.round(F.col("gross_amount") - F.col("discount_amount"), 2))
)

# Generate summary metrics for monthly product-category sales
monthly_sales_summary_df = (
    sales_enriched_df
    .withColumn("order_month", F.date_format(F.col("order_date"), "yyyy-MM"))
    .groupBy("order_month", "category")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("quantity").alias("total_units_sold"),
        F.round(F.sum("gross_amount"), 2).alias("gross_revenue"),
        F.round(F.sum("discount_amount"), 2).alias("total_discount"),
        F.round(F.sum("net_amount"), 2).alias("net_revenue"),
        F.round(F.avg("net_amount"), 2).alias("avg_order_value")
    )
)




# Write the DataFrame as an Iceberg table.
(
    orders_df_clean.writeTo(
        # Fully qualified table name:
        # catalog.database.table
        "glue_catalog.iceberg_catalog_db.orders"
    )

    # Specify that the table format should be Apache Iceberg.
    .using("iceberg")

    # Create the table if it doesn't exist.
    # If it already exists, replace it with the new data.
    .createOrReplace()
)

(
    products_df_clean.writeTo(
        # Fully qualified table name:
        # catalog.database.table
        "glue_catalog.iceberg_catalog_db.products"
    )

    # Specify that the table format should be Apache Iceberg.
    .using("iceberg")

    # Create the table if it doesn't exist.
    # If it already exists, replace it with the new data.
    .createOrReplace()
)

(
    sales_enriched_df.writeTo(
        # Fully qualified table name:
        # catalog.database.table
        "glue_catalog.iceberg_catalog_db.sales_enriched"
    )

    # Specify that the table format should be Apache Iceberg.
    .using("iceberg")

    # Create the table if it doesn't exist.
    # If it already exists, replace it with the new data.
    .createOrReplace()
)

(
    monthly_sales_summary_df.writeTo(
        # Fully qualified table name:
        # catalog.database.table
        "glue_catalog.iceberg_catalog_db.monthly_sales_summary"
    )

    # Specify that the table format should be Apache Iceberg.
    .using("iceberg")

    # Create the table if it doesn't exist.
    # If it already exists, replace it with the new data.
    .createOrReplace()
)

(
    customers_df_clean.writeTo(
        # Fully qualified table name:
        # catalog.database.table
        "glue_catalog.iceberg_catalog_db.customers"
    )

    # Specify that the table format should be Apache Iceberg.
    .using("iceberg")

    # Create the table if it doesn't exist.
    # If it already exists, replace it with the new data.
    .createOrReplace()
)

# Export curated datasets to deterministic Parquet folders for Snowflake COPY.
snowflake_export_base_path = "s3://test-bucket-484056256290-us-east-1-an/snowflake_exports"

orders_df_clean.write.mode("overwrite").parquet(f"{snowflake_export_base_path}/orders")
products_df_clean.write.mode("overwrite").parquet(f"{snowflake_export_base_path}/products")
customers_df_clean.write.mode("overwrite").parquet(f"{snowflake_export_base_path}/customers")
sales_enriched_df.write.mode("overwrite").parquet(f"{snowflake_export_base_path}/sales_enriched")
monthly_sales_summary_df.write.mode("overwrite").parquet(f"{snowflake_export_base_path}/monthly_sales_summary")


# Query the newly created Iceberg table to verify that the
# data was written successfully.
spark.sql("""
SELECT *
FROM glue_catalog.iceberg_catalog_db.customers
LIMIT 10
""").show()

# Validate source-to-curated record counts
print("orders source count:", orders_df.count())
print("orders curated count:", orders_df_clean.count())
print("products source count:", products_df.count())
print("products curated count:", products_df_clean.count())
print("customers source count:", customers_df.count())
print("customers curated count:", customers_df_clean.count())
print("sales_enriched curated count:", sales_enriched_df.count())


# Stop the Spark session and release cluster resources.
spark.stop()