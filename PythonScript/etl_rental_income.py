import sys
import re
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import lpad, col, expr, to_date, current_timestamp, year, coalesce, lit, when, trim, split, input_file_name


def to_snake_case(name):
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def short_metro(c):
    """Boston-Cambridge-Newton, MA-NH or Boston, MA -> Boston; null -> Non-metro."""
    name = trim(split(split(c, ",").getItem(0), "-").getItem(0))
    return when(c.isNull() | (trim(c) == ""), lit("Non-metro")).otherwise(name)


args = getResolvedOptions(sys.argv, ["JOB_NAME", "DB_ENDPOINT", "DB_USER", "DB_PASSWORD", "BUCKET_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

df_raw = spark.read.csv(f"s3://{args['BUCKET_NAME']}/cleaned/*zori*.csv", header=True, inferSchema=True)

id_cols = ["RegionID", "SizeRank", "RegionName", "RegionType", "StateName", "State", "City", "Metro", "CountyName"]
date_cols = [c for c in df_raw.columns if c not in id_cols]
stack_expr = f"stack({len(date_cols)}, " + ", ".join([f"'{c}', `{c}`" for c in date_cols]) + ") as (date_str, rental_income)"

df = (
    df_raw.select(*id_cols, expr(stack_expr))
    .withColumn("metric_date", coalesce(to_date(col("date_str"), "yyyy-MM-dd"), to_date(col("date_str"), "M/d/yyyy")))
    .withColumn("rental_income", col("rental_income").cast("double"))
    .withColumn("metro_name", short_metro(col("Metro")))
    .withColumn("zip_code", lpad(col("RegionName").cast("string"), 5, "0"))  # restore leading zeros (MA ZIPs start with 0)
    .drop("date_str")
    .dropna(subset=["rental_income"])
    .withColumn("year", year(col("metric_date")))
    .withColumn("inserted_at", current_timestamp())
)

for c in df.columns:
    df = df.withColumnRenamed(c, to_snake_case(c))

(df.write.format("jdbc")
 .option("url", f"jdbc:postgresql://{args['DB_ENDPOINT']}:5432/postgres")
 .option("dbtable", "landing.lnd_rental_income")
 .option("user", args["DB_USER"]).option("password", args["DB_PASSWORD"])
 .option("driver", "org.postgresql.Driver").mode("overwrite").save())

job.commit()

