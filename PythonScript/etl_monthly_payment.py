import sys
import re
import json
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, expr, to_date, current_timestamp, year, coalesce, lit, when, trim, split, input_file_name


def to_snake_case(name):
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def short_metro(c):
    """Boston-Cambridge-Newton, MA-NH or Boston, MA -> Boston; null -> Non-metro."""
    name = trim(split(split(c, ",").getItem(0), "-").getItem(0))
    return when(c.isNull() | (trim(c) == ""), lit("Non-metro")).otherwise(name)


args = getResolvedOptions(sys.argv, ["JOB_NAME", "DB_ENDPOINT", "SECRET_NAME", "BUCKET_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# DB credentials come from AWS Secrets Manager, not job arguments
creds = json.loads(boto3.client("secretsmanager").get_secret_value(SecretId=args["SECRET_NAME"])["SecretString"])

df_raw = spark.read.csv(f"s3://{args['BUCKET_NAME']}/cleaned/*total_monthly_payment*.csv", header=True, inferSchema=True)

id_cols = ["RegionID", "SizeRank", "RegionName", "RegionType", "StateName"]
date_cols = [c for c in df_raw.columns if c not in id_cols]
# cast every month to double: an all-empty month would otherwise be inferred as string and break stack()
stack_expr = f"stack({len(date_cols)}, " + ", ".join([f"'{c}', CAST(`{c}` AS DOUBLE)" for c in date_cols]) + ") as (date_str, monthly_payment)"

df = (
    df_raw.select(*id_cols, expr(stack_expr))
    .withColumn("metric_date", coalesce(to_date(col("date_str"), "yyyy-MM-dd"), to_date(col("date_str"), "M/d/yyyy")))
    .withColumn("monthly_payment", col("monthly_payment").cast("double"))
    .withColumn("metro_name", short_metro(col("RegionName")))
    .drop("date_str")
    .dropna(subset=["monthly_payment"])
    .withColumn("year", year(col("metric_date")))
    .withColumn("inserted_at", current_timestamp())
)

for c in df.columns:
    df = df.withColumnRenamed(c, to_snake_case(c))

(df.write.format("jdbc")
 .option("url", f"jdbc:postgresql://{args['DB_ENDPOINT']}:5432/postgres")
 .option("dbtable", "landing.lnd_monthly_payment")
 .option("user", creds["username"]).option("password", creds["password"])
 .option("driver", "org.postgresql.Driver").mode("overwrite").save())

job.commit()

