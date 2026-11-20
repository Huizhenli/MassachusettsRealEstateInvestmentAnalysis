"""Glue Python-shell job: raw Zillow CSVs -> MA-only, gap-interpolated CSVs in cleaned/.

Also creates the `landing` schema in Postgres so the Spark jobs can write to it.
The pure function `clean_frame` has no AWS dependencies so it can be unit-tested locally.
"""
import io
import sys

import pandas as pd

# (state column, raw file, cleaned file). Cleaned names keep the substrings the Spark
# jobs glob on: 'zhvi' + tier, 'zori', 'total_monthly_payment'.
FILES = [
    ("State", "raw_City_zhvi_uc_sfrcondo_tier_0.0_0.33_sm_sa_month.csv", "cleaned_City_zhvi_tier_0.0_0.33.csv"),
    ("State", "raw_City_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv", "cleaned_City_zhvi_tier_0.33_0.67.csv"),
    ("State", "raw_City_zhvi_uc_sfrcondo_tier_0.67_1.0_sm_sa_month.csv", "cleaned_City_zhvi_tier_0.67_1.0.csv"),
    ("StateName", "raw_Metro_total_monthly_payment_downpayment_0.20_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv",
     "cleaned_Metro_total_monthly_payment.csv"),
    ("State", "raw_Zip_zori_uc_sfrcondomfr_sm_sa_month.csv", "cleaned_Zip_zori.csv"),
]
STATE = "MA"


def clean_frame(df: pd.DataFrame, state_col: str, state: str = STATE) -> pd.DataFrame:
    """Keep one state, trim strings, linearly interpolate interior gaps across months."""
    df = df[df[state_col] == state].copy()
    df = df.apply(lambda s: s.str.strip() if pd.api.types.is_string_dtype(s) else s)
    # date columns are everything that parses as a date; id columns come first
    date_cols = [c for c in df.columns if _is_date(c)]
    df[date_cols] = (
        df[date_cols].astype(float).interpolate(method="linear", axis=1, limit_area="inside")
    )
    return df.reset_index(drop=True)


def _is_date(name: str) -> bool:
    try:
        pd.to_datetime(name, format="%Y-%m-%d")
        return True
    except (ValueError, TypeError):
        return False


def create_landing_schema(host, user, password, dbname="postgres"):
    import psycopg2

    conn = psycopg2.connect(host=host, database=dbname, user=user, password=password)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS landing;")
        print("Schema 'landing' verified/created.")
    finally:
        conn.close()


def main():
    import boto3
    from awsglue.utils import getResolvedOptions

    args = getResolvedOptions(sys.argv, ["DB_ENDPOINT", "DB_USER", "DB_PASSWORD", "BUCKET_NAME"])
    bucket = args["BUCKET_NAME"]
    create_landing_schema(args["DB_ENDPOINT"], args["DB_USER"], args["DB_PASSWORD"])

    s3 = boto3.client("s3")
    for state_col, raw_name, clean_name in FILES:
        print(f"Cleaning {raw_name}")
        body = s3.get_object(Bucket=bucket, Key=f"raw/{raw_name}")["Body"].read()
        cleaned = clean_frame(pd.read_csv(io.BytesIO(body)), state_col)
        out = io.StringIO()
        cleaned.to_csv(out, index=False)
        s3.put_object(Bucket=bucket, Key=f"cleaned/{clean_name}", Body=out.getvalue().encode())
        print(f"  -> cleaned/{clean_name} ({len(cleaned)} rows)")


if __name__ == "__main__":
    main()
