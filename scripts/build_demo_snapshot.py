"""Export the final tables to small gzip CSVs so the dashboard can run without a database (demo mode).

  python scripts/build_demo_snapshot.py --dsn postgresql+psycopg2://user:pw@host:5432/postgres
Writes data/demo/<table>.csv.gz (committed to the repo).
"""
import argparse
import pathlib

import pandas as pd
from sqlalchemy import create_engine

TABLES = {
    "dim_geography": "select geo_key, metro_name, county_name from dim_geography",
    "fact_house_value": "select geo_key, home_tier, avg_house_value, metric_date, year from fact_house_value",
    "fact_rental_income": "select geo_key, rental_income, metric_date, year from fact_rental_income",
    "fact_monthly_payment": "select geo_key, monthly_payment, metric_date, year from fact_monthly_payment",
    "fact_zip_rent": "select zip_code, city, county_name, metro_name, rental_income, metric_date, year from fact_zip_rent",
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", required=True)
    ap.add_argument("--out", default="data/demo")
    a = ap.parse_args()
    out = pathlib.Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    eng = create_engine(a.dsn)
    for name, sql in TABLES.items():
        df = pd.read_sql(sql, eng)
        df.to_csv(out / f"{name}.csv.gz", index=False, float_format="%.2f")
        print(f"{name}: {len(df)} rows")


if __name__ == "__main__":
    main()
