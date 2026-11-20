"""Download the Zillow Research CSVs used by the pipeline and (optionally) upload them to S3 raw/.

  python scripts/download_zillow.py --out Data                 # download only
  python scripts/download_zillow.py --out Data --bucket NAME   # download + upload to s3://NAME/raw/
"""
import argparse
import pathlib

import requests

BASE = "https://files.zillowstatic.com/research/public_csvs"
FILES = [
    "zhvi/City_zhvi_uc_sfrcondo_tier_0.0_0.33_sm_sa_month.csv",
    "zhvi/City_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv",
    "zhvi/City_zhvi_uc_sfrcondo_tier_0.67_1.0_sm_sa_month.csv",
    "zori/Zip_zori_uc_sfrcondomfr_sm_sa_month.csv",
    "total_monthly_payment/Metro_total_monthly_payment_downpayment_0.20_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv",
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="Data")
    ap.add_argument("--bucket", help="if set, upload each file to s3://BUCKET/raw/raw_<name>")
    a = ap.parse_args()
    out = pathlib.Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    s3 = None
    if a.bucket:
        import boto3
        s3 = boto3.client("s3")
    for rel in FILES:
        name = "raw_" + rel.split("/")[-1]
        dest = out / name
        print(f"Downloading {rel}")
        with requests.get(f"{BASE}/{rel}", stream=True, timeout=120) as r:
            r.raise_for_status()
            with open(dest, "wb") as f:
                for chunk in r.iter_content(1 << 20):
                    f.write(chunk)
        if s3:
            s3.upload_file(str(dest), a.bucket, f"raw/{name}")
            print(f"  uploaded to s3://{a.bucket}/raw/{name}")


if __name__ == "__main__":
    main()
