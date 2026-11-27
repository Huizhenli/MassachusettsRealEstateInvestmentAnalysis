"""Runs the real Glue Spark scripts locally on tiny fixtures.

AWS pieces are stubbed (awsglue, boto3/Secrets Manager, s3:// paths) and the JDBC write is
captured instead of hitting a database, so the transformation logic itself is what's tested.
Skipped automatically when pyspark/Java are not available.
"""
import json
import pathlib
import runpy
import sys
import types

import pandas as pd
import pytest

pyspark = pytest.importorskip("pyspark")
SCRIPTS = pathlib.Path(__file__).resolve().parents[1] / "PythonScript"


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    try:
        s = SparkSession.builder.master("local[1]").appName("ma-tests") \
            .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1").getOrCreate()
    except Exception as e:  # no Java etc.
        pytest.skip(f"Spark unavailable: {e}")
    yield s
    s.stop()


@pytest.fixture()
def cleaned(tmp_path):
    d = tmp_path / "cleaned"
    d.mkdir()
    dates = ["2014-12-31", "2015-01-31", "2015-02-28"]

    def zhvi(tier, vals):
        pd.DataFrame({
            "RegionID": [1, 2], "SizeRank": [1, 2], "RegionName": ["Boston", "Nantucket"], "RegionType": ["city"] * 2,
            "StateName": ["MA"] * 2, "State": ["MA"] * 2,
            "Metro": ["Boston-Cambridge-Newton, MA-NH", None], "CountyName": ["Suffolk County", "Nantucket County"],
            dates[0]: [100.0, 1.0], dates[1]: [vals, 2.0], dates[2]: [None, 3.0],
        }).to_csv(d / f"cleaned_City_zhvi_tier_{tier}.csv", index=False)

    zhvi("0.0_0.33", 200.0); zhvi("0.33_0.67", 300.0); zhvi("0.67_1.0", 400.0)
    pd.DataFrame({"RegionID": [9], "SizeRank": [1], "RegionName": ["Boston, MA"], "RegionType": ["msa"],
                  "StateName": ["MA"], dates[1]: [2500.0], dates[2]: [2510.0]}) \
        .to_csv(d / "cleaned_Metro_total_monthly_payment.csv", index=False)
    pd.DataFrame({"RegionID": [2108], "SizeRank": [1], "RegionName": [2108], "RegionType": ["zip"],
                  "StateName": ["MA"], "State": ["MA"], "City": ["Boston"],
                  "Metro": ["Boston-Cambridge-Newton, MA-NH"], "CountyName": ["Suffolk County"],
                  dates[1]: [3000.0], dates[2]: [None]}).to_csv(d / "cleaned_Zip_zori.csv", index=False)
    return tmp_path


def run_job(script, spark, root, monkeypatch):
    """Execute a Glue script with stubs; return {table: pandas.DataFrame} that it tried to write."""
    from pyspark.sql import DataFrameWriter
    written = {}

    class Reader:  # rewrite s3://bucket/... to the local fixture directory
        def __init__(self, r): self._r = r
        def csv(self, path, **kw): return self._r.csv(path.replace("s3://test-bucket", str(root)), **kw)
        def __getattr__(self, n): return getattr(self._r, n)

    class SparkProxy:
        def __init__(self, s): self._s = s
        @property
        def read(self): return Reader(self._s.read)
        def __getattr__(self, n): return getattr(self._s, n)

    orig_option, orig_save = DataFrameWriter.option, DataFrameWriter.save

    def option(self, k, v):
        if k == "dbtable":
            self._tbl = v
        return orig_option(self, k, v)

    def save(self, *a, **k):
        written[self._tbl] = self._df.toPandas()

    monkeypatch.setattr(DataFrameWriter, "option", option)
    monkeypatch.setattr(DataFrameWriter, "save", save)
    monkeypatch.setattr("pyspark.context.SparkContext", lambda: spark.sparkContext)  # scripts call SparkContext()

    glue = types.ModuleType("awsglue")
    utils = types.ModuleType("awsglue.utils")
    utils.getResolvedOptions = lambda argv, keys: {
        "JOB_NAME": "t", "DB_ENDPOINT": "h", "SECRET_NAME": "s", "BUCKET_NAME": "test-bucket"}
    ctx = types.ModuleType("awsglue.context")
    ctx.GlueContext = lambda sc: types.SimpleNamespace(spark_session=SparkProxy(spark))
    job = types.ModuleType("awsglue.job")
    job.Job = lambda gc: types.SimpleNamespace(init=lambda *a: None, commit=lambda: None)
    boto3 = types.ModuleType("boto3")
    boto3.client = lambda name: types.SimpleNamespace(
        get_secret_value=lambda SecretId: {"SecretString": json.dumps({"username": "u", "password": "p"})})
    for name, mod in {"awsglue": glue, "awsglue.utils": utils, "awsglue.context": ctx,
                      "awsglue.job": job, "boto3": boto3}.items():
        monkeypatch.setitem(sys.modules, name, mod)
    runpy.run_path(str(SCRIPTS / script), run_name="__main__")
    return written


def test_house_value(spark, cleaned, monkeypatch):
    df = run_job("etl_house_value.py", spark, cleaned, monkeypatch)["landing.lnd_house_value"]
    assert {"region_id", "county_name", "metro_name", "home_tier", "house_value", "metric_date", "year"} <= set(df.columns)
    assert set(df.home_tier) == {"bottom", "middle", "top"}
    assert df.metric_date.astype(str).min() >= "2015-01-01"        # pre-2015 filtered out
    assert df.house_value.notna().all()                              # null months dropped
    assert set(df.metro_name) == {"Boston", "Non-metro"}             # long metro name shortened, null -> Non-metro
    boston_mid = df[(df.metro_name == "Boston") & (df.home_tier == "middle")]
    assert boston_mid.house_value.tolist() == [300.0]


def test_monthly_payment(spark, cleaned, monkeypatch):
    df = run_job("etl_monthly_payment.py", spark, cleaned, monkeypatch)["landing.lnd_monthly_payment"]
    assert set(df.metro_name) == {"Boston"}
    assert sorted(df.monthly_payment) == [2500.0, 2510.0]


def test_rental_income_keeps_zip_leading_zero(spark, cleaned, monkeypatch):
    df = run_job("etl_rental_income.py", spark, cleaned, monkeypatch)["landing.lnd_rental_income"]
    assert df.zip_code.tolist() == ["02108"]
    assert df.metro_name.tolist() == ["Boston"]
    assert df.rental_income.tolist() == [3000.0]
