"""Glue Python-shell job: landing.* staging tables -> dimensional model (idempotent upserts).

Model
  dim_geography(geo_key, metro_name, county_name)   -- county_name = 'All counties' for metro-grain rows
  fact_house_value(geo_key, home_tier, metric_date)   county grain + metro grain, avg of city-level ZHVI
  fact_rental_income(geo_key, metric_date)            county grain + metro grain, avg of ZIP-level ZORI
  fact_monthly_payment(geo_key, metric_date)          metro grain only ('All counties')
  fact_zip_rent(zip_code, metric_date)                ZIP grain
"""
import sys

ALL_COUNTIES = "All counties"

DDL = [
    """
    create table if not exists dim_geography (
        geo_key serial primary key,
        metro_name text not null,
        county_name text not null,
        inserted_at timestamp default current_timestamp,
        updated_at timestamp default current_timestamp,
        unique (metro_name, county_name)
    );
    """,
    """
    create table if not exists fact_house_value (
        house_value_id serial primary key,
        geo_key int4 not null references dim_geography(geo_key),
        home_tier text not null,
        avg_house_value float8,
        metric_date date not null,
        "year" int4,
        inserted_at timestamp default current_timestamp,
        updated_at timestamp default current_timestamp,
        unique (geo_key, home_tier, metric_date)
    );
    """,
    """
    create table if not exists fact_monthly_payment (
        monthly_payment_id serial primary key,
        geo_key int4 not null references dim_geography(geo_key),
        monthly_payment float8,
        metric_date date not null,
        "year" int4,
        inserted_at timestamp default current_timestamp,
        updated_at timestamp default current_timestamp,
        unique (geo_key, metric_date)
    );
    """,
    """
    create table if not exists fact_rental_income (
        rental_income_id serial primary key,
        geo_key int4 not null references dim_geography(geo_key),
        rental_income float8,
        metric_date date not null,
        "year" int4,
        inserted_at timestamp default current_timestamp,
        updated_at timestamp default current_timestamp,
        unique (geo_key, metric_date)
    );
    """,
    """
    create table if not exists fact_zip_rent (
        zip_rent_id serial primary key,
        zip_code text not null,
        city text,
        county_name text,
        metro_name text,
        rental_income float8,
        metric_date date not null,
        "year" int4,
        inserted_at timestamp default current_timestamp,
        updated_at timestamp default current_timestamp,
        unique (zip_code, metric_date)
    );
    """,
]

UPSERT_DIM = f"""
insert into dim_geography (metro_name, county_name)
select distinct metro_name, county_name from (
    select metro_name, coalesce(nullif(trim(county_name), ''), 'Unknown') as county_name
      from landing.lnd_house_value where metro_name is not null
    union
    select metro_name, coalesce(nullif(trim(county_name), ''), 'Unknown')
      from landing.lnd_rental_income where metro_name is not null
    union
    select metro_name, '{ALL_COUNTIES}' from landing.lnd_monthly_payment where metro_name is not null
    union
    select metro_name, '{ALL_COUNTIES}' from landing.lnd_house_value where metro_name is not null
    union
    select metro_name, '{ALL_COUNTIES}' from landing.lnd_rental_income where metro_name is not null
) g
on conflict (metro_name, county_name) do update set updated_at = current_timestamp;
"""

UPSERT_HOUSE_VALUE = f"""
insert into fact_house_value (geo_key, home_tier, avg_house_value, metric_date, year)
select g.geo_key, l.home_tier, avg(l.house_value), l.metric_date, l.year
from (
    select metro_name, coalesce(nullif(trim(county_name), ''), 'Unknown') as county_name, home_tier, house_value, metric_date, year
      from landing.lnd_house_value
    union all  -- metro grain: average over all cities in the metro, not over counties
    select metro_name, '{ALL_COUNTIES}', home_tier, house_value, metric_date, year
      from landing.lnd_house_value
) l
join dim_geography g on g.metro_name = l.metro_name and g.county_name = l.county_name
group by g.geo_key, l.home_tier, l.metric_date, l.year
on conflict (geo_key, home_tier, metric_date) do update set
    avg_house_value = excluded.avg_house_value, updated_at = current_timestamp;
"""

UPSERT_RENTAL = f"""
insert into fact_rental_income (geo_key, rental_income, metric_date, year)
select g.geo_key, avg(l.rental_income), l.metric_date, l.year
from (
    select metro_name, coalesce(nullif(trim(county_name), ''), 'Unknown') as county_name, rental_income, metric_date, year
      from landing.lnd_rental_income
    union all
    select metro_name, '{ALL_COUNTIES}', rental_income, metric_date, year
      from landing.lnd_rental_income
) l
join dim_geography g on g.metro_name = l.metro_name and g.county_name = l.county_name
group by g.geo_key, l.metric_date, l.year
on conflict (geo_key, metric_date) do update set
    rental_income = excluded.rental_income, updated_at = current_timestamp;
"""

UPSERT_MONTHLY = f"""
insert into fact_monthly_payment (geo_key, monthly_payment, metric_date, year)
select g.geo_key, avg(l.monthly_payment), l.metric_date, l.year
from landing.lnd_monthly_payment l
join dim_geography g on g.metro_name = l.metro_name and g.county_name = '{ALL_COUNTIES}'
group by g.geo_key, l.metric_date, l.year
on conflict (geo_key, metric_date) do update set
    monthly_payment = excluded.monthly_payment, updated_at = current_timestamp;
"""

UPSERT_ZIP = """
insert into fact_zip_rent (zip_code, city, county_name, metro_name, rental_income, metric_date, year)
select zip_code, max(city), max(county_name), max(metro_name), avg(rental_income), metric_date, year
from landing.lnd_rental_income
where zip_code is not null
group by zip_code, metric_date, year
on conflict (zip_code, metric_date) do update set
    city = excluded.city, county_name = excluded.county_name, metro_name = excluded.metro_name,
    rental_income = excluded.rental_income, updated_at = current_timestamp;
"""


def run_etl(conn_params):
    import psycopg2

    conn = psycopg2.connect(**conn_params)
    try:
        with conn.cursor() as cur:
            for stmt in DDL:
                cur.execute(stmt)
            # dimension first, then facts that reference it
            for stmt in (UPSERT_DIM, UPSERT_HOUSE_VALUE, UPSERT_RENTAL, UPSERT_MONTHLY, UPSERT_ZIP):
                cur.execute(stmt)
        conn.commit()
        print("Dimensional model load completed successfully.")
    except Exception as e:
        conn.rollback()
        print(f"ETL failed, rolled back: {e}")
        raise
    finally:
        conn.close()


def main():
    keys = ["DB_HOST", "DB_NAME", "DB_USER", "DB_PWD"]
    try:  # inside AWS Glue
        from awsglue.utils import getResolvedOptions
        args = getResolvedOptions(sys.argv, keys)
    except ImportError:  # local run: python lnd_to_final_data_load.py --DB_HOST ... 
        import argparse
        p = argparse.ArgumentParser()
        for k in keys:
            p.add_argument(f"--{k}", required=True)
        args = vars(p.parse_args())
    run_etl({"host": args["DB_HOST"], "database": args["DB_NAME"],
             "user": args["DB_USER"], "password": args["DB_PWD"]})


if __name__ == "__main__":
    main()
