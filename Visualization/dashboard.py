# pip install streamlit pandas sqlalchemy psycopg2-binary plotly
# run with:  streamlit run dashboard.py

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine

ALL_COUNTIES = "All counties"

st.set_page_config(page_title="MA Real Estate Investment Analysis", layout="wide", page_icon="📈")


@st.cache_resource
def get_db_engine():
    creds = st.secrets["postgres"]  # .streamlit/secrets.toml
    url = f"postgresql+psycopg2://{creds['username']}:{creds['password']}@{creds['host']}:{creds['port']}/{creds['database']}"
    return create_engine(url)


engine = get_db_engine()


@st.cache_data(ttl=3600)
def q(sql: str) -> pd.DataFrame:
    df = pd.read_sql(sql, engine)
    if "metric_month" in df:
        df["metric_month"] = pd.to_datetime(df["metric_month"])
    return df


# -----------------------------------------------------------------------------
# Data extraction (county grain for value/rent, metro grain for mortgage payment)
# -----------------------------------------------------------------------------
values = q(f"""
    SELECT g.metro_name, g.county_name, v.home_tier, v.metric_date AS metric_month, v.avg_house_value
    FROM fact_house_value v JOIN dim_geography g USING (geo_key)""")
rents = q(f"""
    SELECT g.metro_name, g.county_name, r.metric_date AS metric_month, r.rental_income
    FROM fact_rental_income r JOIN dim_geography g USING (geo_key)""")
payments = q(f"""
    SELECT g.metro_name, p.metric_date AS metric_month, p.monthly_payment
    FROM fact_monthly_payment p JOIN dim_geography g USING (geo_key)
    WHERE g.county_name = '{ALL_COUNTIES}'""")
zips = q("""
    SELECT zip_code, city, county_name, metro_name, metric_date AS metric_month, rental_income
    FROM fact_zip_rent""")

st.sidebar.title("📍 Market Filters")
level = st.sidebar.radio("Geography level", ["Metro", "County"], horizontal=True)
tier = st.sidebar.selectbox(
    "Home value tier", ["middle", "bottom", "top"],
    help="ZHVI price tier. The mortgage-payment series is for the middle tier (20% down), "
         "so cash flow is only like-for-like on 'middle'.")

if values.empty or rents.empty:
    st.error("No data found. Run the pipeline (see README) and check the database connection.")
    st.stop()

# -----------------------------------------------------------------------------
# Feature engineering
# -----------------------------------------------------------------------------
key = "metro_name" if level == "Metro" else "county_name"
label = "Metro" if level == "Metro" else "County"

# fact rows exist at metro grain (county_name = 'All counties') and county grain
is_metro_row = lambda d: d.county_name == ALL_COUNTIES
pick = (lambda d: d[is_metro_row(d)]) if level == "Metro" else (lambda d: d[~is_metro_row(d)])
v = pick(values[values.home_tier == tier]).groupby([key, "metric_month"], as_index=False)["avg_house_value"].mean()
r = pick(rents).groupby([key, "metric_month"], as_index=False)["rental_income"].mean()
df = v.merge(r, on=[key, "metric_month"], how="outer")
if level == "Metro":  # mortgage payment exists only at metro grain
    df = df.merge(payments, on=["metro_name", "metric_month"], how="outer")
else:
    df["monthly_payment"] = float("nan")
df = df.dropna(subset=[key]).sort_values([key, "metric_month"])
df["gross_yield"] = df.rental_income * 12 / df.avg_house_value
df["price_to_rent"] = df.avg_house_value / (df.rental_income * 12)
df["cash_flow"] = df.rental_income - df.monthly_payment

options = sorted(df[key].unique())
defaults = [o for o in (["Boston", "Worcester", "Springfield"] if level == "Metro"
                        else ["Suffolk County", "Middlesex County", "Worcester County"]) if o in options] or options[:3]
selected = st.sidebar.multiselect(f"Select MA {label.lower()}s", options, default=defaults)
fdf = df[df[key].isin(selected)]

with st.sidebar.expander("ℹ️ Data notes"):
    st.markdown(
        "- Source: Zillow Research (ZHVI, ZORI, total monthly payment).\n"
        "- Metro and county values are simple averages of the city (ZHVI) or ZIP (ZORI) series inside them.\n"
        "- Metro names are shortened (e.g. *Boston-Cambridge-Newton, MA-NH* → Boston) and "
        "include only Zillow's regions in the metro; Zillow data may be missing for some places.\n"
        "- Mortgage payments are published for metros only.")

st.title("🏠 Massachusetts Real Estate Market & Investment Analytics")
if fdf.empty:
    st.warning(f"Please select at least one {label.lower()} from the sidebar.")
    st.stop()

# -----------------------------------------------------------------------------
# Headline metrics (latest month with value + rent)
# -----------------------------------------------------------------------------
both = fdf.dropna(subset=["avg_house_value", "rental_income"])


def roi(g, months=60):
    g = g.dropna(subset=["avg_house_value"]).set_index("metric_month")["avg_house_value"]
    if g.empty:
        return float("nan")
    past = g[g.index <= g.index.max() - pd.DateOffset(months=months)]
    return g.iloc[-1] / past.iloc[-1] - 1 if len(past) else float("nan")


snap = []
for k, g in fdf.groupby(key):
    gb = g.dropna(subset=["avg_house_value", "rental_income"])
    last = gb.iloc[-1] if len(gb) else None
    snap.append({
        label: k,
        "As of": last.metric_month.date() if last is not None else None,
        "House value ($)": last.avg_house_value if last is not None else None,
        "Monthly rent ($)": last.rental_income if last is not None else None,
        "Gross yield": last.gross_yield if last is not None else None,
        "Price-to-rent": last.price_to_rent if last is not None else None,
        "Net cash flow ($/mo)": last.cash_flow if last is not None else None,
        "5-yr appreciation": roi(g),
    })
snap = pd.DataFrame(snap)
st.subheader("Latest snapshot")
st.dataframe(snap.style.format({
    "House value ($)": "{:,.0f}", "Monthly rent ($)": "{:,.0f}", "Gross yield": "{:.1%}",
    "Price-to-rent": "{:.1f}", "Net cash flow ($/mo)": "{:,.0f}", "5-yr appreciation": "{:.1%}"}, na_rep="—"),
    width="stretch", hide_index=True)

st.divider()

# -----------------------------------------------------------------------------
# Section 1: trends
# -----------------------------------------------------------------------------
st.header("📊 Section 1: Historical Market Trends")
t1, t2, t3 = st.tabs(["📈 House Values", "🏠 Rental Income", "🏦 Mortgage Payments"])


def line(tab, data, col, title, ylabel, empty_msg, **kw):
    with tab:
        d = data.dropna(subset=[col])
        if d.empty:
            st.info(empty_msg)
        else:
            st.plotly_chart(px.line(d, x="metric_month", y=col, color=key, title=title,
                                    labels={col: ylabel, "metric_month": "Date", key: label}, **kw),
                            width="stretch")


line(t1, fdf, "avg_house_value", f"Average House Value ({tier} tier)", "House Value ($)", "No house value data.")
line(t2, fdf, "rental_income", "Average Monthly Rent (ZORI)", "Monthly Rent ($)", "No rent data.")
line(t3, fdf, "monthly_payment", "Estimated Monthly Mortgage Payment (20% down)", "Payment ($)",
     "Mortgage payments are published for metros only — switch the geography level to Metro.")

st.divider()

# -----------------------------------------------------------------------------
# Section 2: investment analysis
# -----------------------------------------------------------------------------
st.header("💼 Section 2: Investment Analysis")
a1, a2, a3 = st.tabs(["⚖️ Yield & Price-to-Rent", "💵 Cash Flow", "📮 ZIP-level Rents"])

with a1:
    c1, c2 = st.columns(2)
    if both.empty:
        st.info("No overlapping house value and rent data.")
    else:
        with c1:
            fig = px.line(both, x="metric_month", y="gross_yield", color=key, title="Gross Rental Yield",
                          labels={"gross_yield": "Gross yield", "metric_month": "Date", key: label})
            fig.update_layout(yaxis_tickformat=".1%")
            st.plotly_chart(fig, width="stretch")
        with c2:
            st.plotly_chart(px.line(both, x="metric_month", y="price_to_rent", color=key,
                                    title="Price-to-Rent Ratio (higher = renting is relatively cheaper)",
                                    labels={"price_to_rent": "Price / annual rent", "metric_month": "Date", key: label}),
                            width="stretch")

with a2:
    cash = fdf.dropna(subset=["rental_income", "monthly_payment"])
    if level != "Metro":
        st.info("Mortgage payments are published for metros only — switch the geography level to Metro.")
    elif tier != "middle":
        st.warning("Mortgage payments are for the middle price tier; select 'middle' for a like-for-like comparison.")
    if cash.empty:
        st.info("No overlapping rent and mortgage data.")
    else:
        melted = cash.melt(id_vars=["metric_month", key], value_vars=["rental_income", "monthly_payment"],
                           var_name="Metric", value_name="Amount ($)")
        melted["Metric"] = melted["Metric"].map({"rental_income": "Rent income", "monthly_payment": "Mortgage payment"})
        st.plotly_chart(px.line(melted, x="metric_month", y="Amount ($)", color=key, line_dash="Metric",
                                title="Rent vs. Mortgage Payment"), width="stretch")
        fig = px.line(cash, x="metric_month", y="cash_flow", color=key,
                      title="Estimated Monthly Net Cash Flow (Rent − Mortgage; excludes taxes, insurance, upkeep)",
                      labels={"cash_flow": "Net cash flow ($)", "metric_month": "Date", key: label})
        fig.add_hline(y=0, line_dash="dash", line_color="black", annotation_text="Break even")
        st.plotly_chart(fig, width="stretch")

with a3:
    if zips.empty:
        st.info("No ZIP-level rent data.")
    else:
        zsel = zips if level == "Metro" and not selected else zips[zips["metro_name" if level == "Metro" else "county_name"].isin(selected)]
        latest = zsel[zsel.metric_month == zsel.metric_month.max()].dropna(subset=["rental_income"])
        st.caption(f"Latest ZIP-level rents ({zsel.metric_month.max().date()}) for the selected {label.lower()}s.")
        if latest.empty:
            st.info("No ZIP data for the selected filter.")
        else:
            st.plotly_chart(px.bar(latest.sort_values("rental_income", ascending=False).head(30),
                                   x="zip_code", y="rental_income", color="city",
                                   labels={"rental_income": "Monthly rent ($)", "zip_code": "ZIP"},
                                   title="Top 30 ZIP codes by rent"), width="stretch")
            st.dataframe(latest.sort_values("rental_income", ascending=False)
                         [["zip_code", "city", "county_name", "metro_name", "rental_income"]],
                         width="stretch", hide_index=True)

st.divider()
with st.expander("📂 View raw combined data"):
    st.dataframe(fdf.sort_values(["metric_month", key], ascending=[False, True]), width="stretch")
