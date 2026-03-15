import glob, os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(page_title="🛒 Customer Segmentation", page_icon="🛒", layout="wide")

def read_spark(folder):
    files = glob.glob(os.path.join(folder, "part-*.csv"))
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_csv(f) for f in files], ignore_index=True)

@st.cache_data
def load_all():
    return (read_spark("output/customer_segments"),
            read_spark("output/monthly_revenue"),
            read_spark("output/country_revenue"),
            read_spark("output/segment_summary"),
            read_spark("output/top_products"))

segs, monthly, country, summary, prods = load_all()

if segs.empty:
    st.error("⚠️  No output files found. Run sbt 'runMain Main' first.")
    st.stop()

# Sidebar
st.sidebar.title("🛒 Controls")
all_segs = ["All"] + sorted(segs["segment"].dropna().unique().tolist())
seg_filter = st.sidebar.selectbox("Filter by Segment", all_segs)
filtered = segs if seg_filter == "All" else segs[segs["segment"] == seg_filter]
st.sidebar.markdown(f"**Showing:** {len(filtered):,} of {len(segs):,} customers")

# Header
st.title("🛒 Customer Segmentation Dashboard")
st.caption("UK Online Retail  ·  Apache Spark 3.5  ·  KMeans RFM Clustering  ·  4,331 Customers")

# KPI Row
k1,k2,k3,k4,k5 = st.columns(5)
k1.metric("Total Customers",    f"{len(segs):,}")
k2.metric("Avg Spend / Customer",f"£{segs['monetary'].mean():,.0f}")
k3.metric("Avg Recency (days)", f"{segs['recency'].mean():.0f}")
k4.metric("Avg Orders",          f"{segs['frequency'].mean():.1f}")
k5.metric("Segments",            segs["segment"].nunique())
st.divider()

# Row 1: Pie + Revenue bar
col1, col2 = st.columns(2)
with col1:
    st.subheader("🥧 Segment Distribution")
    fig = px.pie(segs.groupby("segment").size().reset_index(name="count"),
                 values="count", names="segment", hole=0.35,
                 color_discrete_sequence=px.colors.qualitative.Set2)
    fig.update_traces(textposition="inside", textinfo="percent+label")
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("💰 Revenue Share by Segment")
    if not summary.empty:
        fig = px.bar(summary.sort_values("total_GBP", ascending=True),
                     x="total_GBP", y="segment", orientation="h",
                     color="segment", text_auto=True,
                     color_discrete_sequence=px.colors.qualitative.Set2,
                     labels={"total_GBP":"Revenue (£)","segment":""})
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

# Row 2: RFM Scatter
st.subheader("📊 RFM Scatter — Recency vs Monetary Value")
sample = filtered.sample(min(2000, len(filtered)), random_state=42)
fig = px.scatter(sample, x="recency", y="monetary", color="segment",
                 size="frequency", hover_data=["CustomerID","avg_basket_value","country"],
                 labels={"recency":"Recency (days)","monetary":"Total Spend (£)"},
                 color_discrete_sequence=px.colors.qualitative.Set2, height=420)
fig.update_layout(legend=dict(orientation="h", y=-0.2))
st.plotly_chart(fig, use_container_width=True)

# Row 3: Monthly + Country
col3, col4 = st.columns(2)
with col3:
    st.subheader("📅 Monthly Revenue Trend")
    if not monthly.empty:
        fig = go.Figure()
        fig.add_trace(go.Bar(x=monthly["month"], y=monthly["revenue_GBP"],
                             name="Revenue £", marker_color="#5C2D91"))
        fig.add_trace(go.Scatter(x=monthly["month"], y=monthly["customers"],
                                 name="Customers", yaxis="y2",
                                 line=dict(color="#E76F51", width=2)))
        fig.update_layout(yaxis=dict(title="Revenue (£)"),
                          yaxis2=dict(title="Customers", overlaying="y", side="right"),
                          height=380, legend=dict(orientation="h", y=-0.25))
        st.plotly_chart(fig, use_container_width=True)

with col4:
    st.subheader("🌍 Revenue by Country (Top 10)")
    if not country.empty:
        fig = px.bar(country.head(10).sort_values("revenue_GBP"),
                     x="revenue_GBP", y="Country", orientation="h",
                     color="revenue_GBP", color_continuous_scale="Purples",
                     labels={"revenue_GBP":"Revenue (£)","Country":""}, height=380)
        fig.update_layout(coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)

# Row 4: Segment profile table
st.subheader("📋 Segment Profile")
if not summary.empty:
    st.dataframe(summary.style.background_gradient(
        subset=["total_GBP","avg_spend_GBP"], cmap="Purples"),
        use_container_width=True)

# Row 5: Top products
st.subheader("🏆 Top 20 Products by Revenue")
if not prods.empty:
    fig = px.bar(prods.head(20).sort_values("revenue_GBP"),
                 x="revenue_GBP", y="Description", orientation="h",
                 color="revenue_GBP", color_continuous_scale="Teal",
                 labels={"revenue_GBP":"Revenue (£)","Description":""}, height=520)
    fig.update_layout(coloraxis_showscale=False)
    st.plotly_chart(fig, use_container_width=True)

# Row 6: Box plots
st.subheader("📦 RFM Distribution by Segment")
metric = st.selectbox("Metric", ["monetary","recency","frequency"])
fig = px.box(segs, x="segment", y=metric, color="segment",
             color_discrete_sequence=px.colors.qualitative.Set2,
             labels={"segment":"","monetary":"Spend (£)","recency":"Days","frequency":"Orders"},
             height=380)
fig.update_layout(showlegend=False)
st.plotly_chart(fig, use_container_width=True)

# Row 7: Raw data
st.subheader("📄 Customer Data")
st.dataframe(filtered.sort_values("monetary", ascending=False).head(200),
             use_container_width=True)

st.divider()
st.caption("Apache Spark 3.5 · Scala 2.12 · Python Streamlit · Plotly  |  UK Online Retail Dataset")
