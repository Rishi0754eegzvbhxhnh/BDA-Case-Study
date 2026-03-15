import pandas as pd
import matplotlib.pyplot as plt
import glob

# Function to load spark output
def load_csv(path):
    file = glob.glob(path + "/*.csv")[0]
    return pd.read_csv(file)

# 1️⃣ Country Revenue
country = load_csv("output/country_revenue")

plt.figure()
plt.bar(country['Country'], country['Revenue'])
plt.xticks(rotation=90)
plt.title("Revenue by Country")
plt.xlabel("Country")
plt.ylabel("Revenue")
plt.tight_layout()
plt.savefig("country_revenue_chart.png")

# 2️⃣ Monthly Revenue
monthly = load_csv("output/monthly_revenue")

plt.figure()
plt.plot(monthly['Month'], monthly['Revenue'], marker='o')
plt.xticks(rotation=45)
plt.title("Monthly Revenue Trend")
plt.xlabel("Month")
plt.ylabel("Revenue")
plt.tight_layout()
plt.savefig("monthly_revenue_chart.png")

# 3️⃣ Customer Segments
segments = load_csv("output/segment_summary")

plt.figure()
plt.pie(segments['Count'], labels=segments['Segment'], autopct='%1.1f%%')
plt.title("Customer Segmentation Distribution")
plt.savefig("customer_segments_chart.png")

# 4️⃣ Top Products
products = load_csv("output/top_products")

top10 = products.head(10)

plt.figure()
plt.barh(top10['Product'], top10['Revenue'])
plt.title("Top 10 Products")
plt.xlabel("Revenue")
plt.ylabel("Product")
plt.tight_layout()
plt.savefig("top_products_chart.png")

print("✅ Charts generated successfully!")
