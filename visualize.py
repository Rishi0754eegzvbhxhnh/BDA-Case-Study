import pandas as pd
import matplotlib.pyplot as plt
import glob

# function to load spark output
def load_csv(folder):
    file = glob.glob(folder + "/*.csv")[0]
    df = pd.read_csv(file)
    print("Columns in", folder, ":", df.columns)  # show column names
    return df

# 1 Country revenue
country = load_csv("output/country_revenue")

plt.figure()
plt.bar(country.iloc[:,0], country.iloc[:,1])
plt.xticks(rotation=90)
plt.title("Revenue by Country")
plt.tight_layout()
plt.savefig("country_revenue_chart.png")

# 2 Monthly revenue
monthly = load_csv("output/monthly_revenue")

plt.figure()
plt.plot(monthly.iloc[:,0], monthly.iloc[:,1], marker='o')
plt.xticks(rotation=45)
plt.title("Monthly Revenue Trend")
plt.tight_layout()
plt.savefig("monthly_revenue_chart.png")

# 3 Customer segments
segments = load_csv("output/segment_summary")

plt.figure()
plt.pie(segments.iloc[:,1], labels=segments.iloc[:,0], autopct='%1.1f%%')
plt.title("Customer Segments")
plt.savefig("customer_segments_chart.png")

# 4 Top products
products = load_csv("output/top_products")

top10 = products.head(10)

plt.figure()
plt.barh(top10.iloc[:,0], top10.iloc[:,1])
plt.title("Top Products")
plt.tight_layout()
plt.savefig("top_products_chart.png")

print("Charts generated successfully!")
