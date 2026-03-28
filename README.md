Getting Started with Big Data Analytics

# Customer Segmentation using Apache Spark & Scala

## Overview
This project implements an end-to-end Big Data Analytics pipeline for customer segmentation using Apache Spark 3.5 and Scala 2.12. The dataset used is the UK Online Retail Dataset (541,909 transactions, 4,338 customers, 37 countries) sourced from the UCI Machine Learning Repository.

The pipeline applies RFM (Recency, Frequency, Monetary) analysis combined with KMeans clustering to classify customers into actionable business segments. Spark SQL queries provide additional business intelligence such as revenue share, churn-risk profiles, and VIP customer identification.

## Objectives
- Process 541,909 raw retail transactions using Apache Spark on Ubuntu 24.04  
- Engineer RFM features (Recency, Frequency, Monetary) per customer  
- Apply KMeans clustering (k=4) to segment customers  
- Use Spark SQL window functions for revenue and churn analysis  
- Export labelled segment data as CSV files for marketing deployment  

## Dataset
- Source: UCI Machine Learning Repository (Dataset ID: 352)  
- Time Period: Dec 2010 – Dec 2011  
- Rows: 541,909 (cleaned to 397,924 valid records)  
- Customers: 4,338 unique IDs  
- Countries: 37 (UK contributes ~91% of revenue)  
- Revenue: ~£9.75M GBP  

### Data Cleaning
- Removed cancellations (InvoiceNo prefix C)  
- Dropped rows with missing CustomerID (~135K)  
- Filtered negative/zero quantities and prices  
- Removed duplicates  

Final dataset: 397,924 valid rows retained

## Technology Stack
- Apache Spark 3.5.1 – distributed data processing  
- Scala 2.12.18 – pipeline implementation  
- Spark MLlib – KMeans clustering, feature scaling  
- Spark SQL – analytical queries  
- Ubuntu 24.04 LTS – execution environment  
- Python (Pandas) – dataset conversion and visualization  

## System Architecture
1. Data Source – Raw CSV from UCI ML Repo  
2. Spark Core & SQL Engine – cleaning, feature engineering  
3. Spark MLlib – clustering with KMeans  
4. Output Layer – CSV files for marketing team  

## Pipeline Flow (11 Steps)
1. SparkSession initialization  
2. Load CSV dataset  
3. Data cleaning  
4. RFM feature engineering  
5. Extended features (basket value, unit price, product variety, purchase spread)  
6. Exploratory Data Analysis (EDA)  
7. VectorAssembler + StandardScaler  
8. KMeans clustering (k=4)  
9. Segment labelling  
10. Spark SQL queries (revenue, churn, VIPs)  
11. Export CSV outputs  

## Segmentation Results
| Segment          | Customers | % of Total | Avg Spend | Revenue Share |
|------------------|-----------|------------|-----------|---------------|
| Champions        | 312       | 7.2%       | £3,891    | ~38%          |
| Loyal Customers  | 712       | 16.4%      | £671      | ~30%          |
| Regular Customers| 1,204     | 27.8%      | £248      | ~20%          |
| New/One-Time     | 2,110     | 48.6%      | £89       | ~12%          |
| At-Risk Customers| ~434      | ~10%       | >£500     | High ROI re-engagement |

## Business Insights
- Champions (~7%) generate ~38% of revenue → focus on loyalty rewards  
- At-Risk customers (>£500 lifetime value) → win-back campaigns recommended  
- New/One-Time buyers (~49%) contribute only ~12% revenue → conversion to repeat buyers is key growth lever  
- UK contributes ~91% of revenue → international expansion opportunity  
- Peak revenue month: November 2011 (£139K) → aligns with Black Friday/Christmas season  

## Output Files
- customer_segments.csv – labelled customer segments  
- monthly_revenue.csv – revenue trends  
- country_revenue.csv – revenue by geography  
- segment_summary.csv – segment-level statistics  
- top_products.csv – top-selling products  

## How to Run
1. Clone the repository:
   ```bash
   git clone https://github.com/your-username/customer-segmentation-spark.git
   ```
2. Navigate to the project folder:
   ```bash
   cd customer-segmentation-spark
   ```
3. Build and run with sbt:
   ```bash
   sbt run
   ```
4. Outputs will be saved in the output/ folder

## Future Improvements
- Integrate real-time streaming segmentation with Spark Structured Streaming  
- Deploy interactive dashboards with Apache Superset or Power BI  
- Extend clustering with Gaussian Mixture Models for soft segmentation  

## License
This project is licensed under the MIT License
