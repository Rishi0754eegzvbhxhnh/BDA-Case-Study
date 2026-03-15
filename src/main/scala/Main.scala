import org.apache.spark.sql.{SparkSession, DataFrame, functions => F}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.Pipeline

object Main {

  def main(args: Array[String]): Unit = {
    banner("🛒  Customer Segmentation  –  UK Online Retail  ·  Spark + Scala")
    val spark = step1_spark()
    val rawDF   = step2_load(spark)
    val cleanDF = step3_clean(rawDF)
    val rfmDF   = step4_rfm(cleanDF, spark)
    val featDF  = step5_features(cleanDF, rfmDF)
    step6_eda(cleanDF, featDF)
    val mlDF    = step7_assemble(featDF)
    val pred    = step8_kmeans(mlDF)
    val labeled = step9_label(pred)
    step10_sql(labeled, cleanDF, spark)
    step11_save(labeled, cleanDF)
    spark.stop()
    banner("✅  Pipeline Complete!  Results in ./output/")
  }

  def step1_spark(): SparkSession = {
    section("STEP 1 – Create SparkSession")
    val spark = SparkSession.builder
      .appName("Customer Segmentation")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "8")
      .config("spark.driver.memory", "2g")
      .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    println(s"  Spark ${spark.version}  |  Java ${System.getProperty("java.version")}  |  ${Runtime.getRuntime.availableProcessors()} cores")
    spark
  }

  def step2_load(spark: SparkSession): DataFrame = {
    section("STEP 2 – Load Dataset")
    val df = spark.read
      .option("header",      "true")
      .option("inferSchema", "false")
      .option("encoding",    "ISO-8859-1")
      .csv("data/online_retail.csv")
    df.show(5, truncate = false)
    df.printSchema()
    println(s"  ✔  Rows loaded : ${df.count()}")
    df
  }

  def step3_clean(df: DataFrame): DataFrame = {
    section("STEP 3 – Clean Data")
    val before = df.count()
    val cleaned = df
      .withColumn("Quantity",   col("Quantity").cast("double"))
      .withColumn("UnitPrice",  col("UnitPrice").cast("double"))
      .withColumn("InvoiceDate",
          coalesce(
            to_timestamp(col("InvoiceDate"), "M/d/yyyy H:mm"),
            to_timestamp(col("InvoiceDate"), "yyyy-MM-dd HH:mm:ss"),
            to_timestamp(col("InvoiceDate"))
          ))
      .withColumn("TotalPrice", col("Quantity") * col("UnitPrice"))
      .filter(col("CustomerID").isNotNull)
      .filter(trim(col("CustomerID")) =!= "")
      .filter(!col("InvoiceNo").startsWith("C"))
      .filter(col("Quantity")  > 0)
      .filter(col("UnitPrice") > 0)
      .filter(col("Quantity")  < 1000)
      .dropDuplicates()
    val after = cleaned.count()
    println(s"  Before : ${before.formatted("%,d")}  →  After : ${after.formatted("%,d")}")
    println(s"  Removed: ${(before - after).formatted("%,d")} rows")
    if (after == 0) {
      System.err.println("  ❌  0 rows after cleaning! Exiting."); System.exit(1)
    }
    println(s"  Unique customers : ${cleaned.select("CustomerID").distinct().count()}")
    cleaned
  }

  def step4_rfm(df: DataFrame, spark: SparkSession): DataFrame = {
    section("STEP 4 – RFM Feature Engineering")
    import spark.implicits._
    val maxDateRow = df.agg(max("InvoiceDate")).head()
    if (maxDateRow.isNullAt(0)) { System.err.println("  ❌  No dates found"); System.exit(1) }
    val refDate = new java.sql.Timestamp(maxDateRow.getTimestamp(0).getTime + 86400000L)
    println(s"  Reference date : $refDate")
    val rfm = df.groupBy("CustomerID")
      .agg(
        F.round(datediff(F.lit(refDate), max("InvoiceDate")).cast("double"), 0).as("recency"),
        countDistinct("InvoiceNo").as("frequency"),
        F.round(sum("TotalPrice"), 2).as("monetary")
      )
      .filter(col("monetary") > 0)
    rfm.show(8, truncate = false)
    rfm.describe("recency", "frequency", "monetary").show(truncate = false)
    rfm
  }

  def step5_features(cleanDF: DataFrame, rfmDF: DataFrame): DataFrame = {
    section("STEP 5 – Extended Features")
    val ext = cleanDF.groupBy("CustomerID")
      .agg(
        F.round(sum("TotalPrice") / countDistinct("InvoiceNo"), 2).as("avg_basket_value"),
        F.round(avg("Quantity"),   2).as("avg_quantity"),
        F.round(avg("UnitPrice"),  2).as("avg_unit_price"),
        countDistinct("StockCode").as("product_variety"),
        datediff(max("InvoiceDate"), min("InvoiceDate")).as("purchase_spread_days"),
        first("Country").as("country")
      )
    val featDF = rfmDF.join(ext, Seq("CustomerID"), "inner")
      .na.fill(0.0, Seq("avg_basket_value","avg_quantity","avg_unit_price","purchase_spread_days"))
    println(s"  ✔  Feature-ready customers : ${featDF.count()}")
    featDF.show(5, truncate = false)
    featDF
  }

  def step6_eda(cleanDF: DataFrame, featDF: DataFrame): Unit = {
    section("STEP 6 – Exploratory Data Analysis")
    println("  ── Monthly revenue ─────────────────────────────────────────")
    cleanDF.withColumn("month", date_format(col("InvoiceDate"), "yyyy-MM"))
      .groupBy("month")
      .agg(F.round(sum("TotalPrice"),0).as("revenue_GBP"),
           countDistinct("InvoiceNo").as("invoices"),
           countDistinct("CustomerID").as("customers"))
      .orderBy("month").show(15, truncate = false)
    println("  ── Top 10 customers by spend ────────────────────────────────")
    featDF.orderBy(desc("monetary"))
      .select("CustomerID","monetary","frequency","recency","avg_basket_value","country")
      .show(10, truncate = false)
    println("  ── Revenue by country ──────────────────────────────────────")
    cleanDF.groupBy("Country")
      .agg(F.round(sum("TotalPrice"),0).as("revenue_GBP"),
           countDistinct("CustomerID").as("customers"))
      .orderBy(desc("revenue_GBP")).show(10)
  }

  def step7_assemble(df: DataFrame): DataFrame = {
    section("STEP 7 – VectorAssembler + StandardScaler")
    val featureCols = Array("recency","frequency","monetary",
                            "avg_basket_value","avg_unit_price",
                            "product_variety","purchase_spread_days")
    val assembler = new VectorAssembler()
      .setInputCols(featureCols).setOutputCol("raw_features").setHandleInvalid("skip")
    val assembled   = assembler.transform(df)
    val scalerModel = new StandardScaler()
      .setInputCol("raw_features").setOutputCol("features")
      .setWithMean(true).setWithStd(true).fit(assembled)
    val mlDF = scalerModel.transform(assembled)
    println(s"  ✔  ML-ready : ${mlDF.count()} customers")
    mlDF
  }

  def step8_kmeans(df: DataFrame): DataFrame = {
    section("STEP 8 – KMeans Clustering  (k=4)")
    val model = new Pipeline().setStages(Array(
      new KMeans().setK(4).setSeed(1L)
        .setFeaturesCol("features").setPredictionCol("cluster").setMaxIter(20)
    )).fit(df)
    val pred = model.transform(df)
    println("  ── Cluster sizes ─────────────────────────────────────────────")
    pred.groupBy("cluster").count().orderBy("cluster").show()
    println("  ── RFM per cluster ───────────────────────────────────────────")
    pred.groupBy("cluster").agg(
      F.round(avg("recency"),1).as("avg_recency"),
      F.round(avg("frequency"),1).as("avg_orders"),
      F.round(avg("monetary"),0).as("avg_spend_GBP"),
      count("*").as("customers")
    ).orderBy("cluster").show(truncate = false)
    pred
  }

  def step9_label(df: DataFrame): DataFrame = {
    section("STEP 9 – Label Segments")
    val labeled = df.withColumn("segment",
      when(col("recency") < 30  && col("monetary") > 500,  "Champion")
      .when(col("recency") < 60  && col("frequency") > 8,  "Loyal Customer")
      .when(col("recency") > 120 && col("monetary") > 200, "At-Risk Customer")
      .when(col("frequency") <= 2,                          "New / One-Time")
      .otherwise("Regular Customer")
    )
    labeled.groupBy("segment").agg(
      count("*").as("customers"),
      F.round(avg("monetary"),0).as("avg_spend_GBP"),
      F.round(avg("recency"),1).as("avg_recency_days")
    ).orderBy(desc("avg_spend_GBP")).show(truncate = false)
    labeled.select("CustomerID","cluster","segment","recency","frequency","monetary")
      .show(15, truncate = false)
    labeled
  }

  def step10_sql(labeled: DataFrame, cleanDF: DataFrame, spark: SparkSession): Unit = {
    section("STEP 10 – Spark SQL Insights")
    labeled.createOrReplaceTempView("customers")
    cleanDF.createOrReplaceTempView("transactions")
    println("  ── Top 3 per cluster ───────────────────────────────────────")
    spark.sql("""
      SELECT cluster, CustomerID, ROUND(monetary,2) AS spend_GBP, segment
      FROM (SELECT *, ROW_NUMBER() OVER
        (PARTITION BY cluster ORDER BY monetary DESC) AS rn FROM customers)
      WHERE rn <= 3 ORDER BY cluster, spend_GBP DESC
    """).show(20, truncate = false)
    println("  ── Revenue share by segment ────────────────────────────────")
    spark.sql("""
      SELECT segment, COUNT(*) AS customers,
             ROUND(SUM(monetary),0) AS total_GBP,
             ROUND(SUM(monetary)*100.0/SUM(SUM(monetary)) OVER(),1) AS pct
      FROM customers GROUP BY segment ORDER BY total_GBP DESC
    """).show(truncate = false)
    println("  ── High-value at-risk customers ────────────────────────────")
    spark.sql("""
      SELECT CustomerID, ROUND(monetary,0) AS lifetime_GBP,
             frequency, recency AS days_inactive
      FROM customers WHERE recency > 120 AND monetary > 500
      ORDER BY monetary DESC LIMIT 10
    """).show(truncate = false)
  }

  def step11_save(labeled: DataFrame, cleanDF: DataFrame): Unit = {
    section("STEP 11 – Save Outputs")
    def save(df: DataFrame, path: String): Unit = {
      df.coalesce(1).write.mode("overwrite").option("header","true").csv(path)
      println(s"  ✔  $path")
    }
    save(labeled.select("CustomerID","cluster","segment","recency",
                        "frequency","monetary","avg_basket_value",
                        "product_variety","country"),
         "output/customer_segments")
    save(cleanDF.withColumn("month", date_format(col("InvoiceDate"),"yyyy-MM"))
           .groupBy("month").agg(F.round(sum("TotalPrice"),0).as("revenue_GBP"),
           countDistinct("InvoiceNo").as("invoices"),
           countDistinct("CustomerID").as("customers")).orderBy("month"),
         "output/monthly_revenue")
    save(cleanDF.groupBy("Country")
           .agg(F.round(sum("TotalPrice"),0).as("revenue_GBP"),
           countDistinct("CustomerID").as("customers"))
           .orderBy(desc("revenue_GBP")), "output/country_revenue")
    save(labeled.groupBy("segment").agg(count("*").as("customers"),
           F.round(sum("monetary"),0).as("total_GBP"),
           F.round(avg("monetary"),0).as("avg_spend_GBP"),
           F.round(avg("recency"),1).as("avg_recency_days"))
           .orderBy(desc("total_GBP")), "output/segment_summary")
    save(cleanDF.groupBy("StockCode","Description")
           .agg(F.round(sum("TotalPrice"),0).as("revenue_GBP"),
           sum("Quantity").as("units_sold"))
           .orderBy(desc("revenue_GBP")).limit(100), "output/top_products")
    println("\n  ✅  All outputs saved to ./output/")
  }

  def banner(t: String): Unit = { val l="═"*68; println(s"\n$l\n  $t\n$l\n") }
  def section(t: String): Unit = { println(s"\n${"─"*68}\n  ▶  $t\n${"─"*68}") }
}
