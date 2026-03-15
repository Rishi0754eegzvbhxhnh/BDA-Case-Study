import org.apache.spark.sql.SparkSession

object DataLoader {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("DataLoader")
      .master("local[*]")
      .getOrCreate()

    // Load dataset (replace with actual download logic if needed)
    val df = spark.read.option("header", "true").csv("data/online_retail.csv")
    df.show(5)
  }
}
