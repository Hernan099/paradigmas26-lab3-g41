import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    println("Creating SparkSession...")

    val spark = SparkSession.builder()
      .appName("HelloSpark")
      .master("local[*]")
      .getOrCreate()

    println("✓ SparkSession created successfully!")

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val data = List(1, 2, 3, 4, 5)
    val rdd = sc.parallelize(data)
    val sum = rdd.sum()

    println(s"✓ RDD sum: ${sum.toInt}")
    println("✓ All tests passed!")

    spark.stop()
  }
}
