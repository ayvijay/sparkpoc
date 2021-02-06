import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object firsttest {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("firsttest").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    val data = spark.read.textFile("C:\\bigdata\\datasets\\bigdata.txt")
    val words = data.flatMap(rec => rec.split(" "))
    val wordCountDs = words.groupByKey(x => x.toLowerCase).count()

    wordCountDs
      //.toDF("word","count")
      .show()

    spark.stop()
  }
}