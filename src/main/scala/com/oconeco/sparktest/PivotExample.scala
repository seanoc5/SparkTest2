package com.oconeco.sparktest
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object PivotExample {
  def main(args: Array[String]): Unit = {

    println(s"Starting ${this.getClass.getSimpleName}...")
    val spark = SparkSession
      .builder
      .master("local[8]")
      .appName("Context Docs Analysis")
      .getOrCreate()

    import spark.implicits._

    val df = spark.sparkContext.parallelize(Seq(
            ("A", "X", "done"),
            ("A", "Y", "done"),
            ("A", "Z", "done"),
            ("C", "X", "done"),
            ("C", "Y", "done"),
            ("B", "Y", "done")
          )).toDF("Company", "Type", "Status")

    val result = df.groupBy("Company")
        .pivot("Type")
        .agg(expr("coalesce(first(Status), \"pending\")"))

    result.show()
//    +-------+-------+----+-------+
//    |Company|      X|   Y|      Z|
//    +-------+-------+----+-------+
//    |      B|pending|done|pending|
//    |      C|   done|done|pending|
//    |      A|   done|done|   done|
//    +-------+-------+----+-------+

    println("Done")
  }

}
