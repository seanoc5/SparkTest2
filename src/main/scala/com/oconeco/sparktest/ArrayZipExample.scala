package com.oconeco.sparktest

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ArrayZipExample {
  case class Input(
                    userId: Integer,
                    someString: String,
                    varA: Array[Integer],
                    varB: Array[Integer])

  case class Result(
                     userId: Integer,
                     someString: String,
                     varA: Integer,
                     varB: Integer)

  // https://stackoverflow.com/questions/33220916/explode-transpose-multiple-columns-in-spark-sql-table
  def main(args: Array[String]): Unit = {

    println(s"Starting ${this.getClass.getSimpleName}...")
    val spark = SparkSession
      .builder
      .master("local[8]")
      .appName("Context Docs Analysis")
      .getOrCreate()

    import spark.implicits._

    def getResult(row: Input): Iterable[Result] = {
      val user_id = row.userId
      val someString = row.someString
      val varA = row.varA
      val varB = row.varB
      val seq = for (i <- 0 until varA.size) yield {
        Result(user_id, someString, varA(i), varB(i))
      }
      seq
    }

    val obj1 = Input(1, "string1", Array(0, 2, 5), Array(1, 2, 9))
    val obj2 = Input(2, "string2", Array(1, 3, 6), Array(2, 3, 10))
    val df = spark.sparkContext.parallelize(Seq(obj1, obj2)).toDS
    println("Source dataframe:")
    df.printSchema()
    df.show()

    println("works, but not great (flatmap??)...?")
    val res = df.flatMap { row => getResult(row) }
    res.show

    println("better suggestion: arrays_zip...")
    val dfBetter = df.withColumn("vars", explode(arrays_zip($"varA", $"varB"))).select(
      $"userId", $"someString",
      $"vars.varA", $"vars.varB")
      dfBetter.show

    println("Done")
  }
}
