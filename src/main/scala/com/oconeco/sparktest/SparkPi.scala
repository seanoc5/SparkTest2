package com.oconeco.sparktest
//code/deployment status: WORKING

import scala.math.random
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory


/** Computes an approximation to pi */
object SparkPi {

  val logger = LoggerFactory.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
//      .master("spark://dell:7077")
      .appName("Spark Pi")
      .getOrCreate()
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    val count = spark.sparkContext.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y <= 1) 1 else 0
    }.reduce(_ + _)

    logger.info(s"Pi is roughly ${4.0 * count / (n - 1)}")

    spark.stop()
  }
}
