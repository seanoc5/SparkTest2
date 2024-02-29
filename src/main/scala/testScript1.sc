import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder
  .master("local[*]")
  .appName("Sample App")
  .getOrCreate()

val myRdd = spark.sparkContext.parallelize(
  Seq("I like Spark", "Spark is awesome", "My first Spark job is working now and is counting down these words")
)

//val df = rdd.map(row => (row(0))).toDF()
//val df = myRdd.

//println(df)

//val filtered = df.filter(line => line.contains("Spark"))

//filtered.collect().foreach(println)

//filtered
