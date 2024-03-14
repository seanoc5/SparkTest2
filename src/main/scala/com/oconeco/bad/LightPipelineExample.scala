package com.oconeco.bad

// code/deployment status: broken
//Caused by: java.io.InvalidClassException: scala.collection.mutable.WrappedArray$ofRef; local class incompatible: stream classdesc
import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import org.apache.spark.sql.SparkSession


object LightPipelineExample {
  def main(args: Array[String]): Unit = {

    val myName = this.getClass.getSimpleName
    println(s"Starting ${myName}...")

    val text =
      """Sean has been learning Apache SparkNLP for more than 100 hours and $50.
        |This is a long a tedious process for him.
        |StackTheory, Lucidworks, and AutoZone might benefit from this investment of """.stripMargin


    // Create a Spark Session
    val spark = SparkSession.builder()
      .appName(myName)
      .master("local[*]") // Use "local[*]" for testing, specify your master for production
      .getOrCreate()

    import spark.implicits._
    val data = Seq(
      text,
      "And this is a second sentence")
      .toDF("text")

    val explainDocumentPipeline = PretrainedPipeline("explain_document_ml")
    val annotations_df = explainDocumentPipeline.transform(data)
    annotations_df.show()

    println("Done!")

    spark.stop()

  }

}
