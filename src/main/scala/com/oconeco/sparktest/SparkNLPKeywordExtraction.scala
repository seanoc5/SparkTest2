package com.oconeco

import com.johnsnowlabs.nlp.{DocumentAssembler, Finisher}
import com.johnsnowlabs.nlp.annotator.{Normalizer, Tokenizer}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.SparkSession


object SparkNLPKeywordExtraction {
  def main(args: Array[String]): Unit = {
    // Create a Spark Session
    val spark = SparkSession.builder()
      .appName("SparkNLPKeywordExtraction")
      .master("local[*]") // Use "local[*]" for testing, specify your master for production
      .getOrCreate()


    import spark.implicits._
    // Sample text data
    val textData = Seq(
      (1, "Spark NLP is an open-source text processing library for advanced natural language processing."),
      (2, "It provides easy-to-use APIs and scalable algorithms for NLP tasks.")
    ).toDF("id", "text")

    // Define the Spark NLP pipeline
    val documentAssembler = new DocumentAssembler()
      .setInputCol("text")
      .setOutputCol("document")

    val tokenizer = new Tokenizer()
      .setInputCols("document")
      .setOutputCol("token")

    val normalizer = new Normalizer()
      .setInputCols("token")
      .setOutputCol("normalized")
      .setLowercase(true)

    val finisher = new Finisher()
      .setInputCols("normalized")
      .setOutputCols("keywords")
      .setValueSplitSymbol(" ") // Use space as a separator for the tokens in the finished column
      .setAnnotationSplitSymbol(",") // Use comma to separate different annotations

    // Build the pipeline
    val pipeline = new Pipeline().setStages(Array(documentAssembler, tokenizer, normalizer, finisher))

    // Fit the pipeline to the data
    val model = pipeline.fit(textData)

    // Transform the data
    val results = model.transform(textData)

    // Show the extracted keywords
    results.select("id", "keywords").show(false)

    spark.stop()
  }

}
