package com.oconeco

import com.johnsnowlabs.nlp.base._
import com.johnsnowlabs.nlp.annotator._
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.SparkSession

object SparkNLPPosAndNer {
  def main(args: Array[String]): Unit = {
    // Create a Spark session
    val spark = SparkSession.builder()
      .appName("SparkNLPPosAndNer")
      .master("local[1]") // Use local for testing, set your master for production
      .getOrCreate()

    import spark.implicits._
    // Sample paragraph of text
    val sampleText = Seq(
      (1, "John Snow, a leading figure in the development of modern epidemiology, was born in York, England.")
    ).toDF("id", "text")

    // Define the Spark NLP pipeline
    val documentAssembler = new DocumentAssembler()
      .setInputCol("text")
      .setOutputCol("document")

    val sentenceDetector = new SentenceDetector()
      .setInputCols("document")
      .setOutputCol("sentence")

    val tokenizer = new Tokenizer()
      .setInputCols("sentence")
      .setOutputCol("token")

    val wordEmbeddings = WordEmbeddingsModel.pretrained()
          .setInputCols("document", "token")
          .setOutputCol("word_embeddings")

    val posTagger = PerceptronModel.pretrained() // Pretrained POS model
      .setInputCols("sentence", "token")
      .setOutputCol("pos")

    val nerTagger = NerDLModel.pretrained() // Pretrained NER model
      .setInputCols("sentence", "token", "pos")
      .setOutputCol("ner")


    val converter = new NerConverter()
      .setInputCols("sentence", "token", "ner")
      .setOutputCol("ner_chunk")

    val pipeline = new Pipeline().setStages(Array(
      documentAssembler,
      sentenceDetector,
      tokenizer,
      posTagger,
      nerTagger,
      converter
    ))

    // Fit the pipeline to the sample text
    val model = pipeline.fit(sampleText)

    // Transform the sample text
    val results = model.transform(sampleText)

    // Show the results
//    results.select("id", "pos.result", "ner_chunk.result").show(false)
    results.show(false)

    spark.stop()
  }
}
