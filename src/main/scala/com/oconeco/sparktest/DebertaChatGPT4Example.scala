package com.oconeco.sparktest
// code/deployment status: broken

import org.apache.spark.sql.SparkSession
import com.johnsnowlabs.nlp.DocumentAssembler
import com.johnsnowlabs.nlp.annotator._
import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import org.apache.spark.sql.SparkSession
import com.johnsnowlabs.nlp.SparkNLP
import com.johnsnowlabs.nlp.base._
import org.apache.spark.ml.Pipeline

object DebertaChatGPT4Example {
  def main(args: Array[String]): Unit = {

    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("Spark NLP DeBERTa NER")
      .master("local[*]") // Use local in a non-distributed mode; replace as necessary
      .config("spark.driver.memory", "4G")
      .config("spark.kryoserializer.buffer.max", "1G")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    // Initialize Spark NLP
    SparkNLP.start()

    // Log Spark NLP version
    println("Spark NLP version: " + SparkNLP.version())

    // Define a simple pipeline using DeBERTa for NER
    val documentAssembler = new DocumentAssembler()
      .setInputCol("text")
      .setOutputCol("document")

    val sentenceDetector = new SentenceDetector()
      .setInputCols("document")
      .setOutputCol("sentence")

    val embeddings = BertSentenceEmbeddings.pretrained("sent_small_bert_L2_128")
      .setInputCols("sentence")
      .setOutputCol("sentence_bert_embeddings")
      .setCaseSensitive(true)
      .setMaxSentenceLength(512)

    val tokenizer = new Tokenizer()
      .setInputCols("sentence")
      .setOutputCol("token")

    // Replace "deberta_base_ontonotes" with the actual name of the model you intend to use
    // This is just a placeholder and might not match the actual model name in Spark NLP
    val nerModel = NerDLModel.pretrained("deberta_v3_base_token_classifier_ontonotes", "en")
      .setInputCols("sentence", "token")
      .setOutputCol("ner")

    val nerConverter = new NerConverter()
      .setInputCols("document", "token", "ner")
      .setOutputCol("ner_chunk")

    import spark.implicits._
    // Load some example data to apply NER
    val data = Seq("Jeff visited New York last week.").toDF("text")
    val pipeline = new Pipeline().setStages(Array(
      documentAssembler,
      sentenceDetector,
      embeddings,
      tokenizer,
      nerModel,
      nerConverter))
    val result = pipeline.fit(data).transform(data)

    // Show results
    result.select("ner_chunk.result").show(false)
  }
}
