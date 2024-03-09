package com.oconeco.sparktest
// code/deployment status: WORKING
//https://sparknlp.org/api/com/johnsnowlabs/nlp/annotators/ner/crf/NerCrfModel.html

import com.johnsnowlabs.nlp.annotator.NerConverter
import com.johnsnowlabs.nlp.base.DocumentAssembler
import com.johnsnowlabs.nlp.annotators.Tokenizer
import com.johnsnowlabs.nlp.annotators.sbd.pragmatic.SentenceDetector
import com.johnsnowlabs.nlp.embeddings.WordEmbeddingsModel
import com.johnsnowlabs.nlp.annotators.pos.perceptron.PerceptronModel
import com.johnsnowlabs.nlp.annotators.ner.crf.NerCrfModel
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode

object NerCrfModelExample {
  def main(args: Array[String]): Unit = {

    println(s"Starting ${this.getClass.getSimpleName}...")

    val spark = SparkSession
      .builder
      .master("local[8]")
      .appName("NER CRF Example")
      .getOrCreate()

    import spark.implicits._

    // First extract the prerequisites for the NerCrfModel
    val documentAssembler = new DocumentAssembler()
      .setInputCol("text")
      .setOutputCol("document")

    val sentence = new SentenceDetector()
      .setInputCols("document")
      .setOutputCol("sentence")

    val tokenizer = new Tokenizer()
      .setInputCols("sentence")
      .setOutputCol("token")

    val embeddings = WordEmbeddingsModel.pretrained()
      .setInputCols("sentence", "token")
      .setOutputCol("word_embeddings")

    val posTagger = PerceptronModel.pretrained()
      .setInputCols("sentence", "token")
      .setOutputCol("pos")

    // Then NER can be extracted
    val nerTagger = NerCrfModel.pretrained()
      .setInputCols("sentence", "token", "word_embeddings", "pos")
      .setOutputCol("ner")

    val nerConverter = new NerConverter()
      .setInputCols("document", "token", "ner")
      .setOutputCol("ner_chunk")


    val pipeline = new Pipeline().setStages(Array(
      documentAssembler,
      sentence,
      tokenizer,
      embeddings,
      posTagger,
      nerTagger,
      nerConverter
    ))

    val data = Seq("John Smith, is a fictional name and was born in Albany, New York. Spark is a tool for big companies like Google, Amazon, and Mr. Kevin Butler does not like it.", "Sean is learning Scala."). toDF("text")
    val result = pipeline.fit(data).transform(data)

    println("Result schema:")
    result.printSchema()

    result.selectExpr("explode(ner_chunk)").show(40,80,true)

    // Replace "entities" with the actual name of your column containing the NER results
    val explodedDF = result.withColumn("nerentity", explode($"ner"))

    // Now, you can select the specific fields of interest from the "entity" column
    val entitiesDF = explodedDF.select(
      $"entity.result".as("entity_text"),
      $"entity.metadata.word".as("entity_word"),
      $"entity.metadata.sentence".as("entity_sentence"),
      $"entity.begin".as("start_pos"),
      $"entity.end".as("end_pos")
    )

    entitiesDF.show(80, 120)


//    result.select("ner.result").show(false)

    println("Done??...")
  }
}
