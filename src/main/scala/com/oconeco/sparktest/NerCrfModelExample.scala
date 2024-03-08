package com.oconeco.sparktest
// code/deployment status: WORKING
//https://sparknlp.org/api/com/johnsnowlabs/nlp/annotators/ner/crf/NerCrfModel.html

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

    val pipeline = new Pipeline().setStages(Array(
      documentAssembler,
      sentence,
      tokenizer,
      embeddings,
      posTagger,
      nerTagger
    ))

    val data = Seq("U.N. official Ekeus heads for Baghdad. Spark is a tool for big companies like Google, Amazon, and MS. Kevin Butler does not like it. Sean is learning"). toDF("text")
    val result = pipeline.fit(data).transform(data)

    println("Result schema:")
    result.printSchema()

    // Replace "entities" with the actual name of your column containing the NER results
    val explodedDF = result.withColumn("entity", explode($"ner"))

    // Now, you can select the specific fields of interest from the "entity" column
    val entitiesDF = explodedDF.select(
      $"entity.result".as("entity_text"),
      $"entity.metadata.word".as("entity_word"),
      $"entity.metadata.sentence".as("entity_sentence"),
      $"entity.begin".as("start_pos"),
      $"entity.end".as("end_pos")
    )

    entitiesDF.show(40, false)


//    result.select("ner.result").show(false)

    println("Done??...")
  }
}
