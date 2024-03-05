package com.oconeco.sparktest
// code/deployment status: ???
//https://sparknlp.org/api/com/johnsnowlabs/nlp/annotators/ner/crf/NerCrfModel.html

import com.johnsnowlabs.nlp.base.DocumentAssembler
import com.johnsnowlabs.nlp.annotators.Tokenizer
import com.johnsnowlabs.nlp.annotators.sbd.pragmatic.SentenceDetector
import com.johnsnowlabs.nlp.embeddings.WordEmbeddingsModel
import com.johnsnowlabs.nlp.annotators.pos.perceptron.PerceptronModel
import com.johnsnowlabs.nlp.annotators.ner.crf.NerCrfModel
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.SparkSession

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

    val data = Seq("U.N. official Ekeus heads for Baghdad."). toDF("text")
    val result = pipeline.fit(data).transform(data)

    result.select("ner.result").show(false)

    println("Done??...")
  }
}
