package com.oconeco.sparktest
//https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5276288487867556/871609068453572/3684151997220649/latest.html
//code/deployment status: ???

import com.johnsnowlabs.nlp.base.DocumentAssembler
import com.johnsnowlabs.nlp.annotators.Tokenizer
import com.johnsnowlabs.nlp.annotators.sbd.pragmatic.SentenceDetector
import com.johnsnowlabs.nlp.embeddings.WordEmbeddingsModel
import com.johnsnowlabs.nlp.annotators.pos.perceptron.PerceptronModel
import com.johnsnowlabs.nlp.training.CoNLL
import com.johnsnowlabs.nlp.annotator.NerCrfApproach
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.SparkSession

object DatabricksNerCRFApproachExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[8]")
      .appName("DataBricks NER Example")
      .getOrCreate()

    val documentAssembler = new DocumentAssembler()
      .setInputCol("text")
      .setOutputCol("document")

    val sentence = new SentenceDetector()
      .setInputCols("document")
      .setOutputCol("sentence")

    val tokenizer = new Tokenizer()
      .setInputCols("sentence")
      .setOutputCol("token")

    val posTagger = PerceptronModel.pretrained()
      .setInputCols("sentence", "token")
      .setOutputCol("pos")

    // Then the training can start
    val embeddings = WordEmbeddingsModel.pretrained()
      .setInputCols("sentence", "token")
      .setOutputCol("embeddings")
      .setCaseSensitive(false)

    val nerTagger = new NerCrfApproach()
      .setInputCols("sentence", "token", "pos", "embeddings")
      .setLabelColumn("label")
      .setMinEpochs(1)
      .setMaxEpochs(3)
      .setOutputCol("ner")

    val pipeline = new Pipeline().setStages(Array(
      embeddings,
      nerTagger
    ))


    // We use the sentences, tokens, POS tags and labels from the CoNLL dataset.
    val conll = CoNLL()
    val trainingData = conll.readDataset(spark, "data/eng.train")

    val pipelineModel = pipeline.fit(trainingData)

//    pipelineModel.

  }
}
