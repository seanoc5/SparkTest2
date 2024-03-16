package com.oconeco.sparktest
// code/deployment status: WORKING
//https://sparknlp.org/api/com/johnsnowlabs/nlp/annotators/ner/crf/NerCrfModel.html
// this is more current than ContentDocumentsAnalysis

import com.johnsnowlabs.nlp.annotator.NerConverter
import com.johnsnowlabs.nlp.annotators.Tokenizer
import com.johnsnowlabs.nlp.annotators.keyword.yake.YakeKeywordExtraction
import com.johnsnowlabs.nlp.annotators.ner.crf.NerCrfModel
import com.johnsnowlabs.nlp.annotators.pos.perceptron.PerceptronModel
import com.johnsnowlabs.nlp.annotators.sbd.pragmatic.SentenceDetector
import com.johnsnowlabs.nlp.base.DocumentAssembler
import com.johnsnowlabs.nlp.embeddings.WordEmbeddingsModel
import org.apache.log4j.LogManager
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions._
import org.postgresql.Driver

object DocumentsAnalysis {
  private val logger = LogManager.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {

    val user = "sean" // todo -- move these to params, get out of code...
    val pass = "pass1234"
    logger.info(s"Starting ${this.getClass.getSimpleName}...")
    val driverFoo = new Driver()

    val spark = SparkSession
      .builder
//      .master("spark://dell:7077")
      .master("local[8]")
      .appName("Document Analysis")
      .getOrCreate()

    val batchSize = 3
    val bodyMinSize = 1000
    val bodyMaxSize = 10000

    val jdbcUrl = "jdbc:postgresql://dell/cm_dev"
    logger.info(s"get DB data: ${jdbcUrl}")
    val dfContentDocs = spark.read.format("jdbc")
      .option("url", jdbcUrl)
      .option("driver", "org.postgresql.Driver")
      .option("user", user)
      .option("password", pass)
      .option("query",
        s"""select c.*, src.label as source
           |from content c
           | left join source src on c.source_id = src.id
           |where structure_size > $bodyMinSize and structure_size < $bodyMaxSize
           |orderby last_modified desc
           |limit $batchSize""".stripMargin)
//      .option("partitionColumn", "display_order")
      // lowest value to pull data for with the partitionColumn
//      .option("lowerBound", "0")
      // max value to pull data for with the partitionColumn
//      .option("upperBound", "20")
      // number of partitions to distribute the data into. Do not set this very large (~hundreds)
      .option("numPartitions", 20)
      .load()

    val numParts = dfContentDocs.rdd.getNumPartitions
    logger.error(s"Number of partions from Postgres load: ${numParts} ====================")

    dfContentDocs.printSchema()
    dfContentDocs.show(2, 120, true)

    // ---------------------- BODY ----------------------
    logger.info("Build body pipeline...")
    val bodyPipeline: Pipeline = buildBodyPipeline("body_text")
    val bodyModel = bodyPipeline.fit(dfContentDocs)
    val dfBodyTransformed = bodyModel.transform(dfContentDocs)

    val dfBodyResult = dfBodyTransformed
      .withColumn("sentences", expr("transform(sentence_struct, x -> x.result)"))
      .withColumn("entities", expr("transform(ner_chunk, x -> x.result)"))
      .withColumn("keywords", expr("transform(yake_keywords, x -> x.result)"))
      .drop("ner_chunk", "yake_keywords", "document", "word_embeddings", "pos", "sentence_struct", "token", "ner")
    dfBodyResult.show(5, 150, true)

    saveContentToSolr(dfBodyResult, "corpusminder", "192.168.0.17:2181")

    spark.stop()
    logger.info("Done??...")
  }


  // --------------------- FUNCTIONS ---------------------
  def saveContentToSolr(dfWithTimestamp: DataFrame, collectionName:String, zkHost:String): Unit = {
    val writeOptions = Map(
      "collection" -> collectionName,
      "zkhost" -> zkHost
    )
    logger.info(s"writeOptions: ${writeOptions}")
    val result = dfWithTimestamp.write.format("solr")
      .options(writeOptions)
      .mode("overwrite")
      .save()
    result
  }


  private def buildBodyPipeline(sourceField: String): Pipeline = {
    // First extract the prerequisites for the NerCrfModel
    val documentAssembler = new DocumentAssembler()
      .setInputCol(sourceField)
      .setOutputCol("document")

    val sentence = new SentenceDetector()
      .setInputCols("document")
      .setOutputCol("sentence_struct")

    val tokenizer = new Tokenizer()
      .setInputCols("document")
//      .setInputCols("sentence_struct")
      .setOutputCol("token")

    val embeddings = WordEmbeddingsModel.pretrained()
      .setInputCols("document", "token")
      .setOutputCol("word_embeddings")

    val posTagger = PerceptronModel.pretrained()
      .setInputCols("sentence_struct", "token")
      .setOutputCol("pos")

    // Then NER can be extracted
    val nerTagger = NerCrfModel.pretrained()
//      .setInputCols("sentence_struct", "token", "word_embeddings", "pos")
      .setInputCols("document", "token", "word_embeddings", "pos")
      .setOutputCol("ner")

    val nerConverter = new NerConverter()
      .setInputCols("document", "token", "ner")
      .setOutputCol("ner_chunk")

    val keywords = new YakeKeywordExtraction()
      .setInputCols("token")
      .setOutputCol("yake_keywords")
      .setThreshold(0.6f)
      .setMinNGrams(1)
      .setNKeywords(10)


    val pipeline = new Pipeline().setStages(Array(
      documentAssembler,
      sentence,
      tokenizer,
      embeddings,
      posTagger,
      nerTagger,
      keywords,
      nerConverter
    ))
    logger.warn(s"Pipeline: ${pipeline}")
    pipeline
  }

  private def buildTitlePipeline(sourceField: String): Pipeline = {
    // First extract the prerequisites for the NerCrfModel
    val documentAssembler = new DocumentAssembler()
      .setInputCol(sourceField)
      .setOutputCol("document")

    val tokenizer = new Tokenizer()
      .setInputCols("document")
      .setOutputCol("token")

    val embeddings = WordEmbeddingsModel.pretrained()
      .setInputCols("document", "token")
      .setOutputCol("word_embeddings")

    val posTagger = PerceptronModel.pretrained()
      .setInputCols("document", "token")
      .setOutputCol("pos")

    // Then NER can be extracted
    val nerTagger = NerCrfModel.pretrained()
      .setInputCols("document", "token", "word_embeddings", "pos")
      .setOutputCol("ner")

    val nerConverter = new NerConverter()
      .setInputCols("document", "token", "ner")
      .setOutputCol("ner_chunk")

    val keywords = new YakeKeywordExtraction()
      .setInputCols("token")
      .setOutputCol("yake_keywords")
      .setThreshold(0.6f)
      .setMinNGrams(1)
      .setNKeywords(3)


    val pipeline = new Pipeline().setStages(Array(
      documentAssembler,
      tokenizer,
      embeddings,
      posTagger,
      nerTagger,
      keywords,
      nerConverter
    ))
    pipeline
  }


  def time[R](block: => R): R = {
    val start = System.nanoTime()
    val result = block
    val end = System.nanoTime()
    logger.info(s"Elapsed time: ${(end - start) / 1e9} seconds")
    result
  }
}
