package com.oconeco.sparktest
// code/deployment status: WORKING
//https://sparknlp.org/api/com/johnsnowlabs/nlp/annotators/ner/crf/NerCrfModel.html

import com.johnsnowlabs.nlp.annotator.NerConverter
import com.johnsnowlabs.nlp.annotators.Tokenizer
import com.johnsnowlabs.nlp.annotators.keyword.yake.YakeKeywordExtraction
import com.johnsnowlabs.nlp.annotators.ner.crf.NerCrfModel
import com.johnsnowlabs.nlp.annotators.pos.perceptron.PerceptronModel
import com.johnsnowlabs.nlp.annotators.sbd.pragmatic.SentenceDetector
import com.johnsnowlabs.nlp.base.{DocumentAssembler, LightPipeline}
import com.johnsnowlabs.nlp.embeddings.WordEmbeddingsModel
import com.oconeco.sparktest.ContentDocumentsAnalysis.buildTitlePipeline
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array_join, col, collect_list, concat_ws, expr, sort_array}

object ContentDocumentsAnalysis {
  def main(args: Array[String]): Unit = {

    println(s"Starting ${this.getClass.getSimpleName}...")
    val spark = SparkSession
      .builder
      .master("local[8]")
      .appName("Context Docs Analysis")
      .getOrCreate()

    val connectionProperties = new java.util.Properties()
    connectionProperties.put("user", "corpusminder")
    connectionProperties.put("password", "pass1234")

    val batchSize = 3
    val bodyMinSize = 1000
    val bodyMaxSize = 10000

    val jdbcUrl = "jdbc:postgresql://dell/cm_dev?user=corpusminder&password=pass1234"
    println("get DB data...")
    val dfContentDocs = spark.read.format("jdbc")
      .option("url", jdbcUrl)
      .option("query", s"select id, title, uri, body_text from content where structure_size > ${bodyMinSize} and structure_size < ${bodyMaxSize} limit ${batchSize}")
      .load()
    //    dfContextDocs.show(10, 120, true)
    //    dfContextDocs.printSchema()

    import spark.implicits._

    // ---------------------- BODY ----------------------
    println("Build body pipeline...")
    val bodyPipeline: Pipeline = buildBodyPipeline("body_text")
    println("fit body pipeline...")
    val bodyModel = bodyPipeline.fit(dfContentDocs)
    println("transform body pipeline...")
    val dfBodyResult = bodyModel.transform(dfContentDocs)
    //    bodyResult.printSchema()
    println("show body dataframe...")
    dfBodyResult.show(5, 120, true)

    println("get body chunks...")
    val dfBodyExplodeNerChunks = dfBodyResult.selectExpr("id as docId", "explode(ner_chunk) as explodedChunks")
    println("exploded body chunks schema:")
    val dfBodyChunks = dfBodyExplodeNerChunks.withColumn("entity", $"explodedChunks.result".cast("string"))
      .withColumn("entity_type", $"explodedChunks.metadata.entity".cast("string"))
      .withColumn("sentence_num", $"explodedChunks.metadata.sentence".cast("integer"))
      .withColumn("start_pos", $"explodedChunks.metadata.start".cast("integer"))
      .withColumn("id", concat_ws("-", col("docId"), $"entity_type", $"explodedChunks.begin".cast("string")))
      .drop("explodedChunks")
    dfBodyChunks.printSchema()
    dfBodyChunks.show(5, 120, true)

    println("pivot and rejoin main df to save to solr...")
    val dfEntGroup = dfBodyChunks.groupBy("docId")
      .pivot("entity_type")
      .agg(
        array_join(
          sort_array(collect_list($"entity")), ", "
        )
      )

    dfEntGroup.printSchema()
    dfEntGroup.show(10,120,true)

    // ---------------------- Write DataFrame to a new table in PostgreSQL ----------------------
    // Define connection properties
    println("Starting save dfBodyChunks to postgresql...")
    dfBodyChunks.write
      .mode("overwrite") // Specifies the behavior when data or table already exists. Options include: append, overwrite, ignore, error, errorifexists.
      .jdbc(jdbcUrl, "document_entities", connectionProperties)
    println("Finished saving bodyChunks")


    // ---------------------- TITLE ----------------------
    val titlePipeline: Pipeline = buildTitlePipeline("title")
    val titleModel = titlePipeline.fit(dfContentDocs)
    val titleResult = titleModel.transform(dfContentDocs)
    val titleKeywords = titleResult.select("keywords")
    println("Show title keywords...")
    titleKeywords.show(5, 80, true)

    println("Done??...")
  }

  private def buildBodyPipeline(sourceField: String): Pipeline = {
    // First extract the prerequisites for the NerCrfModel
    val documentAssembler = new DocumentAssembler()
      .setInputCol(sourceField)
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

    val keywords = new YakeKeywordExtraction()
      .setInputCols("token")
      .setOutputCol("keywords")
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
      .setOutputCol("keywords")
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
    println(s"Elapsed time: ${(end - start) / 1e9} seconds")
    result
  }
}
