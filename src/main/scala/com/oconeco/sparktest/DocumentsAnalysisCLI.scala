//package com.oconeco.sparktest
//
//// code/deployment status: WORKING
////https://sparknlp.org/api/com/johnsnowlabs/nlp/annotators/ner/crf/NerCrfModel.html
//// this is more current than ContentDocumentsAnalysis
//
//import cats.effect.{ExitCode, IO, IOApp}
//import cats.implicits.catsSyntaxTuple2Semigroupal
//import com.johnsnowlabs.nlp.annotator.NerConverter
//import com.johnsnowlabs.nlp.annotators.Tokenizer
//import com.johnsnowlabs.nlp.annotators.keyword.yake.YakeKeywordExtraction
//import com.johnsnowlabs.nlp.annotators.ner.crf.NerCrfModel
//import com.johnsnowlabs.nlp.annotators.pos.perceptron.PerceptronModel
//import com.johnsnowlabs.nlp.annotators.sbd.pragmatic.SentenceDetector
//import com.johnsnowlabs.nlp.base.DocumentAssembler
//import com.johnsnowlabs.nlp.embeddings.WordEmbeddingsModel
//import com.monovore.decline.{Command, Opts}
////import org.apache.logging.log4j.LogManager
//import org.apache.log4j.LogManager
//import org.apache.spark.ml.Pipeline
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.{DataFrame, SparkSession}
//
//object DocumentsAnalysisCLI extends IOApp {
//  private val logger = LogManager.getLogger(getClass.getName)
//
//  private val usernameOpt = Opts.option[String]("username", help = "The user's username.")
//  private val passwordOpt = Opts.option[String]("password", help = "The user's password.", metavar = "secret")
//
//  private val greetingCommand = Command(name = "greet", header = "Greets the user with the provided username and password.") {
//    (usernameOpt, passwordOpt).mapN { (username, password) =>
//      println(s"Welcome, $username! Your password is $password")
//    }
//  }
//
//  //  def main(args: Array[String]): Unit = {
////  override def run(args: List[String]): IO[ExitCode] = {
//    greetingCommand.parse(args) match {
//      case Left(help) =>
//        IO(System.err.println(help)).as(ExitCode.Error)
//      case Right(run) =>
//        IO(run).as(ExitCode.Success)
//    }
//
//    val start = System.nanoTime()
//
//    //    val user = usernameOpt.orEmpty // "sean" // todo -- move these to params, get out of code...
//    //    val pass = "pass1234"
//    val targetPartions = 5
//    val batchSize = 3000
//    val bodyMinSize = 1000
//    val bodyMaxSize = 10000
//
//    logger.info(s"Starting ${this.getClass.getSimpleName}...")
//    //    val driverFoo = new Driver()
//
//    val spark = SparkSession
//      .builder
//      //      .master("spark://dell:7077")
//      .master("local[8]")
//      .appName("Document Analysis")
//      .getOrCreate()
//
//
//    val pushdownQuery =
//      s"""select c.*, src.label as source
//         |from content c
//         | left join source src on c.source_id = src.id
//         |where structure_size > $bodyMinSize and structure_size < $bodyMaxSize
//         |order by last_updated desc
//         |limit $batchSize""".stripMargin
//
//    val jdbcUrl = "jdbc:postgresql://dell/cm_dev"
//    logger.info(s"get DB data: ${jdbcUrl}")
//    val user = usernameOpt.toString()
//    val pass = passwordOpt.orNone
//    val dfContentDocs = spark.read.format("jdbc") // todo - need to figure out how to load with reasonable partitions (currently calling rdd.repartition()
//      .option("url", jdbcUrl)
//      .option("dbtable", s"(${pushdownQuery}) as qryTable")
//      .option("driver", "org.postgresql.Driver")
//      //      .option("user", usernameOpt.toString())
//      .option("user", user)
//      .option("password", passwordOpt.toString())
//      //      .option("query", pushdownQuery)
//      .option("partitionColumn", "display_order")
//      // lowest value to pull data for with the partitionColumn
//      .option("lowerBound", "0")
//      // max value to pull data for with the partitionColumn
//      .option("upperBound", "20")
//      // number of partitions to distribute the data into. Do not set this very large (~hundreds)
//      .option("numPartitions", targetPartions)
//      .load()
//
//    //    val bigPartitions = dfBig.rdd.getNumPartitions
//    //    logger.info(s"BIG Number of partions from Postgres load: ${bigPartitions} ====================")
//
//    //    val dfContentDocs = dfBig.repartition(targetPartions)
//    val repartCount = dfContentDocs.rdd.getNumPartitions
//    logger.info(s"Number of partions from Postgres load: ${repartCount} ====================")
//
//    dfContentDocs.printSchema()
//    dfContentDocs.show(2, 120, true)
//
//    // ---------------------- BODY ----------------------
//    logger.info("Build body pipeline...")
//    val bodyPipeline: Pipeline = buildBodyPipeline("body_text")
//    val bodyModel = bodyPipeline.fit(dfContentDocs)
//    val dfBodyTransformed = bodyModel.transform(dfContentDocs)
//
//    val dfBodyResult = dfBodyTransformed
//      .withColumn("sentences", expr("transform(sentence_struct, x -> x.result)"))
//      .withColumn("entities", expr("transform(ner_chunk, x -> x.result)"))
//      .withColumn("keywords", expr("transform(yake_keywords, x -> x.result)"))
//      .drop("ner_chunk", "yake_keywords", "document", "word_embeddings", "pos", "sentence_struct", "token", "ner")
//    dfBodyResult.show(5, 150, true)
//
//    saveContentToSolr(dfBodyResult, "corpusminder", "192.168.0.17:2181")
//
//    val end = System.nanoTime()
//    logger.info(s"Elapsed time: ${(end - start) / 1e9} seconds")
//
//    spark.stop()
//    logger.info("Done??...")
//
//  }
//
//
//  // --------------------- FUNCTIONS ---------------------
//  def saveContentToSolr(dfWithTimestamp: DataFrame, collectionName: String, zkHost: String): Unit = {
//    val writeOptions = Map(
//      "collection" -> collectionName,
//      "zkhost" -> zkHost
//    )
//    logger.info(s"writeOptions: ${writeOptions}")
//    val result = dfWithTimestamp.write.format("solr")
//      .options(writeOptions)
//      .mode("overwrite")
//      .save()
//    result
//  }
//
//
//  private def buildBodyPipeline(sourceField: String): Pipeline = {
//    // First extract the prerequisites for the NerCrfModel
//    val documentAssembler = new DocumentAssembler()
//      .setInputCol(sourceField)
//      .setOutputCol("document")
//
//    val sentence = new SentenceDetector()
//      .setInputCols("document")
//      .setOutputCol("sentence_struct")
//
//    val tokenizer = new Tokenizer()
//      .setInputCols("document")
//      //      .setInputCols("sentence_struct")
//      .setOutputCol("token")
//
//    val embeddings = WordEmbeddingsModel.pretrained()
//      .setInputCols("document", "token")
//      .setOutputCol("word_embeddings")
//
//    val posTagger = PerceptronModel.pretrained()
//      .setInputCols("sentence_struct", "token")
//      .setOutputCol("pos")
//
//    // Then NER can be extracted
//    val nerTagger = NerCrfModel.pretrained()
//      //      .setInputCols("sentence_struct", "token", "word_embeddings", "pos")
//      .setInputCols("document", "token", "word_embeddings", "pos")
//      .setOutputCol("ner")
//
//    val nerConverter = new NerConverter()
//      .setInputCols("document", "token", "ner")
//      .setOutputCol("ner_chunk")
//
//    val keywords = new YakeKeywordExtraction()
//      .setInputCols("token")
//      .setOutputCol("yake_keywords")
//      .setThreshold(0.6f)
//      .setMinNGrams(1)
//      .setNKeywords(10)
//
//
//    val pipeline = new Pipeline().setStages(Array(
//      documentAssembler,
//      sentence,
//      tokenizer,
//      embeddings,
//      posTagger,
//      nerTagger,
//      keywords,
//      nerConverter
//    ))
//    logger.warn(s"Pipeline: ${pipeline}")
//    pipeline
//  }
//
//  private def buildTitlePipeline(sourceField: String): Pipeline = {
//    // First extract the prerequisites for the NerCrfModel
//    val documentAssembler = new DocumentAssembler()
//      .setInputCol(sourceField)
//      .setOutputCol("document")
//
//    val tokenizer = new Tokenizer()
//      .setInputCols("document")
//      .setOutputCol("token")
//
//    val embeddings = WordEmbeddingsModel.pretrained()
//      .setInputCols("document", "token")
//      .setOutputCol("word_embeddings")
//
//    val posTagger = PerceptronModel.pretrained()
//      .setInputCols("document", "token")
//      .setOutputCol("pos")
//
//    // Then NER can be extracted
//    val nerTagger = NerCrfModel.pretrained()
//      .setInputCols("document", "token", "word_embeddings", "pos")
//      .setOutputCol("ner")
//
//    val nerConverter = new NerConverter()
//      .setInputCols("document", "token", "ner")
//      .setOutputCol("ner_chunk")
//
//    val keywords = new YakeKeywordExtraction()
//      .setInputCols("token")
//      .setOutputCol("yake_keywords")
//      .setThreshold(0.6f)
//      .setMinNGrams(1)
//      .setNKeywords(3)
//
//
//    val pipeline = new Pipeline().setStages(Array(
//      documentAssembler,
//      tokenizer,
//      embeddings,
//      posTagger,
//      nerTagger,
//      keywords,
//      nerConverter
//    ))
//    pipeline
//  }
//
//
//  def time[R](block: => R): R = {
//    val start = System.nanoTime()
//    val result = block
//    val end = System.nanoTime()
//    logger.info(s"Elapsed time: ${(end - start) / 1e9} seconds")
//    result
//  }
//}
