package com.oconeco.sparktest

import com.johnsnowlabs.nlp.annotator.NerConverter
import com.johnsnowlabs.nlp.annotators.Tokenizer
import com.johnsnowlabs.nlp.annotators.keyword.yake.YakeKeywordExtraction
import com.johnsnowlabs.nlp.annotators.ner.crf.NerCrfModel
import com.johnsnowlabs.nlp.annotators.pos.perceptron.PerceptronModel
import com.johnsnowlabs.nlp.annotators.sbd.pragmatic.SentenceDetector
import com.johnsnowlabs.nlp.base.DocumentAssembler
import com.johnsnowlabs.nlp.embeddings.WordEmbeddingsModel
import com.oconeco.sparktest.DocumentsAnalysis.{logger, saveContentToSolr}
import org.apache.logging.log4j.LogManager
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions.{getClass, _}
import org.apache.spark.sql.{DataFrame, SparkSession}
import picocli.CommandLine
import picocli.CommandLine.{Command, Option}

import java.sql.{Connection, DriverManager}


@Command(name = "SimpleApp", mixinStandardHelpOptions = true, version = Array("SimpleApp 1.0"),
  description = Array("Processes user credentials."))
class SimpleApp extends Runnable {
  private val logger = LogManager.getLogger(getClass.getName)

  @Option(names = Array("-u", "--username"), defaultValue = "sean", description = Array("The user's username."))
  private var username: String = _

  @Option(names = Array("-p", "--password"), required = true, description = Array("The user's password."))
  private var password: String = _

  @Option(names = Array("-n", "--minSize"), defaultValue = "100", description = Array("Minimum content size"))
  private var minSize: Integer = _

  @Option(names = Array("-x", "--maxSize"), defaultValue = "1000000",  description = Array("Maximum content size."))
  private var maxSize: Integer = _

  @Option(names = Array("-b", "--batchSize"), defaultValue = "100",  description = Array("Batch size for reading source db records size."))
  private var batchSize: Integer = _

  @Option(names = Array("-t", "--targetPartitions"), description = Array("Target count of partitions for analysis and save to solr"))
  private var tartgetPartitions: Long = 10

  override def run(): Unit = {
    logger.info(s"Welcome, $username! Your password is $password")

    val master = "local[8]"
    val appName = "Document Analysis"
    val spark: SparkSession = getSparkSession(master, appName)

    val dfContentDocs: DataFrame = loadContentDBRecords(spark)

    val partionCount = dfContentDocs.rdd.getNumPartitions
    logger.info(s"Number of partions from Postgres load: ${partionCount} ====================")

//    dfContentDocs.show(5,180,true)

/*    dfContentDocs.foreachPartition { partition =>
      // Note : Each partition one connection (more better way is to use connection pools)
      val sqlExecutorConnection: Connection = DriverManager.getConnection(sqlDatabaseConnectionString)
      //Batch size of 1000 is used since some databases cant use batch size more than 1000 for ex : Azure sql
      partition.grouped(1000).foreach {
        group =>
          val insertString: scala.collection.mutable.StringBuilder = new scala.collection.mutable.StringBuilder()
          group.foreach {
            record => insertString.append("('" + record.mkString(",") + "'),")
          }

          sqlExecutorConnection.createStatement()
            .executeUpdate(f"INSERT INTO [$sqlTableName] ($tableHeader) VALUES "
              + insertString.stripSuffix(","))
      }
      sqlExecutorConnection.close() // close the connection so that connections wont exhaust.
    }
    }
*/

    saveContentToSolr(dfContentDocs, "corpusminder", "192.168.0.17:2181/solr")

    spark.stop()

    logger.info("Done!")
  }




  // --------------------- FUNCTIONS ---------------------
  def saveContentToSolr(df: DataFrame, collectionName: String, zkHost: String): Unit = {
    val dfWithTimestamp = df    //.withColumn("timestamp", lit(myTimestamp))
    val writeOptions = Map(
      "collection" -> collectionName,
      "zkhost" -> zkHost
    )
    logger.info(s"writeOptions: ${writeOptions}")
    val result = dfWithTimestamp.write.format("solr")
      .options(writeOptions)
      .mode("overwrite")
      .save()
  }

  def loadContentDBRecords(spark: SparkSession): DataFrame = {
    val pushdownQuery =
      s"""select c.*, src.label as source
         |from content c
         | left join source src on c.source_id = src.id
         |where structured_size > $minSize and structured_size < $maxSize
         |and c.solr_index_date is null
         |order by last_updated desc
         |limit $batchSize""".stripMargin

    logger.info(s"Pushdown query: '${pushdownQuery}")

    val jdbcUrl = "jdbc:postgresql://dell/cm_dev"
    logger.info(s"get DB data: ${jdbcUrl}")
    val dfContentDocs = spark.read.format("jdbc") // todo - need to figure out how to load with reasonable partitions (currently calling rdd.repartition()
      .option("url", jdbcUrl)
      .option("dbtable", s"(${pushdownQuery}) as qryTable")
      .option("driver", "org.postgresql.Driver")
      .option("user", username)
      .option("password", password)
      .option("partitionColumn", "id")
      .option("lowerBound", "0")
      .option("upperBound", "37000") //36141
      .option("numPartitions", tartgetPartitions)
      .load()

    return dfContentDocs
  }

  private def getSparkSession(master: String, appName: String) = {
    val spark = SparkSession
      .builder
      .master(master)
      .appName(appName)
      .getOrCreate()
    spark
  }
}

object DBExample {
  val logger = LogManager.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    val exitCode = new CommandLine(new SimpleApp()).execute(args: _*)
    println("Done")
    System.exit(exitCode)
  }


  // --------------------- FUNCTIONS ---------------------
  def saveContentToSolr(df: DataFrame, collectionName: String, zkHost: String): Unit = {
//    val myTimestamp = new java.sql.Timestamp(new Date().getTime)

    val dfWithTimestamp = df    //.withColumn("timestamp", lit(myTimestamp))
    val writeOptions = Map(
      "collection" -> collectionName,
      "zkhost" -> zkHost
    )
    logger.info(s"writeOptions: ${writeOptions}")
    val result = dfWithTimestamp.write.format("solr")
      .options(writeOptions)
      .mode("overwrite")
      .save()
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
