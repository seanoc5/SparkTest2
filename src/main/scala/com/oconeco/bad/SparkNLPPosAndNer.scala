package com.oconeco.bad

// code/deployment status: broken
//# A fatal error has been detected by the Java Runtime Environment:
//#  SIGSEGV (0xb) at pc=0x00007f36171b19c8, pid=250691, tid=0x00007f3633dd6640

import com.johnsnowlabs.nlp.annotator._
import com.johnsnowlabs.nlp.base._
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.SparkSession

object SparkNLPPosAndNer {
  def main(args: Array[String]): Unit = {
    println("Getting started...")

    // Create a Spark session
    val spark = SparkSession.builder()
      .appName("SparkNLPPosAndNer")
      .master("local[*]") // Use local for testing, set your master for production
      .getOrCreate()

    import spark.implicits._
    // Sample paragraph of text
    val sampleText = Seq(
      (1, "John Snow, a leading figure in the development of modern epidemiology, was born in York, England."),
      (2, """Cursors are used by default to pull documents out of Solr. By default, the number of tasks allocated will be the number of shards available for the collection.
            |If your Spark cluster has more available executor slots than the number of shards, then you can increase parallelism when reading from Solr by splitting each shard into sub ranges using a split field. A good candidate for the split field is the version field that is attached to every document by the shard leader during indexing. See splits section to enable and configure intra shard splitting.
            |Cursors won’t work if the index changes during the query time. Constrain your query to a static index by using additional Solr parameters using solr.params.""".stripMargin)
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

//    val posTagger = PerceptronModel.pretrained() // Pretrained POS model
//      .setInputCols("sentence", "token")
//      .setOutputCol("pos")

    val nerTagger = NerDLModel.pretrained() // Pretrained NER model
//      .setInputCols("sentence", "token", "pos")
      .setInputCols("document", "token", "word_embeddings")
      .setOutputCol("ner")


    val converter = new NerConverter()
      .setInputCols("sentence", "token", "ner")
      .setOutputCol("ner_chunk")

    val pipeline = new Pipeline().setStages(Array(
      documentAssembler,
      sentenceDetector,
      tokenizer,
      wordEmbeddings,
//      posTagger,
      nerTagger,
      converter
    ))

    println("Starting fit...")
    // Fit the pipeline to the sample text
    val model = pipeline.fit(sampleText)

    println("Starting transform...")
        // Transform the sample text
    val results = model.transform(sampleText)


    println("Starting selectt...")
    // Show the results
    results.select("id", "pos.result", "ner_chunk.result").show(false)

    println("Starting show...")
    results.show(false)

    println("Doine")

    spark.stop()
  }
}
