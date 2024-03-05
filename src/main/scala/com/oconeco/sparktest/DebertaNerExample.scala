package com.oconeco.sparktest
// code/deployment status: WORKING

import com.johnsnowlabs.nlp.annotator.{DeBertaForTokenClassification, NerConverter, SentenceDetector, Tokenizer}
//import com.johnsnowlabs.nlp.annotator.{, Tokenizer}
import com.johnsnowlabs.nlp.base.{Finisher}
import com.johnsnowlabs.nlp.base.DocumentAssembler
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.SparkSession
import com.johnsnowlabs.nlp.annotators.ner.NerConverter

object DebertaNerExample {
  def main(args: Array[String]): Unit = {
    println(s"Starting ${this.getClass.getSimpleName}...")

    val spark = SparkSession.builder()
      .appName("DebertaNERExample")
      .master("local[*]") // Use local for testing, set your master for production
      .getOrCreate()

    val  pipeline: Pipeline = buildNERPipeline

    import spark.implicits._
    //    val example = Seq("I really liked that movie!").toDF("text")
    val example = Seq(
      (1, "John Snow, a leading figure in the development of modern epidemiology, was born in York, England."),
      (2,
        """Cursors are used by default to pull documents out of Solr. By default, the number of tasks allocated will be the number of shards available for the collection.
          |If your Spark cluster has more available executor slots than the number of shards, then you can increase parallelism when reading from Solr by splitting each shard into sub ranges using a split field. A good candidate for the split field is the version field that is attached to every document by the shard leader during indexing. See splits section to enable and configure intra shard splitting.
          |Cursors won’t work if the index changes during the query time. Constrain your query to a static index by using additional Solr parameters using solr.params.""".stripMargin)
    ).toDF("id", "text")

    val result = pipeline.fit(example).transform(example)
//    result.select("sentence").show(5, false)
    result.selectExpr("explode(sentence)").show(false)

//    list(zip(light_result('token'), light_result('ner') ) )

    // since output column is IOB/IOB2 style, NerConverter can extract entities
    val ner_converter = new NerConverter()
      .setInputCols("sentence", "token", "ner")
      .setOutputCol("entities")
      .setPreservePosition(false)

    ner_converter.transform(result).selectExpr("explode(entities)").show(false)

    println("Done...?")

  }

  private def buildNERPipeline: Pipeline = {
    val document_assembler = new DocumentAssembler()
      .setInputCol("text")
      .setOutputCol("document")


    val sentenceDetector = new SentenceDetector()
      .setInputCols("document")
      .setOutputCol("sentence")


    val tokenizer = new Tokenizer()
      .setInputCols("sentence")
      //      .setInputCols("document")
      .setOutputCol("token")

    val tokenClassifier = DeBertaForTokenClassification.pretrained("deberta_v3_base_token_classifier_ontonotes", "en")
      //      .setInputCols("document", "token")
      .setInputCols("sentence", "token")
      .setOutputCol("ner")
      .setCaseSensitive(true)
      .setMaxSentenceLength(512)


    //    val pipeline = new Pipeline().setStages(Array(document_assembler, tokenizer, tokenClassifier, ner_converter))
    val pipeline = new Pipeline().setStages(Array(
      document_assembler,
      sentenceDetector,
      tokenizer,
      tokenClassifier
    ))
    return pipeline
  }
}
