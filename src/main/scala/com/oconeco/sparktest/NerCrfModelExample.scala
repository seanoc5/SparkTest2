package com.oconeco.sparktest
// code/deployment status: WORKING
//https://sparknlp.org/api/com/johnsnowlabs/nlp/annotators/ner/crf/NerCrfModel.html

import com.johnsnowlabs.nlp.annotator.NerConverter
import com.johnsnowlabs.nlp.base.DocumentAssembler
import com.johnsnowlabs.nlp.annotators.Tokenizer
import com.johnsnowlabs.nlp.annotators.keyword.yake.YakeKeywordExtraction
import com.johnsnowlabs.nlp.annotators.sbd.pragmatic.SentenceDetector
import com.johnsnowlabs.nlp.embeddings.WordEmbeddingsModel
import com.johnsnowlabs.nlp.annotators.pos.perceptron.PerceptronModel
import com.johnsnowlabs.nlp.annotators.ner.crf.NerCrfModel
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{concat_ws, explode, expr}

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

    val data = Seq("John Smith, is a fictional name and was born in Albany, New York. Spark is a tool for big companies like Google, Amazon, and Mr. Kevin Butler does not like it.",
      "Sean is learning Scala."). toDF("text")
    val resultDf = pipeline.fit(data).transform(data)

    resultDf.printSchema()
    println(s"Number of partitions: ${resultDf.rdd.getNumPartitions}")

//    val repartitionDf = resultDf.repartition(20)

//    def foo = result.selectExpr("explode(ner_chunk) as exploded")
    val chunkiesDf = resultDf.selectExpr("explode(ner_chunk) as exploded").select("exploded.*")
    chunkiesDf.show(10,120,true)

    resultDf.selectExpr("explode(ner_chunk) as foo").show(40,80,true)

    // Exploding the array into separate rows (if needed for row-wise operations)
    // val explodedDF = df.withColumn("annotation", explode($"annotations"))

    // Transforming each struct in the array to concatenate 'result' and 'metadata.entity'
//    val transformedDF = resultDf.withColumn("transformed_annotations", expr("TRANSFORM(annotations, x -> CONCAT(x.result, '-', x.metadata['entity']))"))

    // To aggregate the transformed strings back into a single string per original row, if exploded
    // Here, you might need to group by an identifier if you exploded the DataFrame, then collect_list and join
    // val finalDF = transformedDF.groupBy("id").agg(concat_ws(" ", collect_list($"transformed_annotation")).as("concatenated_results"))

    // If you did not explode the DataFrame and used TRANSFORM directly:
//    val finalDF = transformedDF.withColumn("concatenated_results", concat_ws(" ", $"transformed_annotations"))


//  result.select(explode(arrays_zip('ner_chunk.result', 'ner_chunk.metadata')).alias("cols")) \
//  .select(F.expr("cols['0']").alias("chunk"),
//        F.expr("cols['1']['entity']").alias("ner_label")).show(truncate=False)

    resultDf.select("keywords").show(40,80,true)


    println("Done??...")
  }
}
