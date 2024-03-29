package com.oconeco.bad

//code/deployment status: Broken
// works with submit, but not local[...]
//2024-03-03 20:10:30.203607: I external/org_tensorflow/tensorflow/core/platform/cpu_feature_guard.cc:151] This TensorFlow binary is optimized with oneAPI Deep Neural Network Library (oneDNN) to use the following CPU instructions in performance-critical operations:  AVX2 FMA
//To enable them in other operations, rebuild TensorFlow with the appropriate compiler flags.
//# A fatal error has been detected by the Java Runtime Environment:
//#  SIGSEGV (0xb) at pc=0x00007f0a1995f4c9, pid=185469, tid=185470

import com.johnsnowlabs.nlp.annotator._
import com.johnsnowlabs.nlp.annotators.ner.NerConverter
import com.johnsnowlabs.nlp.base._
import com.johnsnowlabs.util.Benchmark
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.SparkSession

object NerDLPipeline extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("NER DL Pipeline test")
    //    .master("spark://dell:7077")
    .master("local[4]")
    //    .config("spark.driver.memory", "4G")
    //    .config("spark.kryoserializer.buffer.max", "200M")
    //    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")

  println("Starting a new version of the test...")

  val document = new DocumentAssembler()
    .setInputCol("text")
    .setOutputCol("document")

  val token = new Tokenizer()
    .setInputCols("document")
    .setOutputCol("token")

  val normalizer = new Normalizer()
    .setInputCols("token")
    .setOutputCol("normal")

  val wordEmbeddings = WordEmbeddingsModel.pretrained()
    .setInputCols("document", "token")
    .setOutputCol("word_embeddings")

  val ner = NerDLModel.pretrained()
    .setInputCols("normal", "document", "word_embeddings")
    .setOutputCol("ner")

  val nerConverter = new NerConverter()
    .setInputCols("document", "normal", "ner")
    .setOutputCol("ner_converter")

  val finisher = new Finisher()
    .setInputCols("ner", "ner_converter")
    .setIncludeMetadata(true)
    .setOutputAsArray(false)
    .setCleanAnnotations(false)
    .setAnnotationSplitSymbol("@")
    .setValueSplitSymbol("#")

  val pipeline = new Pipeline().setStages(Array(document, token, normalizer, wordEmbeddings, ner, nerConverter, finisher))

  val testing = Seq(
    (2, "Peter Parker is a super hero"),
    (3, "Jacksonville Beach is in Florida, along with the Jacksonville Jaguars"),
    (4, "Apple is a famous company"),
  ).toDS.toDF("_id", "text")

  val result = pipeline.fit(Seq.empty[String].toDS.toDF("text")).transform(testing)
  Benchmark.time("Time to convert and show") {
    result.select("ner", "ner_converter").show(truncate = false)
  }

}
