package com.oconeco.sparktest

//import spark.implicits._

import com.johnsnowlabs.nlp.annotator.{SentenceDetector, Tokenizer}
import com.johnsnowlabs.nlp.annotators.keyword.yake.YakeKeywordExtraction
import com.johnsnowlabs.nlp.base.DocumentAssembler
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.SparkSession


object PostgresExample {
  def main(args: Array[String]): Unit = {
    println("Starting...")

    val spark = SparkSession
      .builder
      .master("local[6]")
      .appName("YakeKeywordExtraction")
      .getOrCreate()
    import spark.implicits._


    val jdbcUrl = "jdbc:postgresql://dell/cm_dev?user=corpusminder&password=pass1234"
    val dfContent = spark.read.format("jdbc")
      .option("url", jdbcUrl)
//      .option("query", "select id, title, uri, structured_content from content where structure_size > 1000 and structure_size < 10000 limit 10")
      .option("query", "select id, title, uri, body_text from content where structure_size > 100 and structure_size < 100000 limit 10")
      .load()

    dfContent.show(10, 120, true)

    val documentAssembler = new DocumentAssembler()
      .setInputCol("body_text")
      .setOutputCol("document")

    val sentenceDetector = new SentenceDetector()
      .setInputCols("document")
      .setOutputCol("sentence")

    val token = new Tokenizer()
      .setInputCols("sentence")
      .setOutputCol("token")
      .setContextChars(Array("(", ")", "?", "!", ".", ","))

    val keywords = new YakeKeywordExtraction()
      .setInputCols("token")
      .setOutputCol("keywords")
      .setThreshold(0.6f)
      .setMinNGrams(2)
      .setNKeywords(10)

    val pipeline = new Pipeline().setStages(Array(
      documentAssembler,
      sentenceDetector,
      token,
      keywords
    ))

/*    val data = Seq(
      """Sources tell us that Google is buying Kaggle, a platform that hosts data science and machine learning competitions.
        |Details about the transaction remain somewhat vague, but given that Google is hosting its Cloud Next conference in San Francisco this week,
        |the official announcement could come as early as tomorrow. Reached by phone, Kaggle co-founder CEO Anthony Goldbloom declined to deny that the acquisition is happening.
        |Google itself declined 'to comment on rumors'. Kaggle, which has about half a million data scientists on its platform, was founded by Goldbloom  and Ben Hamner in 2010.
        |The service got an early start and even though it has a few competitors like DrivenData, TopCoder and HackerRank,
        |it has managed to stay well ahead of them by focusing on its specific niche. The service is basically the de facto home for running data science and machine learning competitions.
        |With Kaggle, Google is buying one of the largest and most active communities for data scientists - and with that, it will get increased mindshare in this community,
        |too (though it already has plenty of that thanks to Tensorflow and other projects). Kaggle has a bit of a history with Google, too,
        |but that's pretty recent. Earlier this month, Google and Kaggle teamed up to host a $100,000 machine learning competition around classifying YouTube videos.
        |That competition had some deep integrations with the Google Cloud Platform, too. Our understanding is that Google will keep the service running -
        |likely under its current name. While the acquisition is probably more about Kaggle's community than technology,
        |Kaggle did build some interesting tools for hosting its competition and 'kernels', too.
        |On Kaggle, kernels are basically the source code for analyzing data sets and developers can share this code on the platform
        |(the company previously called them 'scripts'). Like similar competition-centric sites,
        |Kaggle also runs a job board, too. It's unclear what Google will do with that part of the service.
        |According to Crunchbase, Kaggle raised $12.5 million (though PitchBook says it's $12.75) since its   launch in 2010.
        |Investors in Kaggle include Index Ventures, SV Angel, Max Levchin, Naval Ravikant,
        |Google chief economist Hal Varian, Khosla Ventures and Yuri Milner""".stripMargin
    ).toDF("text")*/
//    val result = pipeline.fit(data).transform(data)
    val dfTransformed = pipeline.fit(dfContent).transform(dfContent)

    // combine the result and score (contained in keywords.metadata)
    val scores = dfTransformed.selectExpr("explode(arrays_zip(keywords.result, keywords.metadata)) as resultTuples")
    //      .select($"resultTuples.0" as "keyword", $"resultTuples.1.score")

    scores.show(false)

    // Order ascending, as lower scores means higher importance
    //    scores.orderBy("score").show(5, truncate = false)

  }

}
