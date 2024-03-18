# Overview
This is a very simple repo as I am learning Spark (scala). I want to do some entry level NLP. For various reasons I would like to use John Snow Labs Spark-NLP.  
Assume nothing here is the Right Way(tm) to do things. Just me learning, trial and error.

I have struggled to get Spark setup.  
I have a laptop (Lenovo Yoga 16Gb) for development, and an old Dell workstation.  Both are running linux mint 21.2 (Victoria)  
At the moment I can run some basic `scala`, `groovy`, and `python` scripts localhost (`master=local[*]`) on the laptop.

I hit various (self-inflicted?) problems when I try to submit jobs to a cluster I have on the Dell workstation/server. It is 80% likely I have mismatched versions.

## Working Goal(s)
At the moment, my interest is improving my devops and smarter approaches for workflow.

Specifically:
* better practices for how to develop on a 16Gb laptop (sampling, smaller models, etc)
* better practices for submitting a job from the laptop to the dell server/workstation
* figure out how to add `sbt aseembly` when needed before the `spark-submit` (in Intellij)
  

## Progress / Current status
* 2024-02-28:
  * I have worked out various incompatible versioning issues. 
  * I have sbt-assembly added to the sbt setup, and worked out dependency resolution issues 
  * I can create a fat jar via `sbt assebly` 
  * and then `spark-submit` it with `--master spark://dell:7077` param, 
    * but only `client` mode (`cluster` gives me a vague error)

## Todo (next phase)
* get better with sbt: switch from old syntax to slash [syntax](https://www.scala-sbt.org/1.x/docs/Migrating-from-sbt-013x.html#slash) 
* reconfigure Dell server/workstation, currently _(ssh'ed into the dell server)_:
  * `start-master.sh` 
    * no args, no config/tuning
    * **Note:** all three start- commands are run from `$SPARK_HOME/sbin/`
  * `start-worker.sh spark://dell:7077 -c 40 -m 90G`
    * dell is an _/etc/hosts_ entry pointing to ethernet card the eth
    * I **think** this means one big-far worker, where I should probably do 20 workers with a couple of cores and Gbs each...??
  * `start-history-server.sh` 
  * improve logging setup, far too verbose with framework msgs
* learn spark/scala
  * how to get the actual NER entities to display nicely instead of some hackish `.show()` that I currently have
  * read content from postgresql db (same host)
  * save NER results back to both Postgres and to a "sister" solr cluster that is mirroring some content (for better search)
  * learn the Spark admin UI(s) better
  * explore the history server
* Intellij 
  * remote development

## Various Details
### Hardware
* Dell T7810 _“Chia Farming”_ Workstation/Server, 
  * 2X Intel Xeon E5-2690 v4 up to 3.5GHz (28 Cores & 56 Threads Total), 
  * 128GB DDR4 _(upgradable to 256 when/if needed)_
  * old HDDs for OS and work (currently) ~4TB 
  * 1TB SDD on a PCIe adapter, but not yet recognized or used
  * Quadro K620 2GB Graphics Card
* Lenovo Yoga Yoga 9-15IMH5 Laptop (Lenovo) 
  * 16 Gb
  * SSD


## Notes
Caution with Large Datasets

For larger datasets that cannot fit into memory, consider using distributed operations like foreachPartition to process each partition of the DataFrame independently without collecting the data to a single machine:

scala

explodedResults.select("normalized_token").foreachPartition { partitionIterator =>
  partitionIterator.foreach { row =>
    val token = row.getString(0) // Process each token
    // Perform your processing logic here
    println(token)
  }
}

This approach leverages Spark's distributed computing capabilities, processing each partition of the DataFrame across the cluster without requiring the data to be collected to a single machine. It's a more scalable way to process the results for large datasets.

Remember, when working with Spark DataFrames, you're operating in a distributed environment, so it's best to use Spark's native transformations and actions to process your data efficiently across the cluster.


## timings
### DocumentsAnalysis 
- linyoga 
  - (300 content docs)
    - 10 partitions
      - 2024-03-16: 
        - 43 seconds - 10 partitions local[*] yoga
        - 34 seconds - 10 partitions local[*] yoga
        - 34 seconds - 10 partitions local[*] yoga (21:26:56)
    - 5 partitions
      - 2024-03-16: 
        - 33.2 
        - 33.350 (2024-03-16 21:34:05,003)
        - 34.22  (2024-03-16 21:35:21)
    - 1 partition
      - 42.486 (2024-03-16 21:37:15)
      - 41.05 (2024-03-16 21:39:15)
    - 30 partitions
      - 34.616 (2024-03-16 21:40:53)
      - 32.86 (2024-03-16 21:41:51)
  - 3000 docs
    - 5 partitions
      - 330.33 (2024-03-16 23:33:)
      - 299.11 (2024-03-16 23:40:29


## Snippets

    // Replace "entities" with the actual name of your column containing the NER results
//    val explodedDF = result.withColumn("nerentity", explode($"ner"))

    // Now, you can select the specific fields of interest from the "entity" column
//    val entitiesDF = explodedDF.select(
//      $"entity.result".as("entity_text"),
//      $"entity.metadata.word".as("entity_word"),
//      $"entity.metadata.sentence".as("entity_sentence"),
//      $"entity.begin".as("start_pos"),
//      $"entity.end".as("end_pos")
//    )
//
//    entitiesDF.show(80, 120)

//    result.select("ner.result").show(false)

//  result.select(explode(arrays_zip('ner_chunk.result', 'ner_chunk.metadata')).alias("cols")) \
//  .select(F.expr("cols['0']").alias("chunk"),
//        F.expr("cols['1']['entity']").alias("ner_label")).show(truncate=False)

