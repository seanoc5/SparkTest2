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
  * `start-worker.sh spark://dell:7077 -c 40 -m 90G`
    * dell is an _/etc/hosts_ entry pointing to ethernet card the eth
    * I **think** this means one big-far worker, where I should probably do 20 workers with a couple of cores and Gbs each...??
* learn spark/scala
  * how to get the actual NER entities to display nicely instead of some hackish `.show()` that I currently have
  * read content from postgresql db (same host)
  * save NER results back to both Postgres and to a "sister" solr cluster that is mirroring some content (for better search)

## Various Details
FWIW: Dell T7810 “Chia Farming” Workstation/Server, 2X Intel Xeon E5-2690 v4 up to 3.5GHz (28 Cores & 56 Threads Total), 128GB DDR4, Quadro K620 2GB Graphics Card,
