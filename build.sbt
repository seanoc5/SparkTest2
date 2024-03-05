ThisBuild / version := "0.1.0"

ThisBuild / scalaVersion := "2.12.10"

lazy val root = (project in file("."))
  .settings(
    name := "SparkTest2",
//        mainClass in Compile := Some ("com.oconeco.sparktest.SparkPi"),
//        mainClass in assembly := Some ("com.oconeco.sparktest.SparkPi"),
  )


val sparkVersion = "3.5.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,

  "com.johnsnowlabs.nlp" %% "spark-nlp" % "5.2.3", // Use the latest version

  "org.postgresql" % "postgresql" % "42.7.1"

)

assemblyJarName in assembly := "oconeco-scala-sbt-assembly-fatjar-0.1.0.jar"

// META-INF discarding
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}


/*
lazy val root = (project in file(".")).settings(
  name := "scala-sbt",
  version := "1.0",
  mainClass in Compile := Some("com.baeldung.scala.sbt.SbtAssemblyExample"),
  mainClass in assembly := Some("com.baeldung.scala.sbt.SbtAssemblyExample")
)

val sparkVersion = "2.4.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion

assemblyJarName in assembly := "baeldung-scala-sbt-assembly-fatjar-1.0.jar"

// META-INF discarding
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x                             => MergeStrategy.first
}
*/
