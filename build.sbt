name := "FinalProject"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.4.8"
val vegasVersion = "0.3.11"

resolvers ++= Seq(
    "apache-snapshots" at "https://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.spark" %% "spark-mllib" % sparkVersion,
    "org.apache.spark" %% "spark-streaming" % sparkVersion,
    "org.apache.spark" %% "spark-hive" % sparkVersion,
    "org.vegas-viz" %% "vegas" % vegasVersion,
    "org.vegas-viz" %% "vegas-spark" % vegasVersion,
)
