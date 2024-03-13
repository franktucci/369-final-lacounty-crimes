version in ThisBuild := "0.1.0-SNAPSHOT"

scalaVersion in ThisBuild := "2.11.8"
val sparkVersion = "2.4.8"
lazy val root = (project in file("."))
  .settings(
    name := "Project",
    resolvers ++= Seq(
      "apache-snapshots" at "https://repository.apache.org/snapshots/"
    ),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.spark" %% "spark-mllib" % sparkVersion,
      "org.apache.spark" %% "spark-streaming" % sparkVersion,
      "org.apache.spark" %% "spark-hive" % sparkVersion
    )
    ,libraryDependencies += "org.scalafx" %% "scalafx" % "8.0.144-R12"
  )
