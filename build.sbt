resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

name := "json-reader-guseynov"

version := "0.0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.scala-lang" % "scala-xml" % "2.11.0-M4"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4" % "provided"

libraryDependencies += "mrpowers" % "spark-daria" % "0.35.2-s_2.11"

libraryDependencies += "MrPowers" % "spark-fast-tests" % "0.20.0-s_2.11" % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)