name := "recomovies"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.4.1" % "provided"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.4.1" % "provided"

assemblyJarName in assembly := "recomive.jar"

mainClass in assembly := Some("my.cf.ItemCF")
