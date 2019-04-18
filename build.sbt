name := "SparkAffinityPropagation"

version := "0.1"

scalaVersion := "2.11.0"

val sparkVersion = "2.1.0"

//libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.5"
//libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion
)
