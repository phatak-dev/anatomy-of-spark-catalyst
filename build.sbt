name := "AnatomyOfSparkSQL"

version := "1.0"

scalaVersion := "2.10.4"

val sparkVersion = "1.6.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion
)
    