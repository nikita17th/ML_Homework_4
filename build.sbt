name := "ML_Homework_4"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.11.12"


javacOptions ++= Seq("-source", "1.8", "-target", "1.8")
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.8",
  "org.apache.spark" %% "spark-sql" % "2.4.8"
)