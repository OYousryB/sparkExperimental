
name := "sparkExperimental"

version := "0.1"

scalaVersion := "2.11.8"


libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"

libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.15"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
