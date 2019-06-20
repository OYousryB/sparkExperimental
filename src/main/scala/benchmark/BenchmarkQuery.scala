package benchmark

import java.sql.{Connection, DriverManager, SQLException}

import org.apache.spark.sql._
import org.apache.spark.SparkContext

import scala.collection.mutable.ListBuffer

abstract class BenchmarkQuery {

  // get the name of the class excluding dollar signs and package
  private def escapeClassName(className: String): String = {
    className.split("\\.").last.replaceAll("\\$", "")
  }

  def getName(): String = escapeClassName(this.getClass.getName)

  /**
    *  implemented in children classes and hold the actual query
    */
  def execute(sc: SparkContext, schemaProvider: SchemaProvider): DataFrame
}

object BenchmarkQuery {

  private val BENCHMARK_DATABASE_NAME = "benchmark"

  def outputDF(df: DataFrame, outputDir: String, className: String): Unit = {
    def replaceAll(in: String, chars: Seq[String], replacement: String) = {
      var s = in
      for (c <- chars) {
        s = s.replace(c, replacement)
      }
      s
    }

    var d = df
    for (c <- df.columns) {
      d = d.withColumnRenamed(c, replaceAll(c, Seq(" ", ",", ";", "{", "}", "(", ")", "\n", "\t", "=", "."), "_"))
    }
    d.write.mode("overwrite").format("parquet").save(outputDir + "/" + className)
  }

  def executeQueries(benchmark: String, queries: Seq[Tuple2[Dataset[Row], String]], outputDir: String): ListBuffer[(String, Double)] = {

    val results = new ListBuffer[(String, Double)]

    for (query <- queries) {

      println("\n" + "*"*80)
      println("*"*20 + s"   ${query._2}")
      println("*"*80 + "\n")

      val t0 = System.nanoTime()
      outputDF(query._1, outputDir, query._2)
      val t1 = System.nanoTime()

      val elapsed = (t1 - t0) / 1000000000.0 // second
      results += Tuple2(benchmark.toUpperCase + "_" + query._2, elapsed)
    }

    results
  }

  def process(benchmark: String, engine: String, queries: Seq[Tuple2[Dataset[Row], String]], outputDir: String): Unit = {
    val times = executeQueries(benchmark, queries, outputDir)

    times.foreach(println)

    Class.forName("com.mysql.cj.jdbc.Driver")
    var connection: Connection = null
    try
      connection = DriverManager.getConnection("jdbc:mysql://localhost?user=root&password=root")

    catch {
      case e: SQLException =>
        println("Skipping insert results into database")
        return
    }

    val statement = connection.createStatement

    statement.execute(s"CREATE DATABASE IF NOT EXISTS $BENCHMARK_DATABASE_NAME")
    statement.execute(s"CREATE TABLE IF NOT EXISTS $BENCHMARK_DATABASE_NAME.$benchmark (" +
      s"commitHash CHAR(40) PRIMARY KEY," +
      s"timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
      s")")
    statement.execute(s"CREATE TABLE IF NOT EXISTS $BENCHMARK_DATABASE_NAME.${benchmark}_times (" +
      s"commitHash CHAR(40)," +
      s"engine CHAR(20)," +
      s"query INT," +
      s"time DOUBLE," +
      s"PRIMARY KEY (commitHash, engine, query)" +
      s")")

    val p = Runtime.getRuntime.exec("git rev-parse HEAD")
    while (p.isAlive) { }
    if (p.exitValue() == 0) {
      val hash = new Array[Byte](40)
      p.getInputStream.read(hash)
      val commitHash = new String(hash)

      statement.execute(s"INSERT IGNORE INTO $BENCHMARK_DATABASE_NAME.$benchmark (commitHash) VALUES ('$commitHash')")

      times.foreach(r => statement.execute(s"INSERT INTO $BENCHMARK_DATABASE_NAME.${benchmark}_times (commitHash, engine, query, time) VALUES (" +
        s"'$commitHash', " +
        s"'$engine', " +
        s"${r._1.replaceFirst(benchmark.toUpperCase + "_Q", "")}," +
        s"${r._2}" +
        s") ON DUPLICATE KEY UPDATE time=${r._2}")
      )
    }
  }
}
