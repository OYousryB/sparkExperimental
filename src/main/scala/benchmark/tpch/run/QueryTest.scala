package benchmark.tpch.run

import java.io.FileInputStream

import benchmark.tpch.run.schemaProvider.VanillaTpchSchemaProvider
import benchmark.{BenchmarkQuery, SparkCtx}

object QueryTest {

  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      println("Missing args")
      return
    }

    val inputDir = args(0)
    val outputDir = inputDir + "/output"

    val schemaProvider = new VanillaTpchSchemaProvider(inputDir)
    val engine = "spark"

    val queryIndices =
      if (args.length > 2) Seq(args(2).toInt)
      else 1 to 22

    val source = new FileInputStream()("src/main/scala/benchmark/tpch/run/queries/tpch_sql_queries_raw.txt")
    val buf = new Array[Byte](source.available())
    source.read(buf)
    val queryText = new String(buf).split('\n')

    val queries = queryIndices.map(queryNo => (SparkCtx.sql(queryText(queryNo - 1)), s"Q$queryNo"))


//    val queryIndices =
//      if (args.length > 2)
//        Seq(args(2).toInt)
//      else
//        1 to 22
//
//    val queries = queryIndices
//      .map(queryNo =>
//        (
//          Class.forName(f"benchmark.tpch.run.queries.Q$queryNo%02d")
//            .newInstance.asInstanceOf[BenchmarkQuery]
//            .execute(SparkCtx.sc, schemaProvider),
//          s"Q$queryNo"
//        )
//      )
    BenchmarkQuery.process("tpch", engine, queries, outputDir)
  }
}
