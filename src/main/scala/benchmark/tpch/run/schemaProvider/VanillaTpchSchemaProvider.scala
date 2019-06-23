package benchmark.tpch.run.schemaProvider

import benchmark.SparkCtx


class VanillaTpchSchemaProvider(inputDir: String) extends TpchSchemaProvider {

  override val dfMap = Map(
    "customer" -> SparkCtx.spark.read.parquet(inputDir + "/customer"),

    "lineitem" -> SparkCtx.spark.read.parquet(inputDir + "/lineitem"),

    "nation" -> SparkCtx.spark.read.parquet(inputDir + "/nation"),

    "region" -> SparkCtx.spark.read.parquet(inputDir + "/region"),

    "order" -> SparkCtx.spark.read.parquet(inputDir + "/order"),

    "part" -> SparkCtx.spark.read.parquet(inputDir + "/part"),

    "partsupp" -> SparkCtx.spark.read.parquet(inputDir + "/partsupp"),

    "supplier" -> SparkCtx.spark.read.parquet(inputDir + "/supplier")
  )

//  dfMap.foreach(x => x._2.createOrReplaceTempView(x._1))
}
