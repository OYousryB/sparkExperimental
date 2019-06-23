package benchmark.tpch.run.queries

import benchmark.BenchmarkQuery
import benchmark.tpch.run.schemaProvider.TpchSchemaProvider
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{sum, udf}

class Q05 extends BenchmarkQuery {

  override def execute(sc: SparkContext, schemaProvider: benchmark.SchemaProvider): DataFrame = {

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val tpc = schemaProvider.asInstanceOf[TpchSchemaProvider]

    import sqlContext.implicits._
    import tpc._

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

    lineitem
        .join(supplier, $"l_suppkey" === supplier("s_suppkey"))
        .join(order, $"l_orderkey" === order("o_orderkey"))
        .join(customer, $"o_custkey" === customer("c_custkey"))
        .join(nation, $"s_nationkey" === nation("n_nationkey") && $"c_nationkey" === nation("n_nationkey"))
        .join(region, $"n_regionkey" === region("r_regionkey"))
        .filter($"r_name" === "ASIA")
        .filter($"o_orderdate" < "1995-01-01" && $"o_orderdate" >= "1994-01-01")
        .select($"n_name", decrease($"l_extendedprice", $"l_discount").as("value"))
        .groupBy($"n_name")
        .agg(sum($"value").as("revenue"))
        .sort($"revenue".desc)
  }
}