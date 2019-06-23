package benchmark.tpch.run.queries

import benchmark.BenchmarkQuery
import benchmark.tpch.run.schemaProvider.TpchSchemaProvider
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.sum

class Q06 extends BenchmarkQuery {

  override def execute(sc: SparkContext, schemaProvider: benchmark.SchemaProvider): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val tpc = schemaProvider.asInstanceOf[TpchSchemaProvider]
    import sqlContext.implicits._
    import tpc._

    lineitem.filter($"l_shipdate" >= "1994-01-01" && $"l_shipdate" < "1995-01-01" && $"l_discount" >= 0.05 && $"l_discount" <= 0.07 && $"l_quantity" < 24)
      .agg(sum($"l_extendedprice" * $"l_discount"))
  }

}
