package benchmark.tpch.run.queries

import benchmark.BenchmarkQuery
import benchmark.tpch.run.schemaProvider.TpchSchemaProvider
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{sum, udf}

class Q12 extends BenchmarkQuery {

  override def execute(sc: SparkContext, schemaProvider: benchmark.SchemaProvider): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val tpc = schemaProvider.asInstanceOf[TpchSchemaProvider]
    import sqlContext.implicits._
    import tpc._

    val mul = udf { (x: Double, y: Double) => x * y }
    val highPriority = udf { (x: String) => if (x == "1-URGENT" || x == "2-HIGH") 1 else 0 }
    val lowPriority = udf { (x: String) => if (x != "1-URGENT" && x != "2-HIGH") 1 else 0 }

    lineitem.filter((
      $"l_shipmode" === "MAIL" || $"l_shipmode" === "SHIP") &&
      $"l_commitdate" < $"l_receiptdate" &&
      $"l_shipdate" < $"l_commitdate" &&
      $"l_receiptdate" >= "1994-01-01" && $"l_receiptdate" < "1995-01-01")
      .join(order, $"l_orderkey" === order("o_orderkey"))
      .select($"l_shipmode", $"o_orderpriority")
      .groupBy($"l_shipmode")
      .agg(sum(highPriority($"o_orderpriority")).as("sum_highorderpriority"),
        sum(lowPriority($"o_orderpriority")).as("sum_loworderpriority"))
      .sort($"l_shipmode")
  }

}
