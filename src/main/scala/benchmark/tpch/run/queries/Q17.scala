package benchmark.tpch.run.queries

import benchmark.BenchmarkQuery
import benchmark.tpch.run.schemaProvider.TpchSchemaProvider
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, sum, udf}

class Q17 extends BenchmarkQuery {

  override def execute(sc: SparkContext, schemaProvider: benchmark.SchemaProvider): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val tpc = schemaProvider.asInstanceOf[TpchSchemaProvider]
    import sqlContext.implicits._
    import tpc._

    val mul02 = udf { (x: Double) => x * 0.2 }

    val flineitem = lineitem.select($"l_partkey", $"l_quantity", $"l_extendedprice")

    val fpart = part.filter($"p_brand" === "Brand#23" && $"p_container" === "MED BOX")
      .select($"p_partkey")
      .join(lineitem, $"p_partkey" === lineitem("l_partkey"), "left_outer")
    // select

    fpart.groupBy("p_partkey")
      .agg(mul02(avg($"l_quantity")).as("avg_quantity"))
      .select($"p_partkey".as("key"), $"avg_quantity")
      .join(fpart, $"key" === fpart("p_partkey"))
      .filter($"l_quantity" < $"avg_quantity")
      .agg(sum($"l_extendedprice") / 7.0)
  }

}
