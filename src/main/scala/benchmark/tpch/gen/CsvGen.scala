package benchmark.tpch.gen

import benchmark.{SparkCtx, SchemaProvider}
import benchmark.tpch.run.schemaProvider.TpchSchemaProvider
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{avg, count, sum}

object CsvGen {

  def process(provider: SchemaProvider, outputDir: String ): Unit = {

    val tpc = provider.asInstanceOf[TpchSchemaProvider]
    val sqlContext = new org.apache.spark.sql.SQLContext(SparkCtx.sc)
    import tpc._
    import sqlContext.implicits._

    val t0 = System.nanoTime()

    lineitem.write.format("csv").mode(SaveMode.Overwrite).save(outputDir + "/" + "lineitem")

    val t1 = System.nanoTime()

    lineitem
      .filter($"l_shipdate" <= "1998-09-02")
      .groupBy($"l_returnflag", $"l_linestatus")
      .agg(sum($"l_quantity"), sum($"l_extendedprice"), avg($"l_quantity"), avg($"l_extendedprice"), avg($"l_discount"), count($"l_quantity"))
      .sort($"l_returnflag", $"l_linestatus")
      .write.format("csv").mode(SaveMode.Overwrite).save(outputDir + "/" + "lineitem")

    val t2 = System.nanoTime()

    lineitem
      .select("l_orderkey", "l_linenumber")
      .write.format("csv").mode(SaveMode.Overwrite).save(outputDir + "/" + "lineitem")

    val t3 = System.nanoTime()

    dfMap.foreach {
      case (key, value) =>
        value.write.format("csv").mode(SaveMode.Overwrite).save(outputDir + "/" + key)
    }

    val t4 = System.nanoTime()

    println(s"Conversion_to_Csv_lineitem_is: ${(t1 - t0) / 1e9} secs")
    println(s"Conversion_to_Csv_lineitem_with_filter_is: ${(t2 - t1) / 1e9} secs")
    println(s"Conversion_to_Csv_lineitem_two_col_selected_is: ${(t3 - t2) / 1e9} secs")
    println(s"Conversion_to_Csv_full_dataset_is: ${(t4 - t3) / 1e9} secs")
  }
}
