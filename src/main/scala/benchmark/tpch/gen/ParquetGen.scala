package benchmark.tpch.gen

import benchmark.SparkCtx
import benchmark.tpch.schema._
import org.apache.spark.sql.SaveMode

object ParquetGen {
  def main(args: Array[String]): Unit = {
    if(args.length < 2) {
      println("Missing args!")
      return
    }

    val inputDir = args(0)
    val outputDir = args(1)
    var partitions = 0
    var useSparkPartitions = true
    if (args.length > 2) {
      partitions = args(2).toInt
      useSparkPartitions = false
    }
    val separator = '|'

    def double(v: String) = v.trim.toDouble
    def long(v: String) = v.trim.toLong
    def str(v: String) = v.trim

    val sc = SparkCtx.sc
    val sqlContext = SparkCtx.sqlContext
    import sqlContext.implicits._

    val dfMap = Map(
      "customer" -> sc.textFile(inputDir + "/customer/*").map(_.split(separator)).map(p =>
        Customer(long(p(0)), str(p(1)), str(p(2)), long(p(3)), str(p(4)), double(p(5)), str(p(6)), str(p(7)))).toDF(),

      "lineitem" -> sc.textFile(inputDir + "/lineitem/*").map(_.split(separator)).map(p =>
        Lineitem(long(p(0)), long(p(1)), long(p(2)), long(p(3)), double(p(4)), double(p(5)), double(p(6)), double(p(7)), str(p(8)), str(p(9)), str(p(10)), str(p(11)), str(p(12)), str(p(13)), str(p(14)), str(p(15)))).toDF(),

      "nation" -> sc.textFile(inputDir + "/nation/*").map(_.split(separator)).map(p =>
        Nation(long(p(0)), str(p(1)), long(p(2)), str(p(3)))).toDF(),

      "region" -> sc.textFile(inputDir + "/region/*").map(_.split(separator)).map(p =>
        Region(long(p(0)), str(p(1)), str(p(2)))).toDF(),

      "order" -> sc.textFile(inputDir + "/orders/*").map(_.split(separator)).map(p =>
        Order(long(p(0)), long(p(1)), str(p(2)), double(p(3)), str(p(4)), str(p(5)), str(p(6)), long(p(7)), str(p(8)))).toDF(),

      "part" -> sc.textFile(inputDir + "/part/*").map(_.split(separator)).map(p =>
        Part(long(p(0)), str(p(1)), str(p(2)), str(p(3)), str(p(4)), long(p(5)), str(p(6)), double(p(7)), str(p(8)))).toDF(),

      "partsupp" -> sc.textFile(inputDir + "/partsupp/*").map(_.split(separator)).map(p =>
        Partsupp(long(p(0)), long(p(1)), long(p(2)), double(p(3)), str(p(4)))).toDF(),

      "supplier" -> sc.textFile(inputDir + "/supplier/*").map(_.split(separator)).map(p =>
        Supplier(long(p(0)), str(p(1)), str(p(2)), long(p(3)), str(p(4)), double(p(5)), str(p(6)))).toDF()
    )

    if (useSparkPartitions){
      dfMap.foreach {
        case (key, value) =>
          value.write.format("parquet").option("compression", "none").mode(SaveMode.Overwrite).save(outputDir + "/" + key)
      }
    } else {
      dfMap.foreach {
        case (key, value) =>
          value.repartition(partitions).write.format("parquet").option("compression", "none").mode(SaveMode.Overwrite).save(outputDir + "/" + key)
      }
    }

  }
}