package benchmark.tpch.run.schemaProvider

import benchmark.SchemaProvider
import org.apache.spark.sql.DataFrame

abstract class TpchSchemaProvider extends SchemaProvider{

  val dfMap = Map[String, DataFrame]()

  // for implicits
  def customer = dfMap("customer")
  def lineitem = dfMap("lineitem")
  def nation = dfMap("nation")
  def region = dfMap("region")
  def order = dfMap("order")
  def part = dfMap("part")
  def partsupp = dfMap("partsupp")
  def supplier = dfMap("supplier")
}
