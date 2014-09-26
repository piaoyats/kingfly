package scwf

import org.apache.spark.sql.catalyst.SqlParser
import org.apache.spark.sql.catalyst.analysis.{SimpleCatalog, Catalog, EmptyFunctionRegistry, Analyzer}
import org.apache.spark.sql.catalyst.optimizer.Optimizer

/**
 * Created by root on 14-5-8.
 */
object StudySqlParser {
  def main (args: Array[String]) {
    val parser = new SqlParser
    val sql = "select a,b from t where a = 'wf' "
    val plan1 = parser(sql)
    println(plan1)

    val catalog: Catalog = new SimpleCatalog(false)

    import org.apache.spark.sql.catalyst.plans.logical._
    import org.apache.spark.sql.catalyst.dsl.expressions._
    catalog.registerTable(None,"t",LocalRelation('a.string, 'b.string))
    val analyzer: Analyzer =
      new Analyzer(catalog, EmptyFunctionRegistry, caseSensitive = true)
    val plan2 = analyzer(plan1)
    println(plan2)

    val optimizer = Optimizer
    val plan3 = optimizer(plan2)
    println(plan3)

  }

}