package wfscala

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext

/**
 * Created by root on 14-5-3.
 */
object StudyExpression {
  def main(args: Array[String]) {
//        val sc = new SparkContext("local", "StudyExpression")
//        val sqlContext = new SQLContext(sc)
//        import sqlContext._

    val s = 'aSymbol
    //输出true
    println( s == 'aSymbol)
    //输出true
    println( s == Symbol("aSymbol"))
   // import org.apache.spark.sql.catalyst.dsl._
//    val a = dsl.ExpressionConversions.DslSymbol('key)
    val eprs = ('key === 1)
    println(eprs)
  }
}
