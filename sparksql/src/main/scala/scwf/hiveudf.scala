package scwf

import org.apache.hadoop.io.Writable
import java.io.{DataInput, DataOutput}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject
import org.apache.spark.sql.hive.test.TestHive
import scala.collection.JavaConversions._

/**
 * Created by root on 14-7-5.
 */
object hiveudf {
  def main(args: Array[String]) {

    val sql =       """
                      |CREATE EXTERNAL TABLE hiveUdfTestTable (
                      |   pair STRUCT<id: INT, value: INT>
                      |)
                      |PARTITIONED BY (partition STRING)
                      |STORED AS SEQUENCEFILE
                    """.stripMargin.format("haha")
    println(sql)

    TestHive.hql( sql )

    TestHive.hql("CREATE TEMPORARY FUNCTION testUdf AS '%s'".format(classOf[PairUdf].getName))
    TestHive.hql("select * from hiveUdfTestTable").foreach(println)

    TestHive.hql("SELECT testUdf(pair) FROM hiveUdfTestTable").foreach(println)

  }

  class TestPair(x: Int, y: Int) extends Writable with Serializable {
    def this() = this(0, 0)
    var entry: (Int, Int) = (x, y)

    override def write(output: DataOutput): Unit = {
      output.writeInt(entry._1)
      output.writeInt(entry._2)
    }

    override def readFields(input: DataInput): Unit = {
      val x = input.readInt()
      val y = input.readInt()
      entry = (x, y)
    }
  }

  class PairUdf extends GenericUDF {
    override def initialize(p1: Array[ObjectInspector]): ObjectInspector =
      ObjectInspectorFactory.getStandardStructObjectInspector(
        Seq("id", "value"),
        Seq(PrimitiveObjectInspectorFactory.javaIntObjectInspector, PrimitiveObjectInspectorFactory.javaIntObjectInspector)
      )

    override def evaluate(args: Array[DeferredObject]): AnyRef = {
      println("Type = %s".format(args(0).getClass.getName))
      Integer.valueOf(args(0).get.asInstanceOf[TestPair].entry._2)
    }

    override def getDisplayString(p1: Array[String]): String = ""
  }
}

