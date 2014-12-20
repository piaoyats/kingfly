package scwf

import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.io.orc.{OrcNewInputFormat, OrcStruct, OrcSerde}
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.{Job, InputFormat}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by kf on 20/12/14.
 */
object OrcTest {
  def main(args: Array[String]) {
    val prop: Properties = new Properties
    prop.setProperty("columns", "key,value")
    prop.setProperty("columns.types", "int:string")
    @transient lazy val serde = initSerde(prop) // 为什么加上 lazy 可以解决序列化的问题？
    // 只要加了这句就会报序列化问题
//    val soi = serde.getObjectInspector().asInstanceOf[StructObjectInspector]

    val sparkContext = new SparkContext(new SparkConf().setMaster("local").setAppName("orcTest"))
    val inputClass = classOf[OrcNewInputFormat].asInstanceOf[Class[_ <: InputFormat[NullWritable, OrcStruct]]]
    val job = new Job(sparkContext.hadoopConfiguration)
    val conf: Configuration = job.getConfiguration
    val path = new Path("./sparksql/src/main/resource/src_orc/")
    org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths(job, path)

    new org.apache.spark.rdd.NewHadoopRDD(
      sparkContext,
      inputClass,
      classOf[NullWritable],
      classOf[OrcStruct],
      conf).map(_._2).foreachPartition { iter =>
      val result = serde.deserialize(iter.next())
      println(result)
      println(result.getClass.getCanonicalName)

    }

    new org.apache.spark.rdd.NewHadoopRDD(
      sparkContext,
      inputClass,
      classOf[NullWritable],
      classOf[OrcStruct],
      conf).map(_._2).foreachPartition { iter =>
      val result = iter.next()
      // 其实都不需要 serde，读出来的OrcStruct 内部有一个  private Object[] fields; 存的就是真实的数据
      // 说法不对，还是需要将其转换为对应的native类型，比如将intwritable 转为int， 将Text转为 string
      // 但orcserde的deserialize方法其实是屁事不干
      //      public Object deserialize(Writable writable) throws SerDeException {
      //        return writable;
      //      }
      // 答案： 实际上hive是通过 ObjectInspector 来获取native的数据的，而ObjectInspector是通过 各种sink的序列化器得到的，
      // 比如orc的是通过 OrcSerde 获得的，不过获得OrcSerde是需要将元数据信息传入，比如列名，每列的类型
      println(result.toString)

      println(result.getClass.getCanonicalName)

    }


  }

  def initSerde(prop: Properties): OrcSerde = {
    val serde = new OrcSerde
    serde.initialize(null, prop)
    serde
  }

}
