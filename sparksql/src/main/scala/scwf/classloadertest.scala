package scwf

import scala.tools.nsc.util.ScalaClassLoader.URLClassLoader
import java.net.{URL}
import java.lang.ClassLoader

/**
 * Created by root on 14-7-13.
 */
object classloadertest {
  def main(args: Array[String]) {
//    val loader = new URLClassLoader(new Array[URL](0),
//      Thread.currentThread.getContextClassLoader)
//    Thread.currentThread().setContextClassLoader(loader)
//    loader.addURL(new URL("file:///home/code/spark/examples/target/spark-examples_2.10-1.0.0-SNAPSHOT.jar"))
    ClassPathHacker.addURL(new URL("file:///home/code/spark/examples/target/spark-examples_2.10-1.0.0-SNAPSHOT.jar"))
    try {
      val a = Class.forName("org.apache.spark.examples.SparkPi")
//      val a = classOf[org.apache.spark.examples.SparkPi].newInstance()

      println(a.toString)
    }
    catch {
      case e:Exception => print(e.toString)

    }

  }

}
