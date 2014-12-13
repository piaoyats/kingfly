package scala.scwf

import java.util.concurrent.{LinkedBlockingDeque, TimeUnit, ThreadPoolExecutor}
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.spark.util.Utils

/**
 * Created by w00228970 on 2014/10/20.
 */
object Hello {
  def main(args: Array[String]) {
    import scala.concurrent._
    //    import ExecutionContext.Implicits.global
    implicit val futureExecContext: ExecutionContext =  ExecutionContext.fromExecutor(
      new ThreadPoolExecutor(1, 1, 60, TimeUnit.SECONDS, new LinkedBlockingDeque[Runnable](), new ThreadFactoryBuilder().setDaemon(true).setNameFormat("scwf").build()))
    val array = 1 to 10
    println("begin!")
    val f: Future[Int] = future {
      array.foldLeft(0)(_ + _)
    }
    import scala.util.{Success, Failure}
    // call back, why onComplete not work!!!??? but onSuccess worked!
    // get it, every time re import ExecutionContext.Implicits.global it will be ok
    f onComplete {
      case Success(a) => println(s"result is $a")
      case Failure(b) => println("An error has occured: " + b.getMessage)
    }
    //    Await.result(f, 1.minute)
    f.onSuccess {
      case a => println(a)
    }
    //    f.onFailure {
    //      case a => println(a.getMessage)
    //    }
    println("end!")
  }
}
