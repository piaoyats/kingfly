package scwf

/**
 * Created by root on 14-7-13.
 */
object Sync {
  val a = new Conf()
  def main(args: Array[String]) {
    new Thread{
      override def run() = {
        a.synchronized {
          val x = new Conf(a)
          println("sfl;"+x.getA)
        }
      }
    }.start()

    new Thread{
      override def run() = {
        a.synchronized {
          val x = new Conf(a)
          println("sfl;"+x.getA)
        }
      }
    }.start()

    new Thread{
      override def run() = {
        a.synchronized {
          val x = new Conf(a)
          println("sfl;"+x.getA)
        }
      }
    }.start()
  }
//  def run() = {
//    a.synchronized {
//      val x = new Conf(a)
//      println("sfl;"+x.getA)
//    }
//  }

}

