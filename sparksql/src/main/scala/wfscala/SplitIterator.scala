package wfscala

/**
 * Created by root on 14-5-8.
 */
object SplitIterator {
  def main(args: Array[String]) {
    val a = Seq(1,2,3,4,5,6,7,8,9,10).iterator
    println(a.slice(0,2).toSeq)
    println(split(5,a,0))
    println(split(5,a,1))


  }
  def split(n: Int, it: Iterator[Int], index: Int): Iterator[Int] = {
    val len = it.size
    val startIndex = len / n * index
    val endIndex =  len / n * (index+1)
    println(len+ " " + startIndex+" "+endIndex)
    it.slice(startIndex,endIndex)
  }

}
