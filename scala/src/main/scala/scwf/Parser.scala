package scwf

import scala.util.parsing.combinator.syntactical.StandardTokenParsers
import scala.util.parsing.combinator.PackratParsers
import org.apache.spark.sql.catalyst.types.StructField
import org.apache.spark.sql.catalyst.SqlLexical

/**
 * Created by w00228970 on 2014/12/18.
 */
object Parser {
  def main (args: Array[String]) {
    val p = new DDLParser
   println(p("(a int, b int)"))
  }
}

class DDLParser extends StandardTokenParsers with PackratParsers {
  protected case class Keyword(str: String)
  protected val STRING = Keyword("(")
  protected val SHORT = Keyword(")")

  // Use reflection to find the reserved words defined in this class.
  protected val reservedWords =
    this.getClass
      .getMethods
      .filter(_.getReturnType == classOf[Keyword])
      .map(_.invoke(this).asInstanceOf[Keyword].str)

  override val lexical = new SqlLexical(reservedWords)

  protected implicit def asParser(k: Keyword): Parser[String] =
    lexical.allCaseVersions(k.str).map(x => x : Parser[String]).reduce(_ | _)

  def apply(input: String): Option[(String)] = {
    phrase(tableCols)(new lexical.Scanner(input)) match {
      case Success(r, x) => Some(r)
      case x =>
        println(s"Not recognized as DDL: $x")
        None
    }
  }

  protected lazy val tableCols: Parser[String] =
    "(" ~> repsep(column, ",") <~ ")" ^^ { case s: Seq[(String, String)] =>
      s.map { t =>
        t._1+t._2.trim
      }.mkString(".")
    }

  protected val column: Parser[(String, String)] = ident ~ ident ^^ { case k ~ v => (k,v) }
}
