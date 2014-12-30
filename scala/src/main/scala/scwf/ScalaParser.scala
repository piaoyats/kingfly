package scwf

import scala.util.parsing.combinator.syntactical.StandardTokenParsers
import scala.util.parsing.combinator.JavaTokenParsers

/**
 * Created by w00228970 on 2014/12/29.
 */
object ScalaParser extends Arith{
  def main (args: Array[String]) {
    println(parseAll(expr, "2 * 3 * 4"))
  }
}

class Arith extends JavaTokenParsers{
//  lexical.delimiters ++= List("+","*","-","/","(",")")
//  def sum: Parser[Any] = product~"+"~sum | product
//  def product: Parser[Any] = primary~"*"~product | primary
//  def primary: Parser[Any] = "("~expr~")" | numericLit

  def expr: Parser[Any] = term ~ rep("+"~term | "-"~term)
  def term: Parser[Any] = factor~rep("*"~factor | "/"~factor)
  def factor: Parser[Any] = floatingPointNumber | "("~expr~")"

}