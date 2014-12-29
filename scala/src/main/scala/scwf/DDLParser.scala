package scwf

import scala.util.parsing.combinator.syntactical.StandardTokenParsers
import scala.util.parsing.combinator.{RegexParsers, PackratParsers}
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.catalyst.SqlLexical
import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.types.StructField
import scala.Some
/**
 * Created by w00228970 on 2014/12/18.
 */
object Parser {
  def main (args: Array[String]) {
    val p = new DDLParser
    val sql = """
                |CREATE TEMPORARY TABLE oneToTen_with_schema(i int)
                |USING org.apache.spark.sql.sources.SimpleScanSource
                |OPTIONS (
                |  from '1',
                |  to '10'
                |)
              """.stripMargin
   println(p(sql))
  }
}

/**
 * A parser for foreign DDL commands.
 */
private class DDLParser extends StandardTokenParsers with PackratParsers with Logging {

  def apply(input: String): Option[String] = {
    phrase(ddl)(new lexical.Scanner(input)) match {
      case Success(r, x) => Some(r)
      case x =>
        logDebug(s"Not recognized as DDL: $x")
        None
    }
  }

  protected case class Keyword(str: String)

  protected implicit def asParser(k: Keyword): Parser[String] =
    lexical.allCaseVersions(k.str).map(x => x : Parser[String]).reduce(_ | _)

  protected val STRING = Keyword("STRING")
  protected val SHORT = Keyword("SHORT")
  protected val DOUBLE = Keyword("DOUBLE")
  protected val BOOLEAN = Keyword("BOOLEAN")
  protected val BYTE = Keyword("BYTE")
  protected val FLOAT = Keyword("FLOAT")
  protected val INT = Keyword("INT")
  protected val INTEGER = Keyword("INTEGER")
  protected val LONG = Keyword("LONG")
  protected val TINYINT = Keyword("TINYINT")
  protected val SMALLINT = Keyword("SMALLINT")
  protected val BIGINT = Keyword("BIGINT")



  protected val CREATE = Keyword("CREATE")
  protected val TEMPORARY = Keyword("TEMPORARY")
  protected val TABLE = Keyword("TABLE")
  protected val USING = Keyword("USING")
  protected val OPTIONS = Keyword("OPTIONS")

  // Use reflection to find the reserved words defined in this class.
  protected val reservedWords =
    this.getClass
      .getMethods
      .filter(_.getReturnType == classOf[Keyword])
      .map(_.invoke(this).asInstanceOf[Keyword].str)

  override val lexical = new SqlLexical(reservedWords)

  protected lazy val ddl: Parser[String] = createTable

  /**
   * `CREATE TEMPORARY TABLE avroTable
   * USING org.apache.spark.sql.avro
   * OPTIONS (path "../hive/src/test/resources/data/files/episodes.avro")`
   * or
   * `CREATE TEMPORARY TABLE avroTable(intField int, stringField string...)
   * USING org.apache.spark.sql.avro
   * OPTIONS (path "../hive/src/test/resources/data/files/episodes.avro")`
   */
  protected lazy val createTable: Parser[String] =
    (  CREATE ~ TEMPORARY ~ TABLE ~> ident ~ (USING ~> className) ~ (OPTIONS ~> options) ^^ {
      case tableName ~ provider ~ opts =>
        "1"
    }
      |
      CREATE ~ TEMPORARY ~ TABLE ~> ident ~ tableCols  ~ (USING ~> className) ~ (OPTIONS ~> options) ^^ {
        case tableName ~ tableColumns ~ provider ~ opts =>
          tableColumns.foreach(println)
        "2"
      }
      )

  protected lazy val metastoreTypes = new MetastoreTypes()

  protected lazy val tableCols: Parser[Seq[StructField]] =
    "(" ~> repsep(column, ",") <~ ")"

  protected lazy val options: Parser[Map[String, String]] =
    "(" ~> repsep(pair, ",") <~ ")" ^^ { case s: Seq[(String, String)] => s.toMap }

  protected lazy val className: Parser[String] = repsep(ident, ".") ^^ { case s => s.mkString(".")}

  protected lazy val pair: Parser[(String, String)] = ident ~ stringLit ^^ { case k ~ v => (k,v) }

  protected lazy val column: Parser[StructField] =
    ident ~ (STRING | BYTE | SHORT | INT | INTEGER | LONG | FLOAT | DOUBLE | BOOLEAN) ^^ { case k ~ v =>
      StructField(k, metastoreTypes.toDataType(v), true)
    }

}

/**
 * :: DeveloperApi ::
 * Provides a parser for data types.
 */
@DeveloperApi
private class MetastoreTypes extends RegexParsers {
  protected lazy val primitiveType: Parser[DataType] =
    "string" ^^^ StringType |
      "float" ^^^ FloatType |
      "int" ^^^ IntegerType |
      "tinyint" ^^^ ByteType |
      "smallint" ^^^ ShortType |
      "double" ^^^ DoubleType |
      "bigint" ^^^ LongType |
      "binary" ^^^ BinaryType |
      "boolean" ^^^ BooleanType |
      "timestamp" ^^^ TimestampType |
      "varchar\\((\\d+)\\)".r ^^^ StringType

  protected lazy val arrayType: Parser[DataType] =
    "array" ~> "<" ~> dataType <~ ">" ^^ {
      case tpe => ArrayType(tpe)
    }

  protected lazy val mapType: Parser[DataType] =
    "map" ~> "<" ~> dataType ~ "," ~ dataType <~ ">" ^^ {
      case t1 ~ _ ~ t2 => MapType(t1, t2)
    }

  protected lazy val structField: Parser[StructField] =
    "[a-zA-Z0-9_]*".r ~ ":" ~ dataType ^^ {
      case name ~ _ ~ tpe => StructField(name, tpe, nullable = true)
    }

  protected lazy val structType: Parser[DataType] =
    "struct" ~> "<" ~> repsep(structField,",") <~ ">"  ^^ {
      case fields => new StructType(fields)
    }

  protected lazy val dataType: Parser[DataType] =
    arrayType |
      mapType |
      structType |
      primitiveType

  def toDataType(metastoreType: String): DataType = parseAll(dataType, metastoreType) match {
    case Success(result, _) => result
    case failure: NoSuccess => sys.error(s"Unsupported dataType: $metastoreType")
  }

  def toMetastoreType(dt: DataType): String = dt match {
    case ArrayType(elementType, _) => s"array<${toMetastoreType(elementType)}>"
    case StructType(fields) =>
      s"struct<${fields.map(f => s"${f.name}:${toMetastoreType(f.dataType)}").mkString(",")}>"
    case MapType(keyType, valueType, _) =>
      s"map<${toMetastoreType(keyType)},${toMetastoreType(valueType)}>"
    case StringType => "string"
    case FloatType => "float"
    case IntegerType => "int"
    case ByteType => "tinyint"
    case ShortType => "smallint"
    case DoubleType => "double"
    case LongType => "bigint"
    case BinaryType => "binary"
    case BooleanType => "boolean"
    case TimestampType => "timestamp"
    case NullType => "void"
  }
}

/*
    "string" ^^^ StringType |
      "float" ^^^ FloatType |
      "int" ^^^ IntegerType |
      "tinyint" ^^^ ByteType |
      "smallint" ^^^ ShortType |
      "double" ^^^ DoubleType |
      "bigint" ^^^ LongType |
      "binary" ^^^ BinaryType |
      "boolean" ^^^ BooleanType |
      fixedDecimalType |                     // Hive 0.13+ decimal with precision/scale
      "decimal" ^^^ DecimalType.Unlimited |  // Hive 0.12 decimal with no precision/scale
      "date" ^^^ DateType |
      "timestamp" ^^^ TimestampType |
      "varchar\\((\\d+)\\)".r ^^^ StringType

      (STRING | FLOAT | TINYINT | INT | SMALLINT | BIGINT | BINARY | DOUBLE | BOOLEAN | DECIMAL | DATE | TIMESTAMP)
 */