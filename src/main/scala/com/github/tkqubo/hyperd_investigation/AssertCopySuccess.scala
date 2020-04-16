package com.github.tkqubo.hyperd_investigation

import java.nio.file.{ Files, Path, Paths }
import java.util.UUID

import com.tableau.hyperapi.Sql.escapeStringLiteral
import com.tableau.hyperapi.{ Connection, CreateMode, HyperProcess, SqlType, TableDefinition, TableName, Telemetry }

import scala.jdk.CollectionConverters._
import scala.util.{ Failure, Success, Try, Using }

object AssertCopySuccess extends App {
  val temporaryDir: Path = Files.createTempDirectory("hyper")
  temporaryDir.toFile.deleteOnExit()

  val tableName = new TableName("Extract")

  Using(new HyperProcess(Paths.get("./lib/hyper"), Telemetry.SEND_USAGE_DATA_TO_TABLEAU)) { process =>
    // For bigInt()
    assertCopySuccess(process, SqlType.bigInt(), Long.MaxValue)
    assertCopySuccess(process, SqlType.bigInt(), Long.MinValue)
    assertCopySuccess(process, SqlType.bigInt(), Int.MaxValue)
    assertCopySuccess(process, SqlType.bigInt(), Int.MinValue)
    assertCopySuccess(process, SqlType.bigInt(), Short.MaxValue)
    assertCopySuccess(process, SqlType.bigInt(), Short.MinValue)
    assertCopyFailure(process, SqlType.bigInt(), "9223372036854775808") // Long.MaxValue + 1
    assertCopyFailure(process, SqlType.bigInt(), "-9223372036854775809") // Long.MinValue - 1
    // For integer()
    assertCopySuccess(process, SqlType.integer(), Int.MaxValue)
    assertCopySuccess(process, SqlType.integer(), Int.MinValue)
    assertCopySuccess(process, SqlType.integer(), Short.MaxValue)
    assertCopySuccess(process, SqlType.integer(), Short.MinValue)
    assertCopyFailure(process, SqlType.integer(), "2147483648") // Int.MaxValue + 1
    assertCopyFailure(process, SqlType.integer(), "-2147483649") // Int.MinValue - 1
    // For smallInt()
    assertCopySuccess(process, SqlType.smallInt(), Short.MaxValue)
    assertCopySuccess(process, SqlType.smallInt(), Short.MinValue)
    assertCopyFailure(process, SqlType.smallInt(), "32768") // Short.MaxValue + 1
    assertCopyFailure(process, SqlType.smallInt(), "-32769") // Short.MinValue - 1
    // For doublePrecision()
    assertCopySuccess(process, SqlType.doublePrecision(), Double.MaxValue)
    assertCopySuccess(process, SqlType.doublePrecision(), Double.MinValue)
    assertCopySuccess(process, SqlType.doublePrecision(), Double.NaN)
    assertCopySuccess(process, SqlType.doublePrecision(), Double.PositiveInfinity)
    assertCopySuccess(process, SqlType.doublePrecision(), Double.NegativeInfinity)
    //    value a bit larger than Double.PositiveInfinity is regarded as POSITIVE_INFINITY
    assertCopySuccess(process, SqlType.doublePrecision(), "1.7976931348623159E308", java.lang.Double.POSITIVE_INFINITY)
    //    value a bit larger than Double.NegativeInfinity is regarded as NEGATIVE_INFINITY
    assertCopySuccess(process, SqlType.doublePrecision(), "-1.7976931348623159E308", java.lang.Double.NEGATIVE_INFINITY)
    // For numeric()
    assertCopySuccess(process, SqlType.numeric(18, 0), new java.math.BigDecimal(123456789012345678L))
    assertCopySuccess(process, SqlType.numeric(18, 18), new java.math.BigDecimal(".123456789012345678"))
    assertCopySuccess(process, SqlType.numeric(1, 0), new java.math.BigDecimal(9))
    //    fail if
    assertCopyFailure(process, SqlType.numeric(1, 0), 10)
    assertCopyFailure(process, SqlType.numeric(18, 0), 1234567890123456789L)
    assertCopyFailure(process, SqlType.numeric(1, 1), ".12")
    assertCopyFailure(process, SqlType.numeric(18, 18), ".1234567890123456789")
  }.get

  private def assertCopySuccess(process: HyperProcess, sqlType: SqlType, valueInCsv: Any): Unit =
    assertCopySuccess(process, sqlType, valueInCsv, valueInCsv)

  private def assertCopySuccess(process: HyperProcess, sqlType: SqlType, valueInCsv: Any, expectedValueInHyper: Any): Unit = {
    val output = assertCopyResult(process, sqlType, valueInCsv) {
      case Success(_) =>
        println(s"Success - expected: sqlType=$sqlType, value=${trim(valueInCsv)}")
      case Failure(exception) =>
        throw new IllegalStateException(s"Failed - not expected: sqlType=$sqlType, value=${trim(valueInCsv)}, error=${exception.getMessage}")
    }
    assertHyperValue(process, output, expectedValueInHyper)
  }

  private def assertCopyFailure(process: HyperProcess, sqlType: SqlType, valueInCsv: Any): Unit =
    assertCopyResult(process, sqlType, valueInCsv) {
      case Success(_) =>
        throw new IllegalStateException(s"Success - not expected: sqlType=$sqlType, value=${trim(valueInCsv)}")
      case Failure(exception) =>
        println(s"Failed - expected: sqlType=$sqlType, value=${trim(valueInCsv)}, error=${exception.getMessage}")
    }

  private def assertCopyResult(process: HyperProcess, sqlType: SqlType, valueInCsv: Any)(checkResult: Try[Long] => Unit): Path = {
    val input: Path = createSingleValuedCsvFile(valueInCsv)
    val output: Path = Paths.get(temporaryDir.toString, s"${UUID.randomUUID()}.hyper")
    Using(new Connection(process.getEndpoint, output.toString, CreateMode.CREATE_AND_REPLACE)) { connection =>
      connection.getCatalog.createTable(createSingleValuedTable(sqlType))
      val result = Try(connection.executeCommand(s"COPY $tableName FROM ${escapeStringLiteral(input.toString)} WITH (format csv, NULL '', delimiter ',', header)").getAsLong)
      checkResult(result)
      output
    }.get
  }

  private def assertHyperValue(process: HyperProcess, output: Path, expectedValueInHyper: Any): Unit = {
    Using(new Connection(process.getEndpoint, output.toString, CreateMode.NONE)) { connection =>
      val result = connection.executeQuery(s"SELECT * FROM $tableName")
      result.nextRow()
      val actual = result.getObject(0)
      if (actual != expectedValueInHyper && !(isNaN(actual) && isNaN(expectedValueInHyper))) {
        throw new IllegalStateException(s"actual=$actual (${actual.getClass}) while expected=$expectedValueInHyper (${expectedValueInHyper.getClass})")
      }
    }.get
  }

  private def isNaN(value: Any) = value match {
    case double: Double if double.isNaN => true
    case _ => false
  }

  private def createSingleValuedTable(sqlType: SqlType): TableDefinition =
    new TableDefinition(tableName, List(new TableDefinition.Column("column", sqlType)).asJava)

  private def createSingleValuedCsvFile(value: Any) = {
    val csv = Seq("value", value).mkString("\n")
    val csvFile = Files.createTempFile(temporaryDir, "", ".csv")
    Files.write(csvFile, csv.getBytes())
    csvFile
  }

  private def trim(any: Any): String = {
    val text = any.toString
    if (text.length > 80) s"${text.substring(0, 80)}..." else text
  }
}
