package com.github.tkqubo.hyperd_investigation

import java.nio.file.{ Files, Path, Paths }

import com.tableau.hyperapi.Sql.escapeStringLiteral
import com.tableau.hyperapi.{ Connection, CreateMode, HyperProcess, Nullability, SqlType, TableDefinition, TableName, Telemetry }

import scala.jdk.CollectionConverters._
import scala.util.{ Failure, Success, Try, Using }

object AssertCopySuccess extends App {
  val temporaryDir: Path = Files.createTempDirectory("hyper")
  temporaryDir.toFile.deleteOnExit()

  val output = Paths.get(temporaryDir.toString, "sample.hyper")
  val tableName = new TableName("Extract")

  Using(new HyperProcess(Paths.get("./lib/hyper"), Telemetry.SEND_USAGE_DATA_TO_TABLEAU)) { process =>
    // For bigInt()
    assertSuccess(process, SqlType.bigInt(), Long.MaxValue)
    assertSuccess(process, SqlType.bigInt(), Long.MinValue)
    assertSuccess(process, SqlType.bigInt(), Int.MaxValue)
    assertSuccess(process, SqlType.bigInt(), Int.MinValue)
    assertSuccess(process, SqlType.bigInt(), Short.MaxValue)
    assertSuccess(process, SqlType.bigInt(), Short.MinValue)
    assertFailure(process, SqlType.bigInt(), "9223372036854775808") // Long.MaxValue + 1
    assertFailure(process, SqlType.bigInt(), "-9223372036854775809") // Long.MinValue - 1
    // For integer()
    assertSuccess(process, SqlType.integer(), Int.MaxValue)
    assertSuccess(process, SqlType.integer(), Int.MinValue)
    assertSuccess(process, SqlType.integer(), Short.MaxValue)
    assertSuccess(process, SqlType.integer(), Short.MinValue)
    assertFailure(process, SqlType.integer(), "2147483648") // Int.MaxValue + 1
    assertFailure(process, SqlType.integer(), "-2147483649") // Int.MinValue - 1
    // For smallInt()
    assertSuccess(process, SqlType.smallInt(), Short.MaxValue)
    assertSuccess(process, SqlType.smallInt(), Short.MinValue)
    assertFailure(process, SqlType.smallInt(), "32768") // Short.MaxValue + 1
    assertFailure(process, SqlType.smallInt(), "-32769") // Short.MinValue - 1
    // For doublePrecision()
    assertSuccess(process, SqlType.doublePrecision(), Double.MaxValue)
    assertSuccess(process, SqlType.doublePrecision(), Double.MinValue)
    // For numeric()
    assertSuccess(process, SqlType.numeric(18, 0), "123456789012345678")
    assertSuccess(process, SqlType.numeric(18, 18), ".123456789012345678")
    assertSuccess(process, SqlType.numeric(1, 0), "9")
    // For Double precision
    val validBigDecimalStrings = Seq(
      s"1E${Math.pow(2, 31).toLong - 1}",
      s"""${"9" * 999999}E${Math.pow(2, 31).toLong - 1}"""
    )
    validBigDecimalStrings.foreach { raw =>
      assertSuccess(process, SqlType.doublePrecision(), raw)
      assertSuccess(BigDecimal(raw))
    }
    val invalidBigDecimalString = s"1E${Math.pow(2, 31).toLong}"
    // MEMO: The valid range of SqlType.doublePrecision() and that of BigDecimal seems similar
    assertFailure(process, SqlType.doublePrecision(), invalidBigDecimalString)
    assertFailure(BigDecimal(invalidBigDecimalString))
  }.get

  private def assertSuccess(f: => Any): Unit =
    println(s"Success as expected: ${trim(String.valueOf(f))}")

  private def assertFailure(f: => Any): Unit =
    Try(f) match {
      case Success(value) =>
        println(s"Success but not expected: ${trim(String.valueOf(value))}")
      case Failure(exception) =>
        println(s"Fail as expected: ${exception.getMessage}")
    }

  private def assertSuccess(process: HyperProcess, sqlType: SqlType, values: Any*): Unit =
    assertResult(process, sqlType, values: _*) {
      case Success(_) =>
        println(s"""Success - For $sqlType, these values inserted successfully: ${trim(values.mkString(","))}""")
      case Failure(exception) =>
        throw exception
    }

  private def assertFailure(process: HyperProcess, sqlType: SqlType, values: Any*): Unit =
    assertResult(process, sqlType, values: _*) {
      case Success(_) =>
        throw new IllegalStateException(s"""Success but not as expected - For $sqlType, these values not inserted: ${trim(values.mkString(", "))}""")
      case Failure(exception) =>
        println(s"""Failure as expected - For $sqlType, these values not inserted: ${trim(values.mkString(", "))}""")
        println(s"  with error message: ${exception.getMessage}")
    }

  private def assertResult(process: HyperProcess, sqlType: SqlType, values: Any*)(checkResult: Try[Long] => Unit): Unit = {
    val input: Path = createCsvFile(values: _*)
    Using(new Connection(process.getEndpoint, output.toString, CreateMode.CREATE_AND_REPLACE)) { connection =>
      connection.getCatalog.createTable(createTable(sqlType))
      val result = Try(connection.executeCommand(s"COPY $tableName FROM ${escapeStringLiteral(input.toString)} WITH (format csv, NULL '', delimiter ',', header)").getAsLong)
      checkResult(result)
    }.get
  }

  private def createTable(sqlType: SqlType): TableDefinition =
    new TableDefinition(tableName, List(new TableDefinition.Column("column", sqlType)).asJava)

  private def createCsvFile(values: Any*) = {
    val csv = ("value" +: values).mkString("\n")
    val csvFile = Files.createTempFile(temporaryDir, "", ".csv")
    Files.write(csvFile, csv.getBytes())
    csvFile
  }

  private def trim(text: String): String = if (text.length > 80) s"${text.substring(0, 80)}..." else text
}
