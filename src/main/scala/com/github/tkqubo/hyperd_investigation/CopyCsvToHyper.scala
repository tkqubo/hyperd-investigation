package com.github.tkqubo.hyperd_investigation

import java.nio.file.{ Files, Path, Paths }
import java.util.Date

import com.tableau.hyperapi.Sql.escapeStringLiteral
import com.tableau.hyperapi.TableDefinition.Column
import com.tableau.hyperapi.{ Connection, CreateMode, HyperProcess, SqlType, TableDefinition, TableName, Telemetry }

import scala.jdk.CollectionConverters._
import scala.util.Using

object CopyCsvToHyper extends App {
  val temporaryDir: Path = Files.createTempDirectory("hyper")
  temporaryDir.toFile.deleteOnExit()

  val output = Paths.get("out/from-csv.hyper")
  val tableName = new TableName("Extract" + new Date().getTime)

  Using(new HyperProcess(Paths.get("./lib/hyper"), Telemetry.SEND_USAGE_DATA_TO_TABLEAU)) { process =>
    Using(new Connection(process.getEndpoint, output.toString, CreateMode.CREATE_AND_REPLACE)) { connection =>
      val tableDefinition = new TableDefinition(
        tableName,
        List(
          new Column("bigIntColumn", SqlType.bigInt()),
          new Column("integerColumn", SqlType.integer()),
          new Column("smallIntColumn", SqlType.smallInt()),
          new Column("doubleColumn", SqlType.doublePrecision()),
          new Column("numeric18s0Column", SqlType.numeric(18, 0)),
          new Column("numeric18s18Column", SqlType.numeric(18, 18)),
          new Column("numeric1s0Column", SqlType.numeric(1, 0))
        ).asJava
      )
      connection.getCatalog.createTable(tableDefinition)
      val input: Path = createCsvFile(
        Seq(
          tableDefinition.getColumns.asScala.map(_.getName.getUnescaped).toSeq,
          Seq[Any](Long.MaxValue, Int.MaxValue, Short.MaxValue, Double.MaxValue, "123456789012345678", ".123456789012345678", 1),
          Seq[Any](Long.MaxValue - 1, Int.MaxValue - 1, Short.MaxValue - 1, Double.MaxValue - 1, "123456789012345678", ".123456789012345678", 1),
          Seq[Any](0, 0, 0, "1.7976931348623158E288", 0, 0, 0),
          Seq[Any](Long.MinValue + 1, Int.MinValue + 1, Short.MinValue + 1, Double.MinValue + 1, "-123456789012345678", "-.123456789012345678", -1),
          Seq[Any](Long.MinValue, Int.MinValue, Short.MinValue, "-1.7976931348623157E308", "-123456789012345678", "-.123456789012345678", -1)
        )
      )
      connection.executeCommand(s"COPY $tableName FROM ${escapeStringLiteral(input.toString)} WITH (format csv, NULL '', delimiter ',', header)").getAsLong

      println(Files.readAllLines(input).asScala.mkString("\n"))
    }.get
  }.get


  private def createCsvFile(values: Seq[Seq[Any]]) = {
    val csv = values.map(_.mkString(",")).mkString("\n")
    val csvFile = Files.createTempFile(temporaryDir, "", ".csv")
    Files.write(csvFile, csv.getBytes())
    csvFile
  }
}
