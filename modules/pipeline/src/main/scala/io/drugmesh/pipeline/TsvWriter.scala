package io.drugmesh.pipeline

import io.drugmesh.core.DrugEntry
import org.apache.spark.sql.{Dataset, SparkSession}

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}

/**
 * Reads and writes the legacy `drug-mappings.tsv` contract so the rewrite can be validated
 * by diff against the original artifact. The single-file local writer collects on the driver
 * (the table is ~22k rows) and emits a deterministic, drugbankId-sorted file with the exact
 * 13-column header; a distributed text writer is provided for cluster-scale output.
 */
object TsvWriter {

  /** Deterministic single-file output that diff-matches the original TSV. */
  def writeLocal(ds: Dataset[DrugEntry], path: String): Unit = {
    val rows    = ds.collect().sortBy(_.drugbankId).map(_.toTsvRow)
    val content = (DrugEntry.Header +: rows).mkString("\n") + "\n"
    val target  = Paths.get(path)
    Option(target.getParent).foreach(Files.createDirectories(_))
    Files.write(target, content.getBytes(UTF_8))
  }

  /** Cluster-scale output: header-less sharded text (use for very large tables). */
  def writeDistributed(ds: Dataset[DrugEntry], dir: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    ds.map(_.toTsvRow).write.mode("overwrite").text(dir)
  }

  /** Load an existing legacy TSV back into typed entries (skips the header). */
  def readTsv(spark: SparkSession, path: String): Dataset[DrugEntry] = {
    import spark.implicits._
    spark.read
      .text(path)
      .as[String]
      .filter(line => line.nonEmpty && !line.startsWith("drugbankId\t"))
      .flatMap(line => DrugEntry.fromTsvRow(line).toOption)
  }
}
