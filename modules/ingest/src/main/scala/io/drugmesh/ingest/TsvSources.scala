package io.drugmesh.ingest

import org.apache.spark.sql.{Dataset, SparkSession}

/** Partial records contributed by each non-DrugBank source, keyed for joining downstream. */
final case class TtdDrug(ttdId: String, name: String, casNum: Option[String], pubchemCid: Option[String])
final case class KeggDrug(keggId: String, name: String, casNum: Option[String], chebiId: Option[String], pubchemCid: Option[String])
final case class StitchMap(drugbankId: String, stitchId: String)
final case class DgidbDrug(drugbankId: Option[String], name: String, chemblId: Option[String], source: String)

/**
 * Readers for the flat-file sources (TTD raw dump, KEGG drug file, DrugBank-SIDER/STITCH
 * mapping, DGIdb export). Each returns a typed `Dataset` so the pipeline joins are
 * statically checked. Block-structured files are read with `wholeTextFiles` and split on
 * their record delimiter; simple TSVs use the Spark CSV reader.
 */
object TsvSources {

  private def opt(s: String): Option[String] = {
    val t = if (s == null) "" else s.trim
    if (t.isEmpty || t.equalsIgnoreCase("null") || t == ".") None else Some(t)
  }

  /**
   * TTD raw drug download: records separated by blank lines, each a set of `KEY<TAB>VALUE`
   * lines (TTDDRUGID, DRUGNAME, CASNUMBER, PUBCHCID, ...). We pull the fields we map.
   */
  def readTtd(spark: SparkSession, path: String): Dataset[TtdDrug] = {
    import spark.implicits._
    spark.sparkContext
      .wholeTextFiles(path)
      .flatMap { case (_, content) => content.split("(?m)^\\s*$") }
      .map { block =>
        val kv = block.linesIterator
          .map(_.split("\t", 2))
          .collect { case Array(k, v) => k.trim.toUpperCase -> v.trim }
          .toMap
        TtdDrug(
          ttdId = kv.getOrElse("TTDDRUGID", ""),
          name = kv.getOrElse("DRUGNAME", ""),
          casNum = kv.get("CASNUMBER").flatMap(opt),
          pubchemCid = kv.get("PUBCHCID").flatMap(opt)
        )
      }
      .filter(d => d.ttdId.nonEmpty && d.name.nonEmpty)
      .toDS()
  }

  /**
   * KEGG `drug` flat file: records terminated by `///`; fields are `ENTRY`, `NAME`, and a
   * `DBLINKS` section with `PubChem:`, `ChEBI:`, `CAS:` cross-references.
   */
  def readKegg(spark: SparkSession, path: String): Dataset[KeggDrug] = {
    import spark.implicits._
    spark.sparkContext
      .wholeTextFiles(path)
      .flatMap { case (_, content) => content.split("///") }
      .map(parseKeggBlock)
      .filter(_.keggId.nonEmpty)
      .toDS()
  }

  private[ingest] def parseKeggBlock(block: String): KeggDrug = {
    val lines = block.linesIterator.toVector
    def firstField(tag: String): Option[String] =
      lines.find(_.startsWith(tag)).map(_.stripPrefix(tag).trim).flatMap(opt)
    val dblinks = lines
      .dropWhile(!_.startsWith("DBLINKS"))
      .takeWhile(l => l.startsWith("DBLINKS") || l.startsWith(" "))
      .map(_.replace("DBLINKS", "").trim)
    def dbLink(prefix: String): Option[String] =
      dblinks.find(_.startsWith(prefix)).map(_.stripPrefix(prefix).trim).flatMap(opt)
    KeggDrug(
      keggId = firstField("ENTRY").map(_.split("\\s+").head).getOrElse(""),
      name = firstField("NAME").map(_.split(";").head).getOrElse(""),
      casNum = dbLink("CAS:"),
      chebiId = dbLink("ChEBI:"),
      pubchemCid = dbLink("PubChem:")
    )
  }

  /** DrugBank-SIDER mapping used to attach STITCH ids: a 2+ column TSV (drugbankId, stitchId). */
  def readStitchSider(spark: SparkSession, path: String): Dataset[StitchMap] = {
    import spark.implicits._
    spark.read
      .option("sep", "\t")
      .option("header", value = true)
      .csv(path)
      .toDF()
      .flatMap { r =>
        val db = Option(r.getString(0)).map(_.trim).filter(_.nonEmpty)
        val st = Option(r.getString(1)).map(_.trim).filter(_.nonEmpty)
        for { d <- db; s <- st } yield StitchMap(d, s)
      }
  }

  /** DGIdb drugs export: used to cross-check ChEMBL ids by DrugBank id or by name. */
  def readDgidb(spark: SparkSession, path: String): Dataset[DgidbDrug] = {
    import spark.implicits._
    spark.read
      .option("sep", "\t")
      .option("header", value = true)
      .csv(path)
      .toDF()
      .flatMap { r =>
        val cols = r.schema.fieldNames.zipWithIndex.toMap
        def col(name: String): Option[String] =
          cols.get(name).flatMap(i => Option(r.getString(i))).flatMap(opt)
        col("name").map { nm =>
          DgidbDrug(
            drugbankId = col("drugbank_id").orElse(col("concept_id")),
            name = nm,
            chemblId = col("chembl_id"),
            source = col("source").getOrElse("DGIdb")
          )
        }
      }
  }
}
