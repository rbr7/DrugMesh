package io.drugmesh.ingest

import io.drugmesh.core.DrugEntry
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.xml.{Elem, Node, XML}

/**
 * Intermediate ingest record. Carries the seed [[DrugEntry]] plus two extra fields that
 * the legacy 13-column schema has nowhere to put but the ML stages need:
 *   - `inchiKey`  — a structural blocking key for entity resolution.
 *   - `description` — free text mined by the biomedical NER stage.
 */
final case class DrugBankRecord(
    entry: DrugEntry,
    inchiKey: Option[String],
    description: Option[String]
)

/**
 * Parses the DrugBank full-database XML into seed records, using **scala-xml**.
 *
 * scala-xml is a DOM parser, so the multi-gigabyte full dump should be split or streamed
 * (e.g. `fs2-data-xml`) in production; for tests and the curated subset shipped here, a
 * straight `XML.load` is fine. Parsing happens on the driver and the resulting `Seq` is
 * then parallelized into a `Dataset` so the rest of the pipeline is Spark-native.
 */
object DrugBankXmlParser {

  // DrugBank `resource` label -> the DrugEntry field it populates.
  private def extractExternalIds(drug: Node): Map[String, String] =
    (drug \ "external-identifiers" \ "external-identifier").flatMap { ext =>
      val resource = (ext \ "resource").text.trim
      val id       = (ext \ "identifier").text.trim
      if (resource.nonEmpty && id.nonEmpty) Some(resource -> id) else None
    }.toMap

  private def calculatedProperty(drug: Node, kind: String): Option[String] =
    (drug \ "calculated-properties" \ "property")
      .find(p => (p \ "kind").text.trim.equalsIgnoreCase(kind))
      .map(p => (p \ "value").text.trim)
      .filter(_.nonEmpty)

  private def opt(s: String): Option[String] =
    Option(s).map(_.trim).filter(_.nonEmpty)

  /** Parse a single `<drug>` node. */
  def parseDrug(drug: Node): Option[DrugBankRecord] = {
    val primaryId =
      (drug \ "drugbank-id").find(n => (n \ "@primary").text == "true").map(_.text.trim)
    val name = (drug \ "name").text.trim
    primaryId.filter(_.nonEmpty).map { db =>
      val ext = extractExternalIds(drug)
      val entry = DrugEntry(
        drugbankId = db,
        name = name,
        casNum = opt((drug \ "cas-number").text),
        pubchemCid = ext.get("PubChem Compound").orElse(ext.get("PubChem Substance")),
        chemblId = ext.get("ChEMBL"),
        chebiId = ext.get("ChEBI"),
        keggId = ext.get("KEGG Drug"),
        keggCid = ext.get("KEGG Compound"),
        bindingDbId = ext.get("BindingDB")
      )
      DrugBankRecord(
        entry = entry,
        inchiKey = calculatedProperty(drug, "InChIKey"),
        description = opt((drug \ "description").text)
      )
    }
  }

  /** Parse an in-memory XML root element. */
  def parseElem(root: Elem): Seq[DrugBankRecord] =
    (root \ "drug").flatMap(parseDrug)

  /** Parse a DrugBank XML file on the local/driver filesystem. */
  def parseFile(path: String): Seq[DrugBankRecord] =
    parseElem(XML.loadFile(path))

  /** Load the seed entries as a Spark `Dataset[DrugEntry]`. */
  def loadEntries(spark: SparkSession, path: String): Dataset[DrugEntry] = {
    import spark.implicits._
    spark.createDataset(parseFile(path).map(_.entry))
  }

  /** Load the full ingest records (with InChIKey + description) for the ML stages. */
  def loadRecords(spark: SparkSession, path: String): Dataset[DrugBankRecord] = {
    import spark.implicits._
    spark.createDataset(parseFile(path))
  }
}
