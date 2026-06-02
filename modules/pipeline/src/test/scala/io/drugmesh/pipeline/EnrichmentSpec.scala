package io.drugmesh.pipeline

import io.drugmesh.core.DrugEntry
import io.drugmesh.ingest.{SparkSupport, TtdDrug}
import org.apache.spark.sql.SparkSession

import java.nio.file.Files

class EnrichmentSpec extends munit.FunSuite {

  implicit lazy val spark: SparkSession =
    SparkSupport.session(appName = "drugmesh-test", master = Some("local[2]"), shufflePartitions = 2)

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

  test("mergeTtd fills missing ids by normalized name without overwriting existing values") {
    import spark.implicits._
    val base = spark.createDataset(
      Seq(
        DrugEntry("DB1", "Fluoxetine Hydrochloride"),
        DrugEntry("DB2", "Aspirin", casNum = Some("50-78-2"))
      )
    )
    val ttd = spark.createDataset(
      Seq(
        TtdDrug("D0AAA", "fluoxetine", Some("54-91-1"), Some("3386")),
        TtdDrug("D0BBB", "aspirin", Some("99-99-9"), None)
      )
    )
    val merged = Enrichment.mergeTtd(base, ttd).collect().map(d => d.drugbankId -> d).toMap
    assertEquals(merged("DB1").ttdId, Some("D0AAA"))
    assertEquals(merged("DB1").casNum, Some("54-91-1"))
    assertEquals(merged("DB1").pubchemCid, Some("3386"))
    assertEquals(merged("DB2").casNum, Some("50-78-2")) // existing value wins
  }

  test("applyLookup sets a field from a broadcast lookup and never overwrites") {
    import spark.implicits._
    val base   = spark.createDataset(Seq(DrugEntry("DB1", "x"), DrugEntry("DB2", "y", chemblId = Some("CHEMBL999"))))
    val lookup = spark.createDataset(Seq(("DB1", "CHEMBL1"), ("DB2", "CHEMBL2")))
    val out    = Enrichment.applyLookup(base, lookup)(ApiEnrichment.setChembl).collect().map(d => d.drugbankId -> d).toMap
    assertEquals(out("DB1").chemblId, Some("CHEMBL1"))
    assertEquals(out("DB2").chemblId, Some("CHEMBL999"))
  }

  test("TsvWriter round-trips entries through the legacy 13-column contract") {
    import spark.implicits._
    val dir  = Files.createTempDirectory("drugmesh").toString
    val path = s"$dir/drug-mappings.tsv"
    val entries = Seq(
      DrugEntry("DB2", "b", pubchemCid = Some("2")),
      DrugEntry("DB1", "a", umlsCuis = Seq("C1", "C2"))
    )
    TsvWriter.writeLocal(spark.createDataset(entries), path)
    val back = TsvWriter.readTsv(spark, path).collect().sortBy(_.drugbankId)
    assertEquals(back.map(_.drugbankId).toSeq, Seq("DB1", "DB2"))
    assertEquals(back.find(_.drugbankId == "DB1").get.umlsCuis, Seq("C1", "C2"))
  }
}
