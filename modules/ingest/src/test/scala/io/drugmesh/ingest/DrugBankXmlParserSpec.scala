package io.drugmesh.ingest

import scala.xml.XML

class DrugBankXmlParserSpec extends munit.FunSuite {

  private val sample = XML.loadString(
    """<drugbank>
      |  <drug type="biotech">
      |    <drugbank-id primary="true">DB00001</drugbank-id>
      |    <drugbank-id>BTD00024</drugbank-id>
      |    <name>Lepirudin</name>
      |    <description>Lepirudin is a recombinant hirudin used as an anticoagulant.</description>
      |    <cas-number>138068-37-8</cas-number>
      |    <calculated-properties>
      |      <property><kind>InChIKey</kind><value>ABCDEFGHIJKLMN-OPQRSTUVWX-Y</value></property>
      |    </calculated-properties>
      |    <external-identifiers>
      |      <external-identifier><resource>PubChem Compound</resource><identifier>46507011</identifier></external-identifier>
      |      <external-identifier><resource>ChEMBL</resource><identifier>CHEMBL1201666</identifier></external-identifier>
      |      <external-identifier><resource>KEGG Drug</resource><identifier>D06880</identifier></external-identifier>
      |    </external-identifiers>
      |  </drug>
      |  <drug type="small molecule">
      |    <drugbank-id primary="true">DB00002</drugbank-id>
      |    <name>Cetuximab</name>
      |    <cas-number>205923-56-4</cas-number>
      |    <external-identifiers>
      |      <external-identifier><resource>ChEBI</resource><identifier>643474</identifier></external-identifier>
      |    </external-identifiers>
      |  </drug>
      |</drugbank>""".stripMargin
  )

  test("parses primary drugbank id, name and external identifiers") {
    val recs = DrugBankXmlParser.parseElem(sample)
    assertEquals(recs.size, 2)

    val lepirudin = recs.head
    assertEquals(lepirudin.entry.drugbankId, "DB00001")
    assertEquals(lepirudin.entry.name, "Lepirudin")
    assertEquals(lepirudin.entry.pubchemCid, Some("46507011"))
    assertEquals(lepirudin.entry.chemblId, Some("CHEMBL1201666"))
    assertEquals(lepirudin.entry.keggId, Some("D06880"))
    assertEquals(lepirudin.entry.casNum, Some("138068-37-8"))
    assertEquals(lepirudin.inchiKey, Some("ABCDEFGHIJKLMN-OPQRSTUVWX-Y"))
    assert(lepirudin.entry.chebiId.isEmpty)
    assert(lepirudin.description.exists(_.contains("anticoagulant")))
  }

  test("ignores drugs without a primary id and keeps unmapped fields as None") {
    val recs = DrugBankXmlParser.parseElem(sample)
    val cetuximab = recs(1)
    assertEquals(cetuximab.entry.chebiId, Some("643474"))
    assertEquals(cetuximab.entry.pubchemCid, None)
  }

  test("KEGG block parser extracts entry, name and DBLINKS cross-references") {
    val block =
      """ENTRY       D00528                      Drug
        |NAME        Caffeine (JP18);
        |            Cafcit (TN)
        |DBLINKS     CAS: 58-08-2
        |            PubChem: 7847594
        |            ChEBI: 27732
        |""".stripMargin
    val kegg = TsvSources.parseKeggBlock(block)
    assertEquals(kegg.keggId, "D00528")
    assertEquals(kegg.name, "Caffeine (JP18)")
    assertEquals(kegg.casNum, Some("58-08-2"))
    assertEquals(kegg.pubchemCid, Some("7847594"))
    assertEquals(kegg.chebiId, Some("27732"))
  }
}
