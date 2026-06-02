package io.drugmesh.core

class NormalizationSpec extends munit.FunSuite {

  test("normalizeName lowercases, strips accents and punctuation") {
    assertEquals(Normalization.normalizeName("Acetylsalicylic-Acid (ASA)"), "acetylsalicylic acid asa")
    assertEquals(Normalization.normalizeName("Café Citrate"), "cafe citrate")
  }

  test("stripSalts removes salt/hydrate descriptors") {
    assertEquals(Normalization.stripSalts("Fluoxetine Hydrochloride"), "fluoxetine")
    assertEquals(Normalization.stripSalts("Diclofenac Sodium"), "diclofenac")
  }

  test("blockingKey is salt-stripped, despaced and truncated") {
    assertEquals(Normalization.blockingKey("Fluoxetine Hydrochloride", 6), "fluoxe")
  }

  test("inchiKeySkeleton returns the 14-char connectivity block") {
    assertEquals(
      Normalization.inchiKeySkeleton("BSYNRYMUTXBXSQ-UHFFFAOYSA-N"),
      Some("BSYNRYMUTXBXSQ")
    )
    assertEquals(Normalization.inchiKeySkeleton("tooshort"), None)
  }

  test("isValidCas accepts known-good and rejects bad checksums") {
    assertEquals(Normalization.isValidCas("50-78-2"), true)   // aspirin
    assertEquals(Normalization.isValidCas("7732-18-5"), true) // water
    assertEquals(Normalization.isValidCas("50-78-3"), false)  // wrong check digit
    assertEquals(Normalization.isValidCas("not-a-cas"), false)
  }

  test("string similarities behave at the boundaries") {
    assertEquals(Normalization.levenshtein("kitten", "sitting"), 3)
    assert(Normalization.jaroWinkler("paracetamol", "paracetamol") == 1.0)
    assert(Normalization.jaroWinkler("acetaminophen", "acetaminophen ") > 0.9)
    assert(Normalization.tokenJaccard("acetylsalicylic acid", "acid acetylsalicylic") == 1.0)
  }

  test("DrugEntry round-trips through the legacy TSV row contract") {
    val e = DrugEntry(
      drugbankId = "DB00945",
      name = "Acetylsalicylic acid",
      pubchemCid = Some("2244"),
      casNum = Some("50-78-2"),
      chemblId = Some("CHEMBL25"),
      umlsCuis = Seq("C0004057", "C0718405")
    )
    val row = e.toTsvRow
    assert(row.contains("C0004057,C0718405"))
    assert(row.split("\t", -1).length == DrugEntry.Columns.length)
    val parsed = DrugEntry.fromTsvRow(row)
    assertEquals(parsed, Right(e))
  }

  test("missing identifiers serialize as the 'null' token") {
    val e = DrugEntry(drugbankId = "DB1", name = "x")
    val cols = e.toTsvRow.split("\t", -1)
    assertEquals(cols(2), "null")
    assertEquals(cols(11), "null")
  }
}
