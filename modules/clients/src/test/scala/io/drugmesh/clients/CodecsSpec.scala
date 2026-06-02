package io.drugmesh.clients

import io.circe.parser.decode

class CodecsSpec extends munit.FunSuite {
  import Codecs._

  test("decodes a UniChem cross-map response") {
    val json = """[{"src_compound_id":"CHEMBL25"},{"src_compound_id":"CHEMBL1697753"}]"""
    val got  = decode[List[UniChemHit]](json)
    assertEquals(got.map(_.headOption.map(_.src_compound_id)), Right(Some("CHEMBL25")))
  }

  test("decodes a PubChem PUG-REST CID list") {
    val json = """{"IdentifierList":{"CID":[2244,5090]}}"""
    assertEquals(decode[PubChemCidList](json).map(_.IdentifierList.CID.head), Right(2244L))
  }

  test("decodes a UMLS search payload and drops NONE results") {
    val json =
      """{"result":{"results":[
        |  {"ui":"C0004057","name":"Aspirin","rootSource":"RXNORM"},
        |  {"ui":"NONE","name":"NO RESULTS"}
        |]}}""".stripMargin
    val cuis = decode[UmlsSearchResult](json).map(_.result.results.map(_.ui).filter(_ != "NONE"))
    assertEquals(cuis, Right(List("C0004057")))
  }

  test("decodes a DGIdb interactions response") {
    val json = """{"matchedTerms":[{"drugName":"ASPIRIN","conceptId":"chembl:CHEMBL25"}]}"""
    val got  = decode[DgidbResponse](json).map(_.matchedTerms.headOption.flatMap(_.conceptId))
    assertEquals(got, Right(Some("chembl:CHEMBL25")))
  }
}
