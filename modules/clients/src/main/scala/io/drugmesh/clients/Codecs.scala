package io.drugmesh.clients

import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder

/**
 * Circe decoders for the external API payloads. Field names mirror the JSON keys exactly
 * (including the upstream snake_case / PascalCase) so derivation works without remapping.
 */
object Codecs {

  // --- UniChem (ChEMBL/ChEBI/PubChem cross-mapping) -------------------------------------
  final case class UniChemHit(src_compound_id: String)
  implicit val uniChemHitDecoder: Decoder[UniChemHit] = deriveDecoder

  // --- PubChem PUG REST -----------------------------------------------------------------
  final case class PubChemIds(CID: List[Long])
  final case class PubChemCidList(IdentifierList: PubChemIds)
  implicit val pubChemIdsDecoder: Decoder[PubChemIds]      = deriveDecoder
  implicit val pubChemCidListDecoder: Decoder[PubChemCidList] = deriveDecoder

  // --- UMLS Metathesaurus search --------------------------------------------------------
  final case class UmlsConcept(ui: String, name: String, rootSource: Option[String])
  final case class UmlsResultBody(results: List[UmlsConcept])
  final case class UmlsSearchResult(result: UmlsResultBody)
  implicit val umlsConceptDecoder: Decoder[UmlsConcept]       = deriveDecoder
  implicit val umlsResultBodyDecoder: Decoder[UmlsResultBody] = deriveDecoder
  implicit val umlsSearchResultDecoder: Decoder[UmlsSearchResult] = deriveDecoder

  // --- DGIdb v2 -------------------------------------------------------------------------
  final case class DgidbTerm(drugName: String, conceptId: Option[String])
  final case class DgidbResponse(matchedTerms: List[DgidbTerm])
  implicit val dgidbTermDecoder: Decoder[DgidbTerm]         = deriveDecoder
  implicit val dgidbResponseDecoder: Decoder[DgidbResponse] = deriveDecoder
}
