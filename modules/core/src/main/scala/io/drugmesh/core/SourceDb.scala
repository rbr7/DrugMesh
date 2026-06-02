package io.drugmesh.core

/**
 * The reference databases DrugMesh reconciles. Each carries the canonical name of the
 * identifier column it contributes to the unified mapping table. Modeling the sources as
 * a sealed ADT (instead of bare strings, as the original Java did) lets the compiler check
 * that every enrichment pass targets a known source and column.
 */
sealed abstract class SourceDb(val key: String, val idColumn: String) extends Product with Serializable

object SourceDb {
  case object DrugBank  extends SourceDb("drugbank", "drugbankId")
  case object TTD       extends SourceDb("ttd", "ttd_id")
  case object PubChem   extends SourceDb("pubchem", "pubchem_cid")
  case object ChEMBL    extends SourceDb("chembl", "chembl_id")
  case object ZINC      extends SourceDb("zinc", "zinc_id")
  case object ChEBI     extends SourceDb("chebi", "chebi_id")
  case object KEGG      extends SourceDb("kegg", "kegg_id")
  case object BindingDB extends SourceDb("bindingdb", "bindingDB_id")
  case object UMLS      extends SourceDb("umls", "UMLS_cuis")
  case object STITCH    extends SourceDb("stitch", "stitch_id")
  case object CAS       extends SourceDb("cas", "cas_num")

  val all: Vector[SourceDb] =
    Vector(DrugBank, TTD, PubChem, ChEMBL, ZINC, ChEBI, KEGG, BindingDB, UMLS, STITCH, CAS)

  def fromKey(k: String): Option[SourceDb] = all.find(_.key == k.toLowerCase)
}
