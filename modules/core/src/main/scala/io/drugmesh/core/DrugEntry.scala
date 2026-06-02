package io.drugmesh.core

/**
 * Immutable domain record for a single drug, with one optional field per external
 * identifier. This is the Scala replacement for the original 13-field mutable Java
 * `DrugEntry`.
 *
 * Key design changes versus the original:
 *   - `Option[String]` replaces the `"null"` string sentinel the legacy TSV used. The
 *     `"null"` contract is re-applied only at the very edge, in [[DrugEntry.toTsvRow]],
 *     so the emitted file stays byte-compatible with the original `drug-mappings.tsv`.
 *   - `umlsCuis` is a real `Seq[String]` rather than a comma-joined string, because a
 *     drug legitimately maps to several UMLS concepts.
 *   - The class is a `case class`: immutable, structurally comparable, pattern-matchable,
 *     and `Encoder`-derivable so it can flow through Spark `Dataset`s with static typing.
 */
final case class DrugEntry(
    drugbankId: String,
    name: String,
    ttdId: Option[String] = None,
    pubchemCid: Option[String] = None,
    casNum: Option[String] = None,
    chemblId: Option[String] = None,
    zincId: Option[String] = None,
    chebiId: Option[String] = None,
    keggCid: Option[String] = None,
    keggId: Option[String] = None,
    bindingDbId: Option[String] = None,
    umlsCuis: Seq[String] = Seq.empty,
    stitchId: Option[String] = None
) {

  /** Number of external identifiers successfully resolved — a crude completeness score. */
  def filledIdCount: Int =
    Seq(ttdId, pubchemCid, casNum, chemblId, zincId, chebiId, keggCid, keggId, bindingDbId, stitchId)
      .count(_.isDefined) + (if (umlsCuis.nonEmpty) 1 else 0)

  /** Render to the legacy tab-separated row contract (missing values become "null"). */
  def toTsvRow: String = {
    def s(o: Option[String]): String = o.filter(_.nonEmpty).getOrElse(DrugEntry.NullToken)
    val cuis = if (umlsCuis.isEmpty) DrugEntry.NullToken else umlsCuis.mkString(",")
    Seq(
      drugbankId,
      name,
      s(ttdId),
      s(pubchemCid),
      s(casNum),
      s(chemblId),
      s(zincId),
      s(chebiId),
      s(keggCid),
      s(keggId),
      s(bindingDbId),
      cuis,
      s(stitchId)
    ).mkString("\t")
  }
}

object DrugEntry {

  /** Sentinel the legacy TSV uses for an absent identifier. */
  val NullToken = "null"

  /** Column order — the public contract of the output file. Do not reorder. */
  val Columns: Vector[String] = Vector(
    "drugbankId",
    "name",
    "ttd_id",
    "pubchem_cid",
    "cas_num",
    "chembl_id",
    "zinc_id",
    "chebi_id",
    "kegg_cid",
    "kegg_id",
    "bindingDB_id",
    "UMLS_cuis",
    "stitch_id"
  )

  val Header: String = Columns.mkString("\t")

  // Note: Spark `Encoder[DrugEntry]` is derived at use sites via `import spark.implicits._`
  // (DrugEntry is a case class). Keeping it out of the companion avoids implicit ambiguity
  // and keeps `core` independent of the Spark runtime.

  private def opt(raw: String): Option[String] =
    Option(raw).map(_.trim).filter(v => v.nonEmpty && v != NullToken)

  /** Parse a single legacy TSV row back into a typed entry (inverse of [[DrugEntry.toTsvRow]]). */
  def fromTsvRow(line: String): Either[String, DrugEntry] = {
    val f = line.split("\t", -1) // -1 keeps trailing empty fields
    if (f.length != Columns.length)
      Left(s"expected ${Columns.length} columns, got ${f.length}: '${line.take(80)}'")
    else
      Right(
        DrugEntry(
          drugbankId = f(0).trim,
          name = f(1).trim,
          ttdId = opt(f(2)),
          pubchemCid = opt(f(3)),
          casNum = opt(f(4)),
          chemblId = opt(f(5)),
          zincId = opt(f(6)),
          chebiId = opt(f(7)),
          keggCid = opt(f(8)),
          keggId = opt(f(9)),
          bindingDbId = opt(f(10)),
          umlsCuis = opt(f(11)).map(_.split(",").toIndexedSeq.map(_.trim).filter(_.nonEmpty)).getOrElse(Seq.empty),
          stitchId = opt(f(12))
        )
      )
  }
}
