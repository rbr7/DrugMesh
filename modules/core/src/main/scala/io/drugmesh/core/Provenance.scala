package io.drugmesh.core

/**
 * Per-field audit trail: which source/method supplied a value and with what confidence.
 * This is the data substrate for the "explainable AI / auditable reasoning" requirement —
 * every identifier in the final table can point back to how it was obtained.
 */
final case class FieldProvenance(
    field: String,
    source: SourceDb,
    method: String,
    confidence: Double,
    evidence: String
)

object FieldProvenance {
  def apply(field: String, source: SourceDb, method: String, confidence: Double): FieldProvenance =
    FieldProvenance(field, source, method, confidence, evidence = "")
}

/**
 * A [[DrugEntry]] augmented with its provenance trail, an optional anomaly score, and a
 * human-readable reason string. Carried through the pipeline alongside the plain entry so
 * the final output can expose record-level recommendations.
 */
final case class EnrichedDrugEntry(
    entry: DrugEntry,
    provenance: Seq[FieldProvenance] = Seq.empty,
    anomalyScore: Option[Double] = None,
    reasons: Seq[String] = Seq.empty
) {
  def withProvenance(p: FieldProvenance): EnrichedDrugEntry =
    copy(provenance = provenance :+ p)

  def withReason(r: String): EnrichedDrugEntry =
    copy(reasons = reasons :+ r)
}
