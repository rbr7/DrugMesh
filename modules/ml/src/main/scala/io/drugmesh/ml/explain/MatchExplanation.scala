package io.drugmesh.ml.explain

import io.drugmesh.ml.entityres.{FieldWeight, MatchResult}

/**
 * Turns a [[MatchResult]] into human-readable, auditable reason strings. Because the
 * Fellegi-Sunter matcher is additive in log-space, each field's bit contribution already IS
 * its explanation — there is no opaque score to reverse-engineer. This is the inherently
 * interpretable "why did these two records link" trail the data-quality use case requires.
 *
 * For the discriminative GBT matcher, where contributions are not analytic, attach SHAP
 * (global + consistent feature attributions) and LIME (local per-prediction) explanations
 * instead — see [[shapNote]] for the intended integration point.
 */
object MatchExplanation {

  private def label(field: String): String = field match {
    case "shared_inchikey"   => "InChIKey skeleton"
    case "shared_cas"        => "CAS number"
    case "shared_pubchem"    => "PubChem CID"
    case "shared_chembl"     => "ChEMBL id"
    case "name_jaro_winkler" => "name (Jaro-Winkler)"
    case "token_jaccard"     => "name tokens (Jaccard)"
    case other               => other
  }

  /** One reason string per field contribution, strongest evidence first. */
  def reasons(r: MatchResult): Seq[String] =
    r.contributions
      .sortBy(c => -math.abs(c.bits))
      .map { case FieldWeight(field, agreed, bits) =>
        val verb = if (agreed) "agreement" else "disagreement"
        f"${label(field)} $verb: $bits%+.1f bits"
      }

  /** A single-line, panel-ready summary of the decision. */
  def summary(r: MatchResult): String =
    f"${r.idA} ~ ${r.idB}: ${r.decision.toUpperCase} " +
      f"(posterior ${r.posterior}%.3f, total ${r.totalBits}%+.1f bits) — " +
      reasons(r).mkString("; ")

  /** Where to plug SHAP/LIME for the GBT matcher (documented integration point). */
  def shapNote: String =
    "For the GBT matcher, compute TreeSHAP values per candidate pair for global feature " +
      "importance, and LIME for intuitive local explanations of individual link decisions."
}
