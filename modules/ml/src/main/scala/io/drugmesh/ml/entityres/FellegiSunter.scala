package io.drugmesh.ml.entityres

/** Per-field contribution to a match decision, in bits (log2 likelihood ratio). */
final case class FieldWeight(field: String, agreed: Boolean, bits: Double)

/** The outcome of scoring a candidate pair: total match weight, posterior, and the
 * decomposition that makes the decision auditable. */
final case class MatchScore(
    totalBits: Double,
    posterior: Double,
    contributions: Seq[FieldWeight]
)

/**
 * A Fellegi-Sunter probabilistic record-linkage model. For each comparison field it holds an
 * `m` probability (P(agree | the pair is a true match)) and a `u` probability
 * (P(agree | not a match)). The match weight of an agreeing field is log2(m/u); of a
 * disagreeing field, log2((1-m)/(1-u)). Summing the field weights and adding the prior gives
 * a total in bits, converted to a posterior with the logistic function.
 *
 * Crucially, the per-field `bits` ARE the explanation — "matched on CAS: +9.1 bits; name
 * Jaro-Winkler agreement: +5.0 bits" — which is exactly the auditable, confidence-scored
 * reasoning the data-quality use case requires. No post-hoc explainer needed.
 */
final case class FellegiSunter(
    fields: Seq[FsField],
    lambdaPrior: Double = 1e-4
) {

  private val priorBits: Double = log2(lambdaPrior / (1.0 - lambdaPrior))

  def score(f: PairFeatures): MatchScore = {
    val agreements: Seq[(String, Boolean)] = Seq(
      "name_jaro_winkler" -> (f.nameJaroWinkler >= 0.92),
      "token_jaccard"     -> (f.tokenJaccard >= 0.6),
      "shared_cas"        -> f.sharedCas,
      "shared_inchikey"   -> f.sharedInchiKeySkeleton,
      "shared_pubchem"    -> f.sharedPubChem,
      "shared_chembl"     -> f.sharedChembl
    )
    val byName = fields.map(fl => fl.name -> fl).toMap
    val contributions = agreements.flatMap { case (name, agreed) =>
      byName.get(name).map { fl =>
        val bits = if (agreed) log2(fl.m / fl.u) else log2((1 - fl.m) / (1 - fl.u))
        FieldWeight(name, agreed, bits)
      }
    }
    val total     = priorBits + contributions.map(_.bits).sum
    val posterior = 1.0 / (1.0 + math.pow(2.0, -total))
    MatchScore(total, posterior, contributions)
  }

  private def log2(x: Double): Double = math.log(x) / math.log(2.0)
}

/** A comparison field with its trained m/u probabilities. */
final case class FsField(name: String, m: Double, u: Double)

object FellegiSunter {

  /**
   * A sensible default model. In production the m/u values are learned with EM on unlabeled
   * pairs (Splink-style) or estimated from the Snorkel-labeled set; these priors encode the
   * obvious domain facts: a shared CAS or InChIKey is overwhelming evidence; name agreement
   * is supportive but weaker.
   */
  val default: FellegiSunter = FellegiSunter(
    fields = Seq(
      FsField("shared_inchikey", m = 0.99, u = 0.000001),
      FsField("shared_cas", m = 0.95, u = 0.00001),
      FsField("shared_pubchem", m = 0.97, u = 0.00005),
      FsField("shared_chembl", m = 0.97, u = 0.00005),
      FsField("name_jaro_winkler", m = 0.90, u = 0.02),
      FsField("token_jaccard", m = 0.85, u = 0.03)
    )
  )
}
