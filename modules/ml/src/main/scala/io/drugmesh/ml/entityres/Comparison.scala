package io.drugmesh.ml.entityres

import io.drugmesh.core.{DrugEntry, Normalization}

/** A candidate record for entity resolution: a drug entry plus its structural InChIKey. */
final case class ResolutionRecord(entry: DrugEntry, inchiKey: Option[String] = None) {
  def id: String              = entry.drugbankId
  def normalizedName: String  = Normalization.stripSalts(entry.name)
}

/**
 * The comparison-vector for a candidate pair. Each field is a partial-agreement signal the
 * Fellegi-Sunter model turns into a match weight. Booleans capture strong identity evidence
 * (shared CAS / InChIKey); the reals capture graded string agreement.
 */
final case class PairFeatures(
    nameJaroWinkler: Double,
    nameLevenshteinNorm: Double,
    tokenJaccard: Double,
    sharedCas: Boolean,
    sharedInchiKeySkeleton: Boolean,
    sharedPubChem: Boolean,
    sharedChembl: Boolean
)

/**
 * Builds comparison features for a candidate pair. Pure and deterministic so it is unit-
 * testable and safe to broadcast into a Spark UDF.
 */
object Comparison {

  private def bothDefinedEqual(a: Option[String], b: Option[String]): Boolean =
    (a, b) match {
      case (Some(x), Some(y)) => x.nonEmpty && x == y
      case _                  => false
    }

  def features(a: ResolutionRecord, b: ResolutionRecord): PairFeatures = {
    val na = a.normalizedName
    val nb = b.normalizedName
    val lev = Normalization.levenshtein(na, nb)
    val levNorm = if (math.max(na.length, nb.length) == 0) 0.0
                  else 1.0 - lev.toDouble / math.max(na.length, nb.length)
    PairFeatures(
      nameJaroWinkler = Normalization.jaroWinkler(na, nb),
      nameLevenshteinNorm = levNorm,
      tokenJaccard = Normalization.tokenJaccard(a.entry.name, b.entry.name),
      sharedCas = bothDefinedEqual(a.entry.casNum, b.entry.casNum),
      sharedInchiKeySkeleton = bothDefinedEqual(
        a.inchiKey.flatMap(Normalization.inchiKeySkeleton),
        b.inchiKey.flatMap(Normalization.inchiKeySkeleton)
      ),
      sharedPubChem = bothDefinedEqual(a.entry.pubchemCid, b.entry.pubchemCid),
      sharedChembl = bothDefinedEqual(a.entry.chemblId, b.entry.chemblId)
    )
  }

  /** Flatten features into a numeric vector (order is the contract for the ML matcher). */
  def toVector(f: PairFeatures): Array[Double] =
    Array(
      f.nameJaroWinkler,
      f.nameLevenshteinNorm,
      f.tokenJaccard,
      if (f.sharedCas) 1.0 else 0.0,
      if (f.sharedInchiKeySkeleton) 1.0 else 0.0,
      if (f.sharedPubChem) 1.0 else 0.0,
      if (f.sharedChembl) 1.0 else 0.0
    )

  val FeatureNames: Vector[String] =
    Vector(
      "name_jaro_winkler",
      "name_levenshtein_norm",
      "token_jaccard",
      "shared_cas",
      "shared_inchikey",
      "shared_pubchem",
      "shared_chembl"
    )
}
