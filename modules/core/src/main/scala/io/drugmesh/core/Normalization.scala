package io.drugmesh.core

import java.text.Normalizer

/**
 * Pure, side-effect-free text/identifier normalization used everywhere downstream:
 * blocking keys for entity resolution, fuzzy-search analyzers, and validity checks for
 * anomaly detection. Keeping these functions pure makes them trivially unit-testable and
 * safe to call inside Spark UDFs.
 */
object Normalization {

  private val SaltSuffixes: Set[String] = Set(
    "hydrochloride", "hcl", "sodium", "potassium", "sulfate", "sulphate", "phosphate",
    "maleate", "mesylate", "besylate", "tartrate", "citrate", "acetate", "fumarate",
    "succinate", "bromide", "chloride", "nitrate", "calcium", "dihydrate", "monohydrate",
    "hydrate", "anhydrous"
  )

  private val Punctuation = """[\p{Punct}]""".r
  private val MultiSpace  = """\s+""".r

  /** Lowercase, strip accents, drop punctuation, collapse whitespace. */
  def normalizeName(raw: String): String = {
    if (raw == null) return ""
    val deAccented = Normalizer
      .normalize(raw, Normalizer.Form.NFKD)
      .replaceAll("\\p{InCombiningDiacriticalMarks}+", "")
    val cleaned = Punctuation.replaceAllIn(deAccented.toLowerCase, " ")
    MultiSpace.replaceAllIn(cleaned, " ").trim
  }

  /** Normalized name with trailing salt/hydrate descriptors removed (so a salt and its
   * parent drug block together). */
  def stripSalts(raw: String): String = {
    val tokens = normalizeName(raw).split(" ").toVector
    val kept   = tokens.filterNot(SaltSuffixes.contains)
    if (kept.isEmpty) tokens.mkString(" ") else kept.mkString(" ")
  }

  /** Coarse blocking key: salt-stripped, despaced, first `n` characters. Used to keep the
   * pairwise comparison in entity resolution from going O(n^2) across the whole table. */
  def blockingKey(raw: String, n: Int = 6): String = {
    val k = stripSalts(raw).replace(" ", "")
    if (k.length <= n) k else k.substring(0, n)
  }

  /** The 14-character skeleton block of an InChIKey (connectivity layer), a strong
   * structural blocking key that is robust to salt/stereochemistry differences. */
  def inchiKeySkeleton(inchiKey: String): Option[String] =
    Option(inchiKey).map(_.trim).filter(_.length >= 14).map(_.substring(0, 14))

  /**
   * Validate a CAS Registry Number via its check digit. Format `N..N-NN-C`; the check
   * digit C equals (sum of each preceding digit times its position, counted from the right
   * starting at 1) mod 10. Used by anomaly detection to flag malformed `cas_num` values.
   */
  def isValidCas(cas: String): Boolean = {
    if (cas == null) return false
    val digits = cas.replaceAll("[^0-9]", "")
    if (digits.length < 5 || digits.length > 10) return false
    val body  = digits.dropRight(1)
    val check = digits.last - '0'
    val sum = body.reverse.zipWithIndex.map { case (ch, i) => (ch - '0') * (i + 1) }.sum
    sum % 10 == check
  }

  /** Levenshtein edit distance (iterative two-row DP). */
  def levenshtein(a: String, b: String): Int = {
    val (s, t) = (a, b)
    if (s == t) return 0
    if (s.isEmpty) return t.length
    if (t.isEmpty) return s.length
    var prev = (0 to t.length).toArray
    var curr = new Array[Int](t.length + 1)
    for (i <- 1 to s.length) {
      curr(0) = i
      for (j <- 1 to t.length) {
        val cost = if (s(i - 1) == t(j - 1)) 0 else 1
        curr(j) = math.min(math.min(curr(j - 1) + 1, prev(j) + 1), prev(j - 1) + cost)
      }
      val tmp = prev; prev = curr; curr = tmp
    }
    prev(t.length)
  }

  /** Jaro similarity in [0,1]. */
  def jaro(s1: String, s2: String): Double = {
    if (s1.isEmpty && s2.isEmpty) return 1.0
    if (s1.isEmpty || s2.isEmpty) return 0.0
    val matchDistance = math.max(s1.length, s2.length) / 2 - 1
    val s1Matches = new Array[Boolean](s1.length)
    val s2Matches = new Array[Boolean](s2.length)
    var matches = 0
    for (i <- s1.indices) {
      val start = math.max(0, i - matchDistance)
      val end   = math.min(i + matchDistance + 1, s2.length)
      var j = start
      var found = false
      while (j < end && !found) {
        if (!s2Matches(j) && s1(i) == s2(j)) {
          s1Matches(i) = true
          s2Matches(j) = true
          matches += 1
          found = true
        }
        j += 1
      }
    }
    if (matches == 0) return 0.0
    var transpositions = 0
    var k = 0
    for (i <- s1.indices if s1Matches(i)) {
      while (!s2Matches(k)) k += 1
      if (s1(i) != s2(k)) transpositions += 1
      k += 1
    }
    val m = matches.toDouble
    ((m / s1.length) + (m / s2.length) + ((m - transpositions / 2.0) / m)) / 3.0
  }

  /** Jaro-Winkler similarity (boosts common-prefix agreement up to 4 chars). */
  def jaroWinkler(s1: String, s2: String, p: Double = 0.1): Double = {
    val j = jaro(s1, s2)
    if (j < 0.7) j
    else {
      val prefix = s1.zip(s2).takeWhile { case (a, b) => a == b }.length.min(4)
      j + prefix * p * (1 - j)
    }
  }

  /** Token-set Jaccard over whitespace tokens of the normalized names. */
  def tokenJaccard(a: String, b: String): Double = {
    val ta = normalizeName(a).split(" ").filter(_.nonEmpty).toSet
    val tb = normalizeName(b).split(" ").filter(_.nonEmpty).toSet
    if (ta.isEmpty && tb.isEmpty) 1.0
    else if (ta.isEmpty || tb.isEmpty) 0.0
    else ta.intersect(tb).size.toDouble / ta.union(tb).size.toDouble
  }
}
