package io.drugmesh.ml.entityres

import io.drugmesh.core.DrugEntry
import io.drugmesh.ml.explain.MatchExplanation

class ComparisonSpec extends munit.FunSuite {

  private def rec(id: String, name: String, cas: Option[String] = None, inchi: Option[String] = None) =
    ResolutionRecord(DrugEntry(id, name, casNum = cas), inchi)

  test("a shared CAS number drives a confident, explainable match") {
    val a = rec("DB1", "Acetaminophen", cas = Some("103-90-2"))
    val b = rec("DB2", "Paracetamol", cas = Some("103-90-2"))
    val score = FellegiSunter.default.score(Comparison.features(a, b))
    assert(score.posterior > 0.99, s"expected high posterior, got ${score.posterior}")
    assert(score.contributions.exists(c => c.field == "shared_cas" && c.agreed && c.bits > 0))
  }

  test("unrelated drugs with different names and ids do not match") {
    val a = rec("DB1", "Aspirin", cas = Some("50-78-2"))
    val b = rec("DB2", "Metformin", cas = Some("657-24-9"))
    val score = FellegiSunter.default.score(Comparison.features(a, b))
    assert(score.posterior < 0.5, s"expected low posterior, got ${score.posterior}")
  }

  test("near-identical names without shared ids land in the review band, not auto-match") {
    val a = rec("DB1", "Fluoxetine")
    val b = rec("DB2", "Fluoxetin")
    val f = Comparison.features(a, b)
    assert(f.nameJaroWinkler > 0.9)
    val score = FellegiSunter.default.score(f)
    assert(score.posterior > 0.0 && score.posterior < 0.999)
  }

  test("explanation summary decomposes the decision into per-field bits") {
    val a = rec("DB1", "Ibuprofen", cas = Some("15687-27-1"))
    val b = rec("DB2", "Ibuprofen", cas = Some("15687-27-1"))
    val score = FellegiSunter.default.score(Comparison.features(a, b))
    val result = MatchResult("DB1", "DB2", score.posterior, score.totalBits, "match", score.contributions)
    val text = MatchExplanation.summary(result)
    assert(text.contains("CAS number"))
    assert(text.contains("bits"))
  }
}
