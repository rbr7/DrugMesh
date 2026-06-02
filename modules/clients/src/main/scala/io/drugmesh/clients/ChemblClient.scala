package io.drugmesh.clients

import cats.effect.IO
import sttp.client4._

/**
 * Cross-maps identifiers through the EBI UniChem REST service. UniChem assigns each source
 * a numeric id (DrugBank=2, ChEMBL=1, ChEBI=7, PubChem=22); a lookup translates a compound
 * id from one source to another. This replaces the original `ChemBL_DrugbankMapper`.
 */
final class ChemblClient(
    base: String,
    backend: Backend[IO],
    userAgent: String,
    policy: RetryPolicy
) extends HttpClientBase(backend, userAgent, policy) {
  import Codecs._

  private def crossMap(srcId: String, fromSrc: Int, toSrc: Int): IO[Either[ApiError, Option[String]]] =
    getJson[List[UniChemHit]](uri"$base/src_compound_id/$srcId/$fromSrc/$toSrc")
      .map(_.map(_.headOption.map(_.src_compound_id)))
      .map(notFoundAsEmpty)

  /** DrugBank id -> ChEMBL id. */
  def chemblForDrugBank(drugbankId: String): IO[Either[ApiError, Option[String]]] =
    crossMap(drugbankId, fromSrc = 2, toSrc = 1)

  /** DrugBank id -> ChEBI id. */
  def chebiForDrugBank(drugbankId: String): IO[Either[ApiError, Option[String]]] =
    crossMap(drugbankId, fromSrc = 2, toSrc = 7)

  /** DrugBank id -> PubChem CID (fallback path when the name lookup misses). */
  def pubchemForDrugBank(drugbankId: String): IO[Either[ApiError, Option[String]]] =
    crossMap(drugbankId, fromSrc = 2, toSrc = 22)
}
