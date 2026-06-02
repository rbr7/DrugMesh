package io.drugmesh.clients

import cats.effect.IO
import sttp.client4._

/**
 * PubChem PUG-REST client: resolves a drug name to a PubChem Compound id (CID).
 * Replaces the original `PubChemDrugExtractor` / `PubChem_DrugbankMapper`. PubChem returns
 * 404 when a name has no compound, which we surface as `Right(None)`.
 */
final class PubChemClient(
    base: String,
    backend: Backend[IO],
    userAgent: String,
    policy: RetryPolicy
) extends HttpClientBase(backend, userAgent, policy) {
  import Codecs._

  /** Drug name -> first PubChem CID, if any. The `uri` interpolator percent-encodes the
   * interpolated name as a single path segment, so no manual encoding is needed. */
  def cidForName(name: String): IO[Either[ApiError, Option[String]]] =
    getJson[PubChemCidList](uri"$base/compound/name/$name/cids/JSON")
      .map(_.map(_.IdentifierList.CID.headOption.map(_.toString)))
      .map(notFoundAsEmpty)
}
