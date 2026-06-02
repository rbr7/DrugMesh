package io.drugmesh.clients

import cats.data.EitherT
import cats.effect.IO
import sttp.client4._

/**
 * UMLS Metathesaurus client. Reproduces the CAS ticket-granting auth flow of the original
 * `MetathesaurusAPIticketService`: exchange the API key for a Ticket-Granting Ticket (TGT),
 * mint a single-use Service Ticket (ST) per request, then call the search endpoint.
 *
 * UMLS requires a (free) license + API key; load it from the environment, never commit it.
 * In production the TGT (valid ~8h) should be cached; here it is fetched per call for
 * clarity, with caching left as a documented optimization.
 */
final class UmlsClient(
    base: String,
    authBase: String,
    apiKey: String,
    backend: Backend[IO],
    userAgent: String,
    policy: RetryPolicy
) extends HttpClientBase(backend, userAgent, policy) {
  import Codecs._

  private val TgtActionPattern = "action=\"([^\"]+)\"".r

  /** Step 1: API key -> Ticket-Granting Ticket URL. */
  def getTgt: IO[Either[ApiError, String]] =
    basicRequest
      .post(uri"$authBase/cas/v1/api-key")
      .header("User-Agent", userAgent)
      .body(Map("apikey" -> apiKey))
      .response(asStringAlways)
      .send(backend)
      .attempt
      .map {
        case Left(t) => Left(ApiError.Network(t.getMessage))
        case Right(resp) =>
          if (resp.code.code >= 400) Left(ApiError.Auth(s"TGT request failed (${resp.code.code})"))
          else
            TgtActionPattern
              .findFirstMatchIn(resp.body)
              .map(_.group(1))
              .toRight(ApiError.Auth("no TGT action URL in CAS response"))
      }

  /** Step 2: TGT -> single-use Service Ticket. */
  def getServiceTicket(tgtUrl: String): IO[Either[ApiError, String]] =
    basicRequest
      .post(uri"$tgtUrl")
      .header("User-Agent", userAgent)
      .body(Map("service" -> base))
      .response(asStringAlways)
      .send(backend)
      .attempt
      .map {
        case Left(t) => Left(ApiError.Network(t.getMessage))
        case Right(resp) =>
          if (resp.code.code >= 400) Left(ApiError.Auth(s"service ticket failed (${resp.code.code})"))
          else Right(resp.body.trim)
      }

  /** Step 3: search the Metathesaurus for a term and collect distinct CUIs. */
  def searchCuis(name: String, ticket: String): IO[Either[ApiError, List[String]]] =
    getJson[UmlsSearchResult](uri"$base/search/current?string=$name&ticket=$ticket")
      .map(_.map(_.result.results.map(_.ui).filter(_ != "NONE").distinct))

  /** Full flow for a single name (fetch TGT, mint ST, search). */
  def cuisForName(name: String): IO[Either[ApiError, List[String]]] =
    (for {
      tgt    <- EitherT(getTgt)
      ticket <- EitherT(getServiceTicket(tgt))
      cuis   <- EitherT(searchCuis(name, ticket))
    } yield cuis).value
}
