package io.drugmesh.clients

import cats.effect.IO
import io.circe.{Decoder, parser}
import sttp.client4._
import sttp.model.Uri

/**
 * Shared base for the typed API clients. Performs a GET, maps transport/status/decoding
 * outcomes onto the [[ApiError]] ADT, and wraps the call in the retry/backoff policy.
 *
 * We deliberately read the body as a plain string and decode with circe ourselves rather
 * than leaning on sttp's JSON integration types, which keeps the error mapping explicit and
 * stable across sttp minor versions.
 */
abstract class HttpClientBase(backend: Backend[IO], userAgent: String, policy: RetryPolicy) {

  protected def getJson[T: Decoder](uri: Uri, headers: Map[String, String] = Map.empty): IO[Either[ApiError, T]] =
    HttpSupport.retrying(policy) {
      basicRequest
        .get(uri)
        .header("User-Agent", userAgent)
        .headers(headers)
        .response(asStringAlways)
        .send(backend)
        .attempt
        .map {
          case Left(t) => Left(ApiError.Network(Option(t.getMessage).getOrElse(t.toString)))
          case Right(resp) =>
            val code = resp.code.code
            if (code == 429) Left(ApiError.RateLimited(None))
            else if (code >= 400) Left(ApiError.Http(code, resp.body))
            else parser.decode[T](resp.body).left.map(e => ApiError.Decode(e.getMessage))
        }
    }

  /** Treat a 404 as a legitimate "no match" rather than an error (PubChem, UniChem). */
  protected def notFoundAsEmpty[T](r: Either[ApiError, Option[T]]): Either[ApiError, Option[T]] =
    r match {
      case Left(ApiError.Http(404, _)) => Right(None)
      case other                       => other
    }
}
