package io.drugmesh.clients

import cats.effect.{IO, Resource, Temporal}
import cats.effect.std.Semaphore
import cats.syntax.all._
import sttp.client4.Backend
import sttp.client4.httpclient.cats.HttpClientCatsBackend

import scala.concurrent.duration._

/** Retry/backoff + rate-limiting policy shared by all API clients. */
final case class RetryPolicy(
    maxRetries: Int = 4,
    baseBackoff: FiniteDuration = 500.millis,
    maxBackoff: FiniteDuration = 30.seconds
)

object HttpSupport {

  /** A cats-effect sttp backend as a managed resource (closed deterministically). */
  def backend: Resource[IO, Backend[IO]] =
    HttpClientCatsBackend.resource[IO]()

  /**
   * Retry an effect on transient [[ApiError]]s with exponential backoff and full jitter.
   * Non-transient errors (decode, auth, 4xx other than 429) fail fast.
   */
  def retrying[A](policy: RetryPolicy)(fa: IO[Either[ApiError, A]]): IO[Either[ApiError, A]] = {
    def loop(attempt: Int): IO[Either[ApiError, A]] =
      fa.flatMap {
        case Right(a)                          => IO.pure(Right(a))
        case Left(err) if !transient(err)      => IO.pure(Left(err))
        case Left(err) if attempt >= policy.maxRetries => IO.pure(Left(err))
        case Left(_) =>
          val exp     = policy.baseBackoff * math.pow(2, attempt.toDouble).toLong
          val capped  = exp.min(policy.maxBackoff)
          val jitter  = (capped.toMillis * scala.util.Random.nextDouble()).toLong.millis
          Temporal[IO].sleep(jitter) *> loop(attempt + 1)
      }
    loop(0)
  }

  private def transient(err: ApiError): Boolean = err match {
    case _: ApiError.Network     => true
    case _: ApiError.RateLimited => true
    case ApiError.Http(status, _) => status >= 500
    case _                       => false
  }

  /**
   * A simple concurrency/rate limiter: at most `permits` in-flight requests, each holding
   * its permit for at least `minInterval` so the aggregate request rate stays bounded.
   */
  def rateLimiter(permits: Int, minInterval: FiniteDuration): IO[IO[Unit] => IO[Unit]] =
    Semaphore[IO](permits.toLong).map { sem => (task: IO[Unit]) =>
      sem.permit.use(_ => task <* Temporal[IO].sleep(minInterval))
    }
}
