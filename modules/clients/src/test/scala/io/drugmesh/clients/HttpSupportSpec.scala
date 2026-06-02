package io.drugmesh.clients

import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.unsafe.implicits.global

import scala.concurrent.duration._

class HttpSupportSpec extends munit.FunSuite {

  test("retrying recovers from transient failures and reports the attempt count") {
    val policy = RetryPolicy(maxRetries = 5, baseBackoff = 1.milli, maxBackoff = 5.millis)
    val program = Ref.of[IO, Int](0).flatMap { attempts =>
      val flaky: IO[Either[ApiError, String]] =
        attempts.updateAndGet(_ + 1).map {
          case n if n < 3 => Left(ApiError.Network("connection reset"))
          case _          => Right("ok")
        }
      HttpSupport.retrying(policy)(flaky).flatMap(r => attempts.get.map(r -> _))
    }
    val (result, attempts) = program.unsafeRunSync()
    assertEquals(result, Right("ok"))
    assertEquals(attempts, 3)
  }

  test("retrying fails fast on non-transient errors") {
    val policy = RetryPolicy(maxRetries = 5, baseBackoff = 1.milli)
    val program = Ref.of[IO, Int](0).flatMap { attempts =>
      val decodeErr: IO[Either[ApiError, String]] =
        attempts.update(_ + 1).as(Left(ApiError.Decode("bad json")))
      HttpSupport.retrying(policy)(decodeErr).flatMap(r => attempts.get.map(r -> _))
    }
    val (result, attempts) = program.unsafeRunSync()
    assert(result.isLeft)
    assertEquals(attempts, 1)
  }
}
