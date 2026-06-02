package io.drugmesh.clients

/**
 * Recoverable client failures, modeled as data rather than thrown exceptions so the
 * pipeline can accumulate per-record enrichment errors (one bad API response must not
 * abort the whole batch — a deliberate contrast with the original imperative flow).
 */
sealed trait ApiError extends Product with Serializable {
  def message: String
}

object ApiError {
  final case class Http(status: Int, body: String) extends ApiError {
    def message = s"HTTP $status: ${body.take(200)}"
  }
  final case class Decode(reason: String) extends ApiError {
    def message = s"decode failure: $reason"
  }
  final case class Network(reason: String) extends ApiError {
    def message = s"network failure: $reason"
  }
  final case class RateLimited(retryAfter: Option[Long]) extends ApiError {
    def message = s"rate limited${retryAfter.fold("")(s => s" (retry after ${s}s)")}"
  }
  final case class Auth(reason: String) extends ApiError {
    def message = s"auth failure: $reason"
  }
}
