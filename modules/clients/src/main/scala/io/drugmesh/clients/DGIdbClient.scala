package io.drugmesh.clients

import cats.effect.IO
import sttp.client4._

/**
 * DGIdb (Drug-Gene Interaction database) client, used as an independent cross-check: it
 * resolves a drug name to a normalized concept id that corroborates ChEMBL/DrugBank links.
 * Replaces the DGIdb path in the original `CreateDrugMappings.enrichFromDGIdb`.
 */
final class DGIdbClient(
    base: String,
    backend: Backend[IO],
    userAgent: String,
    policy: RetryPolicy
) extends HttpClientBase(backend, userAgent, policy) {
  import Codecs._

  /** Drug name -> DGIdb normalized concept id, if the term is recognized. */
  def conceptForName(name: String): IO[Either[ApiError, Option[String]]] =
    getJson[DgidbResponse](uri"$base/interactions.json?drugs=$name")
      .map(_.map(_.matchedTerms.headOption.flatMap(_.conceptId)))
      .map(notFoundAsEmpty)
}

/** Bundle of all enrichment clients, constructed from config against a shared backend. */
final case class ApiClients(
    chembl: ChemblClient,
    pubchem: PubChemClient,
    umls: UmlsClient,
    dgidb: DGIdbClient
)
