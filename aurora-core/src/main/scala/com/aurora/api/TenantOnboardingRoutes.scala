package com.aurora.api

import akka.http.scaladsl.server.Directives.*
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.model.StatusCodes
import com.aurora.tenant.{TenantOnboardingService, TenantService, OnboardingResult}
import scala.concurrent.ExecutionContext
import scala.util.{Success, Failure}
import scala.concurrent.Future

/**
 * REST API for automated tenant onboarding
 */
class TenantOnboardingRoutes(implicit ec: ExecutionContext) {

  // ===== JSON Helpers (following TenantConfigRoutes pattern) =====
  private def escapeJson(s: String): String = {
    s.replace("\\", "\\\\").replace("\"", "\\\"")
  }

  private def messageJson(msg: String): String = s"""{"message":"${escapeJson(msg)}"}"""
  private def errorJson(msg: String): String = s"""{"error":"${escapeJson(msg)}"}"""
  private def successJson(msg: String): String = s"""{"success":true,"message":"${escapeJson(msg)}"}"""

  private def onboardingResultToJson(result: OnboardingResult): String = {
    val tenantJson = result.tenant.map { t =>
      s"""
         |"tenant":{
         |  "id":"${escapeJson(t.id)}",
         |  "tenantId":"${escapeJson(t.tenantId)}",
         |  "name":"${escapeJson(t.name)}"
         |}""".stripMargin
    }.getOrElse("")

    val configJson = result.config.map { c =>
      s"""
         |"config":{
         |  "tenantId":"${escapeJson(c.tenantId)}",
         |  "version":${c.version}
         |}""".stripMargin
    }.getOrElse("")

    val limitsJson = result.limits.map { l =>
      s"""
         |"limits":{
         |  "tenantId":"${escapeJson(l.tenantId)}",
         |  "version":${l.version}
         |}""".stripMargin
    }.getOrElse("")

    val errorJson = result.error.map(e => s""","error":"${escapeJson(e)}"""").getOrElse("")

    s"""{
       |"success":${result.success},
       |"tier":"${escapeJson(result.tier)}",
       |"dedicated":${result.dedicated},
       |"durationMs":${result.durationMs}
       |${if (tenantJson.nonEmpty) "," + tenantJson else ""}
       |${if (configJson.nonEmpty) "," + configJson else ""}
       |${if (limitsJson.nonEmpty) "," + limitsJson else ""}
       |${errorJson}
       |}""".stripMargin
  }

  private def tenantSummaryToJson(tenantId: String, name: String, tier: String, dedicated: Boolean, durationMs: Long): String = {
    s"""{
       |"tenantId":"${escapeJson(tenantId)}",
       |"name":"${escapeJson(name)}",
       |"tier":"${escapeJson(tier)}",
       |"dedicated":$dedicated,
       |"durationMs":$durationMs
       |}""".stripMargin
  }

  private def errorSummaryToJson(error: String, tier: String): String = {
    s"""{
       |"error":"${escapeJson(error)}",
       |"tier":"${escapeJson(tier)}"
       |}""".stripMargin
  }

  // ===== Request Case Classes =====
  case class OnboardTenantRequest(
                                   name: String,
                                   tier: String = "free",
                                   enterprise: Boolean = false,
                                   compliance: Boolean = false,
                                   customDomain: Boolean = false,
                                   expectedUsers: Int = 0,
                                   webhookUrl: Option[String] = None
                                 )

  case class BatchOnboardRequest(
                                  tenants: List[OnboardTenantRequest]
                                )

  // ===== Request Body Parsers (following TenantConfigRoutes pattern) =====
  private def parseOnboardRequest(body: String): Option[OnboardTenantRequest] = {
    try {
      val name = "\"name\"\\s*:\\s*\"([^\"]+)\"".r.findFirstMatchIn(body).map(_.group(1))
      val tier = "\"tier\"\\s*:\\s*\"([^\"]+)\"".r.findFirstMatchIn(body).map(_.group(1)).getOrElse("free")
      val enterprise = "\"enterprise\"\\s*:\\s*(true|false)".r.findFirstMatchIn(body).exists(_.group(1) == "true")
      val compliance = "\"compliance\"\\s*:\\s*(true|false)".r.findFirstMatchIn(body).exists(_.group(1) == "true")
      val customDomain = "\"customDomain\"\\s*:\\s*(true|false)".r.findFirstMatchIn(body).exists(_.group(1) == "true")
      val expectedUsers = "\"expectedUsers\"\\s*:\\s*(\\d+)".r.findFirstMatchIn(body).map(_.group(1).toInt).getOrElse(0)
      val webhookUrl = "\"webhookUrl\"\\s*:\\s*\"([^\"]+)\"".r.findFirstMatchIn(body).map(_.group(1))

      name.map { n =>
        OnboardTenantRequest(n, tier, enterprise, compliance, customDomain, expectedUsers, webhookUrl)
      }
    } catch {
      case e: Exception => None
    }
  }

  private def parseBatchRequest(body: String): Option[BatchOnboardRequest] = {
    // Simplified - for batch we'll just handle one tenant for now
    parseOnboardRequest(body).map { req =>
      BatchOnboardRequest(List(req))
    }
  }

  // ===== Routes =====
  val routes: Route = pathPrefix("api" / "v1" / "onboarding") {
    concat(
      // Single tenant onboarding
      pathEnd {
        post {
          entity(as[String]) { body =>
            parseOnboardRequest(body) match {
              case Some(request) =>
                val requirements = TenantService.TenantRequirements(
                  enterprise = request.enterprise,
                  compliance = request.compliance,
                  customDomain = request.customDomain,
                  expectedUsers = request.expectedUsers
                )

                val result = TenantOnboardingService.onboardTenant(
                  name = request.name,
                  tier = request.tier,
                  requirements = requirements,
                  webhookUrl = request.webhookUrl
                )

                onComplete(result) {
                  case Success(onboardingResult) =>
                    if (onboardingResult.success) {
                      complete(StatusCodes.Created, onboardingResultToJson(onboardingResult))
                    } else {
                      complete(StatusCodes.BadRequest, errorJson(
                        onboardingResult.error.getOrElse("Unknown error")
                      ))
                    }
                  case Failure(ex) =>
                    complete(StatusCodes.InternalServerError, errorJson(s"Onboarding failed: ${ex.getMessage}"))
                }

              case None =>
                complete(StatusCodes.BadRequest, errorJson("Invalid request format. Required field: 'name'"))
            }
          }
        }
      },

      // Batch tenant onboarding
      path("batch") {
        post {
          entity(as[String]) { body =>
            parseBatchRequest(body) match {
              case Some(request) =>
                val futures = request.tenants.map { r =>
                  val requirements = TenantService.TenantRequirements(
                    enterprise = r.enterprise,
                    compliance = r.compliance,
                    customDomain = r.customDomain,
                    expectedUsers = r.expectedUsers
                  )
                  TenantOnboardingService.onboardTenant(
                    name = r.name,
                    tier = r.tier,
                    requirements = requirements,
                    webhookUrl = r.webhookUrl
                  )
                }

                val results = Future.sequence(futures)

                onComplete(results) {
                  case Success(onboardingResults) =>
                    val successful = onboardingResults.count(_.success)
                    val failed = onboardingResults.count(!_.success)

                    val resultsJson = onboardingResults.map { r =>
                      if (r.success) {
                        s"""{"success":true,"tenantId":"${escapeJson(r.tenant.map(_.tenantId).getOrElse(""))}","tier":"${escapeJson(r.tier)}","dedicated":${r.dedicated},"durationMs":${r.durationMs}}"""
                      } else {
                        s"""{"success":false,"error":"${escapeJson(r.error.getOrElse("Unknown error"))}","tier":"${escapeJson(r.tier)}"}"""
                      }
                    }.mkString("[", ",", "]")

                    val response = s"""{
                                      |"total":${onboardingResults.size},
                                      |"successful":$successful,
                                      |"failed":$failed,
                                      |"results":$resultsJson
                                      |}""".stripMargin

                    complete(StatusCodes.OK, response)

                  case Failure(ex) =>
                    complete(StatusCodes.InternalServerError, errorJson(s"Batch onboarding failed: ${ex.getMessage}"))
                }

              case None =>
                complete(StatusCodes.BadRequest, errorJson("Invalid batch request format"))
            }
          }
        }
      },

      // Get available templates
      path("templates") {
        get {
          val templates = TenantOnboardingService.templates.keys.toList.sorted
          val templatesJson = templates.map(t => s""""$t"""").mkString("[", ",", "]")
          val response = s"""{"templates":$templatesJson,"default":"free"}"""
          complete(StatusCodes.OK, response)
        }
      },

      // Health check
      path("health") {
        get {
          val response = s"""{"status":"healthy","service":"tenant-onboarding","templates":${TenantOnboardingService.templates.size}}"""
          complete(StatusCodes.OK, response)
        }
      }
    )
  }
}