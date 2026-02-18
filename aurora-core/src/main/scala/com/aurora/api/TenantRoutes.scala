package com.aurora.api

import akka.http.scaladsl.server.Directives.*
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.model.StatusCodes
import com.aurora.tenant.{TenantService, TenantContext, TenantConfigService, TenantConfig}
import scala.util.{Try, Success, Failure}
import scala.jdk.CollectionConverters.*

object TenantRoutes {

  // ------------------------
  // Helpers: manual JSON
  // ------------------------
  private def tenantToJson(t: TenantService.Tenant): String =
    s"""{
       |"id":"${t.id}",
       |"tenantId":"${t.tenantId}",
       |"name":"${t.name}",
       |"createdAt":"${t.createdAt}",
       |"updatedAt":"${t.updatedAt}",
       |"isActive":${t.isActive}
       |}""".stripMargin

  private def configToJson(c: TenantConfig): String = {
    val settingsJson = c.settings.map { case (k,v) => s""""$k":"$v"""" }.mkString("{", ",", "}")
    val featuresJson = c.features.map(f => s""""$f"""").mkString("[", ",", "]")
    s"""{
       |"tenantId":"${c.tenantId}",
       |"settings":$settingsJson,
       |"features":$featuresJson
       |}""".stripMargin
  }

  private def singleFeatureJson(feature: String, enabled: Boolean): String =
    s"""{"feature":"$feature","enabled":$enabled}"""

  private def messageJson(msg: String): String =
    s"""{"message":"$msg"}"""

  // ------------------------
  // Routes
  // ------------------------
  def routes: Route = pathPrefix("tenant") {
    concat(

      // GET /tenant - list active tenants
      pathEndOrSingleSlash {
        get {
          TenantService.getActiveTenants() match {
            case Success(list) =>
              val json = list.map(tenantToJson).mkString("[", ",", "]")
              complete(StatusCodes.OK, json)
            case Failure(ex) =>
              complete(StatusCodes.InternalServerError, s"""{"error":"${ex.getMessage}"}""")
          }
        } ~
          // POST /tenant - create tenant
          post {
            entity(as[String]) { body =>
              val nameOpt = """\"name\"\s*:\s*\"([^\"]+)\"""".r.findFirstMatchIn(body).map(_.group(1))
              nameOpt match {
                case Some(name) =>
                  TenantService.createTenant(name) match {
                    case Success(t) => complete(StatusCodes.Created, tenantToJson(t))
                    case Failure(ex) => complete(StatusCodes.BadRequest, s"""{"error":"${ex.getMessage}"}""")
                  }
                case None => complete(StatusCodes.BadRequest, """{"error":"Missing field 'name'"}""")
              }
            }
          }
      },

      // Tenant operations by ID
      pathPrefix(Segment) { tenantId =>
        concat(

          // GET /tenant/{id}
          get {
            TenantContext.withTenant(tenantId) {
              TenantService.getTenantById(tenantId) match {
                case Success(Some(t)) => complete(StatusCodes.OK, tenantToJson(t))
                case Success(None) => complete(StatusCodes.NotFound, s"""{"error":"Tenant $tenantId not found"}""")
                case Failure(ex) => complete(StatusCodes.InternalServerError, s"""{"error":"${ex.getMessage}"}""")
              }
            }
          },

          // PUT /tenant/{id} - update tenant name
          put {
            entity(as[String]) { body =>
              val newNameOpt = """\"name\"\s*:\s*\"([^\"]+)\"""".r.findFirstMatchIn(body).map(_.group(1))
              newNameOpt match {
                case Some(newName) =>
                  TenantContext.withTenant(tenantId) {
                    TenantService.updateTenantName(tenantId, newName) match {
                      case Success(updated) => complete(StatusCodes.OK, tenantToJson(updated))
                      case Failure(ex) => complete(StatusCodes.BadRequest, s"""{"error":"${ex.getMessage}"}""")
                    }
                  }
                case None => complete(StatusCodes.BadRequest, """{"error":"Missing field 'name'"}""")
              }
            }
          },

          // POST /tenant/{id}/deactivate
          path("deactivate") {
            post {
              TenantContext.withTenant(tenantId) {
                TenantService.deactivateTenant(tenantId) match {
                  case Success(t) => complete(StatusCodes.OK, tenantToJson(t))
                  case Failure(ex) => complete(StatusCodes.BadRequest, s"""{"error":"${ex.getMessage}"}""")
                }
              }
            }
          },

          // POST /tenant/{id}/activate
          path("activate") {
            post {
              TenantContext.withTenant(tenantId) {
                TenantService.activateTenant(tenantId) match {
                  case Success(t) => complete(StatusCodes.OK, tenantToJson(t))
                  case Failure(ex) => complete(StatusCodes.BadRequest, s"""{"error":"${ex.getMessage}"}""")
                }
              }
            }
          },

          // GET / PUT /tenant/{id}/config
          path("config") {
            get {
              TenantContext.withTenant(tenantId) {
                TenantConfigService.getConfig(tenantId) match {
                  case Success(c) => complete(StatusCodes.OK, configToJson(c))
                  case Failure(_) => complete(StatusCodes.NotFound, s"""{"error":"Config not found"}""")
                }
              }
            } ~
              put {
                entity(as[String]) { body =>
                  TenantContext.withTenant(tenantId) {
                    // Parse settings/features manually (or just replace empty for now)
                    val updatedConfig = TenantConfig(tenantId) // here you can parse body if needed
                    TenantConfigService.updateConfig(updatedConfig)
                    complete(StatusCodes.OK, configToJson(updatedConfig))
                  }
                }
              }
          },

          // Feature toggle endpoints
          path("feature" / Segment) { featureName =>
            get {
              TenantContext.withTenant(tenantId) {
                val enabled = TenantConfigService.isFeatureEnabled(featureName, tenantId)
                complete(StatusCodes.OK, singleFeatureJson(featureName, enabled))
              }
            } ~
              path("enable") {
                post {
                  TenantContext.withTenant(tenantId) {
                    TenantConfigService.enableFeature(featureName, tenantId)
                    complete(StatusCodes.OK, messageJson(s"Feature '$featureName' enabled for tenant $tenantId"))
                  }
                }
              } ~
              path("disable") {
                post {
                  TenantContext.withTenant(tenantId) {
                    TenantConfigService.disableFeature(featureName, tenantId)
                    complete(StatusCodes.OK, messageJson(s"Feature '$featureName' disabled for tenant $tenantId"))
                  }
                }
              }
          }

        )
      }

    )
  }

}
