package com.aurora.api

import akka.http.scaladsl.server.Directives.*
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.model.StatusCodes

import com.aurora.tenant.{TenantConfig, TenantConfigService, TenantConfigContext}
import com.aurora.tenant.TenantContext
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Success, Failure}
import scala.jdk.CollectionConverters.*

/**
 * HTTP routes for tenant configuration management.
 *
 * Uses ONLY methods that exist in TenantConfigService:
 * - getConfig
 * - updateConfig
 * - enableFeature/disableFeature/isFeatureEnabled
 * - getEnabledFeatures/getAllFeatures
 * - getSetting/setSetting/deleteSetting
 * - getSettingsByPrefix/getAllSettings
 * - bulkUpdateSettings/bulkUpdateFeatures
 * - resetToDefaults
 * - healthCheck
 * - clearCache/configExists
 */
object TenantConfigRoutes {

  // ===== SLF4J Logger (NO MACROS) =====
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  // ===== JSON Helpers =====
  private def escapeJson(s: String): String = {
    s.replace("\\", "\\\\").replace("\"", "\\\"")
  }

  private def configToJson(c: TenantConfig): String = {
    val settingsJson = if (c.settings.isEmpty) "{}" else {
      c.settings.map { case (k, v) =>
        s""""${escapeJson(k)}":"${escapeJson(v)}""""
      }.mkString("{", ",", "}")
    }

    val featuresJson = if (c.features.isEmpty) "{}" else {
      c.features.map { case (k, v) =>
        s""""${escapeJson(k)}":$v"""
      }.mkString("{", ",", "}")
    }

    s"""{
       |"tenantId":"${escapeJson(c.tenantId)}",
       |"settings":$settingsJson,
       |"features":$featuresJson,
       |"version":${c.version},
       |"updatedAt":"${c.updatedAt}",
       |"createdAt":"${c.createdAt}"
       |}""".stripMargin
  }

  private def messageJson(msg: String): String = s"""{"message":"${escapeJson(msg)}"}"""
  private def errorJson(msg: String): String = s"""{"error":"${escapeJson(msg)}"}"""
  private def successJson(msg: String): String = s"""{"success":true,"message":"${escapeJson(msg)}"}"""

  private def featureToJson(feature: String, enabled: Boolean): String =
    s"""{"feature":"${escapeJson(feature)}","enabled":$enabled}"""

  private def featuresToJson(features: Map[String, Boolean]): String = {
    if (features.isEmpty) "{}" else {
      features.map { case (k, v) => s""""${escapeJson(k)}":$v""" }.mkString("{", ",", "}")
    }
  }

  private def settingsToJson(settings: Map[String, String]): String = {
    if (settings.isEmpty) "{}" else {
      settings.map { case (k, v) => s""""${escapeJson(k)}":"${escapeJson(v)}""" }.mkString("{", ",", "}")
    }
  }

  // ===== Request Body Parsers =====
  private def extractSettingValue(body: String): Option[String] = {
    val pattern = "\"value\"\\s*:\\s*\"([^\"]*)\"".r
    pattern.findFirstMatchIn(body).map(_.group(1))
  }

  private def extractSettingsMap(body: String): Option[Map[String, String]] = {
    try {
      val map = scala.collection.mutable.Map[String, String]()
      val pattern = "\"([^\"]+)\"\\s*:\\s*\"([^\"]*)\"".r
      pattern.findAllMatchIn(body).foreach { m =>
        map(m.group(1)) = m.group(2)
      }
      if (map.isEmpty) None else Some(map.toMap)
    } catch {
      case e: Exception =>
        if (logger.isDebugEnabled) logger.debug("Failed to parse settings: {}", e.getMessage)
        None
    }
  }

  private def extractFeaturesMap(body: String): Option[Map[String, Boolean]] = {
    try {
      val map = scala.collection.mutable.Map[String, Boolean]()
      val pattern = "\"([^\"]+)\"\\s*:\\s*(true|false)".r
      pattern.findAllMatchIn(body).foreach { m =>
        map(m.group(1)) = m.group(2).toBoolean
      }
      if (map.isEmpty) None else Some(map.toMap)
    } catch {
      case e: Exception =>
        if (logger.isDebugEnabled) logger.debug("Failed to parse features: {}", e.getMessage)
        None
    }
  }

  // ===== Routes =====
  val routes: Route = pathPrefix("api" / "v1" / "config") {
    concat(

      // ===== Config Management =====

      // GET /api/v1/config - Get current tenant's config
      pathEndOrSingleSlash {
        get {
          TenantContext.getCurrentTenantId match {
            case tenantId =>
              TenantConfigService.getConfig(tenantId) match {
                case Success(config) =>
                  TenantConfigContext.cacheConfig(tenantId, config)
                  complete(StatusCodes.OK, configToJson(config))
                case Failure(e) =>
                  complete(StatusCodes.NotFound, errorJson(e.getMessage))
              }
          }
        }
      },

      // ===== Settings Management =====

      // GET /api/v1/config/settings - Get all settings
      path("settings") {
        get {
          TenantContext.getCurrentTenantId match {
            case tenantId =>
              TenantConfigService.getAllSettings(tenantId) match {
                case Success(settings) =>
                  complete(StatusCodes.OK, settingsToJson(settings))
                case Failure(e) =>
                  complete(StatusCodes.InternalServerError, errorJson(e.getMessage))
              }
          }
        }
      },

      // GET /api/v1/config/settings/{key} - Get specific setting
      path("settings" / Segment) { key =>
        get {
          TenantContext.getCurrentTenantId match {
            case tenantId =>
              TenantConfigService.getSetting(tenantId, key) match {
                case Success(Some(value)) =>
                  complete(StatusCodes.OK, s"""{"key":"$key","value":"${escapeJson(value)}"}""")
                case Success(None) =>
                  complete(StatusCodes.NotFound, errorJson(s"Setting '$key' not found"))
                case Failure(e) =>
                  complete(StatusCodes.InternalServerError, errorJson(e.getMessage))
              }
          }
        } ~
          put {
            entity(as[String]) { body =>
              TenantContext.getCurrentTenantId match {
                case tenantId =>
                  extractSettingValue(body) match {
                    case Some(value) =>
                      TenantConfigService.setSetting(tenantId, key, value) match {
                        case Success(config) =>
                          TenantConfigContext.cacheConfig(tenantId, config)
                          complete(StatusCodes.OK, successJson(s"Setting '$key' updated"))
                        case Failure(e) =>
                          complete(StatusCodes.BadRequest, errorJson(e.getMessage))
                      }
                    case None =>
                      complete(StatusCodes.BadRequest, errorJson("Missing 'value' field"))
                  }
              }
            }
          } ~
          delete {
            TenantContext.getCurrentTenantId match {
              case tenantId =>
                TenantConfigService.deleteSetting(tenantId, key) match {
                  case Success(config) =>
                    TenantConfigContext.cacheConfig(tenantId, config)
                    complete(StatusCodes.OK, successJson(s"Setting '$key' deleted"))
                  case Failure(e) =>
                    complete(StatusCodes.BadRequest, errorJson(e.getMessage))
                }
            }
          }
      },

      // GET /api/v1/config/settings/by-prefix/{prefix} - Get settings by prefix
      path("settings" / "by-prefix" / Segment) { prefix =>
        get {
          TenantContext.getCurrentTenantId match {
            case tenantId =>
              TenantConfigService.getSettingsByPrefix(tenantId, prefix) match {
                case Success(settings) =>
                  complete(StatusCodes.OK, settingsToJson(settings))
                case Failure(e) =>
                  complete(StatusCodes.InternalServerError, errorJson(e.getMessage))
              }
          }
        }
      },

      // POST /api/v1/config/settings/bulk - Bulk update settings
      path("settings" / "bulk") {
        post {
          entity(as[String]) { body =>
            TenantContext.getCurrentTenantId match {
              case tenantId =>
                extractSettingsMap(body) match {
                  case Some(settings) =>
                    TenantConfigService.bulkUpdateSettings(tenantId, settings) match {
                      case Success(config) =>
                        TenantConfigContext.cacheConfig(tenantId, config)
                        complete(StatusCodes.OK, successJson(s"${settings.size} settings updated"))
                      case Failure(e) =>
                        complete(StatusCodes.BadRequest, errorJson(e.getMessage))
                    }
                  case None =>
                    complete(StatusCodes.BadRequest, errorJson("Invalid settings format"))
                }
            }
          }
        }
      },

      // ===== Feature Toggle Management =====

      // GET /api/v1/config/features - Get all features
      path("features") {
        get {
          TenantContext.getCurrentTenantId match {
            case tenantId =>
              TenantConfigService.getAllFeatures(tenantId) match {
                case Success(features) =>
                  complete(StatusCodes.OK, featuresToJson(features))
                case Failure(e) =>
                  complete(StatusCodes.InternalServerError, errorJson(e.getMessage))
              }
          }
        }
      },

      // GET /api/v1/config/features/enabled - Get enabled features
      path("features" / "enabled") {
        get {
          TenantContext.getCurrentTenantId match {
            case tenantId =>
              TenantConfigService.getEnabledFeatures(tenantId) match {
                case Success(features) =>
                  val json = features.map(f => s""""${escapeJson(f)}"""").mkString("[", ",", "]")
                  complete(StatusCodes.OK, json)
                case Failure(e) =>
                  complete(StatusCodes.InternalServerError, errorJson(e.getMessage))
              }
          }
        }
      },

      // GET /api/v1/config/features/{feature} - Check feature status
      path("features" / Segment) { feature =>
        get {
          TenantContext.getCurrentTenantId match {
            case tenantId =>
              val enabled = TenantConfigService.isFeatureEnabled(feature, tenantId)
              complete(StatusCodes.OK, featureToJson(feature, enabled))
          }
        } ~
          post {
            TenantContext.getCurrentTenantId match {
              case tenantId =>
                TenantConfigService.enableFeature(feature, tenantId) match {
                  case Success(config) =>
                    TenantConfigContext.cacheConfig(tenantId, config)
                    complete(StatusCodes.OK, successJson(s"Feature '$feature' enabled"))
                  case Failure(e) =>
                    complete(StatusCodes.BadRequest, errorJson(e.getMessage))
                }
            }
          } ~
          delete {
            TenantContext.getCurrentTenantId match {
              case tenantId =>
                TenantConfigService.disableFeature(feature, tenantId) match {
                  case Success(config) =>
                    TenantConfigContext.cacheConfig(tenantId, config)
                    complete(StatusCodes.OK, successJson(s"Feature '$feature' disabled"))
                  case Failure(e) =>
                    complete(StatusCodes.BadRequest, errorJson(e.getMessage))
                }
            }
          }
      },

      // POST /api/v1/config/features/bulk - Bulk update features
      path("features" / "bulk") {
        post {
          entity(as[String]) { body =>
            TenantContext.getCurrentTenantId match {
              case tenantId =>
                extractFeaturesMap(body) match {
                  case Some(features) =>
                    TenantConfigService.bulkUpdateFeatures(tenantId, features) match {
                      case Success(config) =>
                        TenantConfigContext.cacheConfig(tenantId, config)
                        complete(StatusCodes.OK, successJson(s"${features.size} features updated"))
                      case Failure(e) =>
                        complete(StatusCodes.BadRequest, errorJson(e.getMessage))
                    }
                  case None =>
                    complete(StatusCodes.BadRequest, errorJson("Invalid features format"))
                }
            }
          }
        }
      },

      // ===== Config Operations =====

      // POST /api/v1/config/reset - Reset to defaults
      path("reset") {
        post {
          TenantContext.getCurrentTenantId match {
            case tenantId =>
              TenantConfigService.resetToDefaults(tenantId) match {
                case Success(config) =>
                  TenantConfigContext.cacheConfig(tenantId, config)
                  complete(StatusCodes.OK, successJson("Config reset to defaults"))
                case Failure(e) =>
                  complete(StatusCodes.BadRequest, errorJson(e.getMessage))
              }
          }
        }
      },

      // POST /api/v1/config/refresh - Clear cache
      path("refresh") {
        post {
          TenantContext.getCurrentTenantId match {
            case tenantId =>
              TenantConfigService.clearCache(tenantId)
              TenantConfigContext.clearCachedTenant(tenantId)
              complete(StatusCodes.OK, successJson("Cache cleared for tenant"))
          }
        }
      },

      // GET /api/v1/config/exists - Check if config exists
      path("exists") {
        get {
          TenantContext.getCurrentTenantId match {
            case tenantId =>
              TenantConfigService.configExists(tenantId) match {
                case Success(exists) =>
                  complete(StatusCodes.OK, s"""{"exists":$exists}""")
                case Failure(e) =>
                  complete(StatusCodes.InternalServerError, errorJson(e.getMessage))
              }
          }
        }
      },

      // GET /api/v1/config/health - Health check
      path("health") {
        get {
          TenantContext.getCurrentTenantId match {
            case tenantId =>
              TenantConfigService.healthCheck(tenantId) match {
                case Success(metrics) =>
                  val json = metrics.map {
                    case (k, v: String) => s""""$k":"${escapeJson(v)}""""
                    case (k, v: Boolean) => s""""$k":$v"""
                    case (k, v: Int) => s""""$k":$v"""
                    case (k, v: Long) => s""""$k":$v"""
                    case (k, v) => s""""$k":"${escapeJson(v.toString)}""""
                  }.mkString("{", ",", "}")
                  complete(StatusCodes.OK, json)
                case Failure(e) =>
                  complete(StatusCodes.ServiceUnavailable, errorJson(e.getMessage))
              }
          }
        }
      },

      // GET /api/v1/config/cache/status - Cache status from context
      path("cache" / "status") {
        get {
          TenantContext.getCurrentTenantId match {
            case tenantId =>
              val isCached = TenantConfigContext.isCached(tenantId)
              val cacheSize = TenantConfigContext.cacheSize
              complete(StatusCodes.OK, s"""{"tenantId":"$tenantId","cached":$isCached,"cacheSize":$cacheSize}""")
          }
        }
      },

      // GET /api/v1/config/metrics - Cache metrics
      path("metrics") {
        get {
          val metrics = TenantConfigContext.getMetrics
          val json = metrics.map {
            case (k, v: String) => s""""$k":"${escapeJson(v)}""""
            case (k, v: Double) => s""""$k":$v"""
            case (k, v: Int) => s""""$k":$v"""
            case (k, v: Long) => s""""$k":$v"""
            case (k, v: Boolean) => s""""$k":$v"""
            case (k, v) => s""""$k":"${escapeJson(v.toString)}""""
          }.mkString("{", ",", "}")
          complete(StatusCodes.OK, json)
        }
      }
    )
  }
}