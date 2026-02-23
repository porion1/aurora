package com.aurora.api

import akka.http.scaladsl.server.Directives.*
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.model.{StatusCodes, ContentTypes, HttpEntity}
import akka.http.scaladsl.model.ws.Message

import com.aurora.tenant.TenantContext
import com.aurora.analytics.*
import org.slf4j.{Logger, LoggerFactory}

import java.time.Instant
import scala.util.{Success, Failure}
import scala.jdk.CollectionConverters.*

/**
 * HTTP routes for tenant analytics.
 *
 * Provides endpoints for:
 * - Session tracking
 * - Real-time metrics
 * - Time-series data
 * - Predictive analytics
 * - Data export
 * - Health monitoring
 */
class TenantAnalyticsRoutes(
                             collector: TenantMetricsCollector,
                             storage: TenantAnalyticsStorage,
                             predictor: TenantPredictiveAnalytics,
                             webSocket: TenantAnalyticsWebSocket
                           )(implicit ec: scala.concurrent.ExecutionContext) {

  // ===== SLF4J Logger =====
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  // ===== JSON Helpers =====
  private def escapeJson(s: String): String = {
    s.replace("\\", "\\\\").replace("\"", "\\\"")
  }

  private def messageJson(msg: String): String = s"""{"message":"${escapeJson(msg)}"}"""
  private def errorJson(msg: String): String = s"""{"error":"${escapeJson(msg)}"}"""
  private def successJson(msg: String): String = s"""{"success":true,"message":"${escapeJson(msg)}"}"""

  private def mapToJson(map: Map[String, Any]): String = {
    if (map.isEmpty) "{}" else {
      map.map {
        case (k, v: String) => s""""${escapeJson(k)}":"${escapeJson(v)}""""
        case (k, v: Int) => s""""${escapeJson(k)}":$v"""
        case (k, v: Long) => s""""${escapeJson(k)}":$v"""
        case (k, v: Double) => s""""${escapeJson(k)}":$v"""
        case (k, v: Boolean) => s""""${escapeJson(k)}":$v"""
        case (k, v: Map[_, _]) => s""""${escapeJson(k)}":${mapToJson(v.asInstanceOf[Map[String, Any]])}"""
        case (k, v: Seq[_]) => s""""${escapeJson(k)}":[${v.map(_.toString).mkString(",")}]"""
        case (k, v) => s""""${escapeJson(k)}":"${escapeJson(v.toString)}""""
      }.mkString("{", ",", "}")
    }
  }

  private def timeSeriesPointToJson(point: TimeSeriesDataPoint): String = {
    s"""{
       |"timestamp":"${point.timestamp}",
       |"value":${point.value},
       |"count":${point.count},
       |"min":${point.min},
       |"max":${point.max},
       |"p95":${point.p95.getOrElse(0.0)},
       |"p99":${point.p99.getOrElse(0.0)}
       |}""".stripMargin
  }

  private def anomalyToJson(a: Anomaly): String = {
    s"""{
       |"timestamp":"${a.timestamp}",
       |"actual":${a.actualValue},
       |"expected":${a.expectedValue},
       |"deviation":${a.deviation},
       |"severity":"${a.severity}",
       |"method":"${a.method}"
       |}""".stripMargin
  }

  private def sessionToJson(id: String, meta: TenantAnalyticsContext.SessionMetadata): String = {
    s"""{
       |"sessionId":"$id",
       |"startTime":${meta.startTimeEpochMs},
       |"lastActivity":${meta.lastActivityEpochMs},
       |"userId":"${meta.userId.getOrElse("anonymous")}",
       |"pageViews":${meta.pageViews},
       |"actions":${meta.actions}
       |}""".stripMargin
  }

  // ===== Request Parameter Parsers =====
  private def extractTimeRange(startStr: String, endStr: String): Option[(Instant, Instant)] = {
    try {
      Some((Instant.parse(startStr), Instant.parse(endStr)))
    } catch {
      case e: Exception =>
        if (logger.isDebugEnabled) logger.debug("Failed to parse time range: {}", e.getMessage)
        None
    }
  }

  // ===== Routes =====
  val routes: Route = pathPrefix("api" / "v1" / "analytics") {
    concat(

      // ===== Session Endpoints =====

      // GET /api/v1/analytics/sessions/current - Get current session
      path("sessions" / "current") {
        get {
          TenantContext.getCurrentTenantId match {
            case tenantId =>
              val sessionId = TenantAnalyticsContext.getCurrentSessionId
              val activeSessions = TenantAnalyticsContext.getActiveSessionsCount
              complete(StatusCodes.OK, mapToJson(Map(
                "tenantId" -> tenantId,
                "sessionId" -> sessionId.getOrElse("none"),
                "activeSessions" -> activeSessions
              )))
          }
        }
      },

      // GET /api/v1/analytics/sessions/active - Get all active sessions
      path("sessions" / "active") {
        get {
          TenantContext.getCurrentTenantId match {
            case tenantId =>
              val sessions = TenantAnalyticsContext.getActiveSessions
              val sessionsJson = sessions.map { case (id, meta) => sessionToJson(id, meta) }.mkString("[", ",", "]")
              complete(StatusCodes.OK, mapToJson(Map(
                "count" -> sessions.size,
                "sessions" -> sessionsJson
              )))
          }
        }
      },

      // ===== Real-time Metrics Endpoints =====

      // GET /api/v1/analytics/metrics/realtime?ws=true - WebSocket connection
      path("metrics" / "realtime") {
        get {
          parameter("ws".as[Boolean] ? false) { useWebSocket =>
            if (useWebSocket) {
              TenantContext.getCurrentTenantId match {
                case tenantId =>
                  handleWebSocketMessages(webSocket.websocketFlow(tenantId))
                case _ =>
                  complete(StatusCodes.BadRequest, errorJson("No tenant context"))
              }
            } else {
              complete(StatusCodes.NotImplemented, errorJson("SSE not yet implemented"))
            }
          }
        }
      },

      // ===== Time-series Metrics Endpoints =====

      // GET /api/v1/analytics/metrics/timeseries?metric=x&start=y&end=z&granularity=minute
      path("metrics" / "timeseries") {
        get {
          parameters(
            "metric".as[String],
            "start".as[String],
            "end".as[String],
            "granularity".as[String] ? "minute"
          ) { (metric, startStr, endStr, granularity) =>
            TenantContext.getCurrentTenantId match {
              case tenantId =>
                extractTimeRange(startStr, endStr) match {
                  case Some((start, end)) =>
                    val data = storage.queryTimeSeries(tenantId, metric, start, end, granularity)
                    val dataJson = data.map(timeSeriesPointToJson).mkString("[", ",", "]")
                    complete(StatusCodes.OK, mapToJson(Map(
                      "tenantId" -> tenantId,
                      "metric" -> metric,
                      "granularity" -> granularity,
                      "data" -> dataJson
                    )))
                  case None =>
                    complete(StatusCodes.BadRequest, errorJson("Invalid time format"))
                }
            }
          }
        }
      },

      // ===== Summary Endpoint =====

      // GET /api/v1/analytics/summary?hours=24
      path("summary") {
        get {
          parameters("hours".as[Int] ? 24) { hours =>
            TenantContext.getCurrentTenantId match {
              case tenantId =>
                storage.getTenantSummary(tenantId, hours) match {
                  case Success(summary) =>
                    complete(StatusCodes.OK, mapToJson(Map(
                      "tenantId" -> summary.tenantId,
                      "timeRange" -> summary.timeRange,
                      "totalRequests" -> summary.totalRequests,
                      "totalErrors" -> summary.totalErrors,
                      "errorRate" -> summary.errorRate,
                      "avgResponseTimeMs" -> summary.avgResponseTimeMs,
                      "p95ResponseTimeMs" -> summary.p95ResponseTimeMs,
                      "p99ResponseTimeMs" -> summary.p99ResponseTimeMs,
                      "maxResponseTimeMs" -> summary.maxResponseTimeMs,
                      "peakConcurrentRequests" -> summary.peakConcurrentRequests,
                      "peakActiveSessions" -> summary.peakActiveSessions,
                      "timestamp" -> summary.timestamp.toString
                    )))
                  case Failure(e) =>
                    logger.error("Failed to get summary: {}", e.getMessage)
                    complete(StatusCodes.InternalServerError, errorJson(e.getMessage))
                }
            }
          }
        }
      },

      // ===== Predictive Analytics Endpoints =====

      // GET /api/v1/analytics/predict/forecast?metric=requestCount&periods=24&periodType=hour
      path("predict" / "forecast") {
        get {
          parameters(
            "metric".as[String],
            "periods".as[Int] ? 24,
            "periodType".as[String] ? "hour"
          ) { (metric, periods, periodType) =>
            TenantContext.getCurrentTenantId match {
              case tenantId =>
                val forecast = predictor.forecastUsage(tenantId, metric, periods, periodType)
                complete(StatusCodes.OK, mapToJson(Map(
                  "tenantId" -> forecast.tenantId,
                  "metric" -> forecast.metric,
                  "forecast" -> forecast.forecast.mkString("[", ",", "]"),
                  "confidence" -> forecast.confidence,
                  "method" -> forecast.method,
                  "message" -> forecast.message,
                  "metadata" -> forecast.metadata
                )))
            }
          }
        }
      },

      // GET /api/v1/analytics/predict/anomalies?metric=requestCount&window=24&threshold=3.0
      path("predict" / "anomalies") {
        get {
          parameters(
            "metric".as[String],
            "window".as[Int] ? 24,
            "threshold".as[Double] ? 3.0
          ) { (metric, window, threshold) =>
            TenantContext.getCurrentTenantId match {
              case tenantId =>
                val anomalies = predictor.detectAnomalies(tenantId, metric, window, threshold)
                val anomaliesJson = anomalies.map(anomalyToJson).mkString("[", ",", "]")
                complete(StatusCodes.OK, mapToJson(Map(
                  "tenantId" -> tenantId,
                  "metric" -> metric,
                  "anomalies" -> anomaliesJson,
                  "count" -> anomalies.size
                )))
            }
          }
        }
      },

      // ===== Admin Endpoints =====

      // GET /api/v1/analytics/admin/all-tenants - Get metrics for all tenants (admin only)
      path("admin" / "all-tenants") {
        get {
          TenantContext.getCurrentTenantInfo match {
            case Some(info) if info.tenantId == "admin" || info.features.contains("admin") =>
              val allMetrics = collector.getAllTenantMetrics
              complete(StatusCodes.OK, mapToJson(allMetrics))
            case _ =>
              complete(StatusCodes.Forbidden, errorJson("Admin access required"))
          }
        }
      },

      // ===== Export Endpoints =====

      // GET /api/v1/analytics/export/csv?metric=x&start=y&end=z
      path("export" / "csv") {
        get {
          parameters(
            "metric".as[String],
            "start".as[String],
            "end".as[String]
          ) { (metric, startStr, endStr) =>
            TenantContext.getCurrentTenantId match {
              case tenantId =>
                extractTimeRange(startStr, endStr) match {
                  case Some((start, end)) =>
                    val data = storage.queryTimeSeries(tenantId, metric, start, end, "minute")

                    // Generate CSV
                    val csv = "timestamp,value,count,min,max,p95,p99\n" +
                      data.map { d =>
                        s"${d.timestamp},${d.value},${d.count},${d.min},${d.max},${d.p95.getOrElse(0.0)},${d.p99.getOrElse(0.0)}"
                      }.mkString("\n")

                    complete(HttpEntity(ContentTypes.`text/csv(UTF-8)`, csv))
                  case None =>
                    complete(StatusCodes.BadRequest, errorJson("Invalid time format"))
                }
            }
          }
        }
      },

      // GET /api/v1/analytics/export/json?metric=x&start=y&end=z
      path("export" / "json") {
        get {
          parameters(
            "metric".as[String],
            "start".as[String],
            "end".as[String]
          ) { (metric, startStr, endStr) =>
            TenantContext.getCurrentTenantId match {
              case tenantId =>
                extractTimeRange(startStr, endStr) match {
                  case Some((start, end)) =>
                    val data = storage.queryTimeSeries(tenantId, metric, start, end, "minute")
                    val dataJson = data.map(timeSeriesPointToJson).mkString("[", ",", "]")
                    complete(StatusCodes.OK, mapToJson(Map(
                      "tenantId" -> tenantId,
                      "metric" -> metric,
                      "data" -> dataJson,
                      "count" -> data.size
                    )))
                  case None =>
                    complete(StatusCodes.BadRequest, errorJson("Invalid time format"))
                }
            }
          }
        }
      },

      // ===== Health Check =====

      // GET /api/v1/analytics/health
      path("health") {
        get {
          TenantContext.getCurrentTenantId match {
            case tenantId =>
              val bufferSizes = collector.getBufferSizes
              complete(StatusCodes.OK, mapToJson(Map(
                "status" -> "healthy",
                "tenantId" -> tenantId,
                "timestamp" -> Instant.now().toString,
                "bufferSizes" -> bufferSizes,
                "activeSessions" -> TenantAnalyticsContext.getActiveSessionsCount,
                "collectorStats" -> collector.getStats
              )))
          }
        }
      },

      // ===== Cache Management =====

      // POST /api/v1/analytics/cache/clear - Clear analytics cache (admin only)
      path("cache" / "clear") {
        post {
          TenantContext.getCurrentTenantInfo match {
            case Some(info) if info.tenantId == "admin" || info.features.contains("admin") =>
              // Clear caches
              TenantAnalyticsContext.cleanupStaleSessions(0) // Clear all sessions
              complete(StatusCodes.OK, successJson("Analytics cache cleared"))
            case _ =>
              complete(StatusCodes.Forbidden, errorJson("Admin access required"))
          }
        }
      }
    )
  }
}