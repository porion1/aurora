package com.aurora.api

import akka.http.scaladsl.server.Directives.*
import akka.http.scaladsl.server.{Directive0, Directive1, Route}
import akka.http.scaladsl.model.StatusCodes
import com.aurora.tenant.{LimitType, ResourceLimit, ResourceType, TenantContext, TenantResourceLimits, TenantResourceService}
import com.typesafe.scalalogging.StrictLogging
import io.circe.*
import io.circe.generic.auto.*
import io.circe.jawn.decode
import io.circe.syntax.*

import java.time.Instant
import scala.concurrent.ExecutionContext
import scala.util.Try

class TenantResourceRoutes(resourceService: TenantResourceService)(implicit ec: ExecutionContext) extends StrictLogging {

  import TenantResourceRoutes.*

  val routes: Route = pathPrefix("api" / "v1" / "limits") {
    concat(
      pathEnd {
        get {
          tenantScope { tenantId =>
            resourceService.getLimits(tenantId) match {
              case Some(limits) => complete(limits.asJson.toString)
              case None => complete(StatusCodes.NotFound, s"No limits found for tenant $tenantId")
            }
          }
        }
      },

      path("admin") {
        put {
          entity(as[String]) { json =>
            tenantScope { tenantId =>
              decode[TenantResourceLimits](json) match {
                case Right(newLimits) =>
                  onComplete(resourceService.setLimits(tenantId, newLimits)) {
                    case scala.util.Success(true) =>
                      logger.info(s"Updated resource limits for tenant $tenantId")
                      complete(StatusCodes.OK, "Limits updated successfully")
                    case scala.util.Success(false) =>
                      complete(StatusCodes.InternalServerError, "Failed to update limits")
                    case scala.util.Failure(ex) =>
                      logger.error(s"Error updating limits for tenant $tenantId", ex)
                      complete(StatusCodes.InternalServerError, s"Error: ${ex.getMessage}")
                  }
                case Left(error) =>
                  complete(StatusCodes.BadRequest, s"Invalid JSON: ${error.getMessage}")
              }
            }
          }
        }
      },

      path("usage") {
        get {
          tenantScope { tenantId =>
            resourceService.getUsage(tenantId) match {
              case Some(usage) =>
                // Convert the usage map to Json manually
                val json = Json.obj(
                  usage.map { case (k, v) =>
                    k -> (v match {
                      case s: String => Json.fromString(s)
                      case i: Int => Json.fromInt(i)
                      case l: Long => Json.fromLong(l)
                      case d: Double => Json.fromDoubleOrNull(d)
                      case b: Boolean => Json.fromBoolean(b)
                      case m: Map[_, _] => Json.obj() // Handle nested maps if needed
                      case null => Json.Null
                      case other => Json.fromString(other.toString)
                    })
                  }.toSeq*
                )
                complete(json.toString)
              case None => complete(StatusCodes.NotFound, s"No usage data for tenant $tenantId")
            }
          }
        }
      },

      path("status") {
        get {
          tenantScope { tenantId =>
            val status = resourceService.getLimitStatus(tenantId)
            // Convert status map to Json manually
            val json = Json.obj(
              status.map { case (resource, metrics) =>
                resource -> Json.obj(
                  "current" -> Json.fromDoubleOrNull(metrics("current").asInstanceOf[Double]),
                  "limit" -> Json.fromDoubleOrNull(metrics("limit").asInstanceOf[Double]),
                  "percentage" -> Json.fromDoubleOrNull(metrics("percentage").asInstanceOf[Double]),
                  "status" -> Json.fromString(metrics("status").asInstanceOf[String]),
                  "unit" -> Json.fromString(metrics("unit").asInstanceOf[String])
                )
              }.toSeq*
            )
            complete(json.toString)
          }
        }
      },
      
      path("reset") {
        post {
          tenantScope { tenantId =>
            resourceService.resetUsage(tenantId)
            complete(StatusCodes.OK, "Usage counters reset")
          }
        }
      },

      path("admin" / "all") {
        get {
          val allUsage = resourceService.getAllTenantMetrics
          val json = Json.obj(
            allUsage.map { case (tenantId, metrics) =>
              tenantId -> Json.obj(
                "total_requests" -> Json.fromLong(metrics("total_requests").asInstanceOf[Long]),
                "rejected_requests" -> Json.fromLong(metrics("rejected_requests").asInstanceOf[Long]),
                "rejection_rate" -> Json.fromDoubleOrNull(metrics("rejection_rate").asInstanceOf[Double]),
                "peak_concurrent" -> Json.fromInt(metrics("peak_concurrent").asInstanceOf[Int]),
                "current_concurrent" -> Json.fromInt(metrics("current_concurrent").asInstanceOf[Int]),
                "avg_response_time_ms" -> Json.fromDoubleOrNull(metrics("avg_response_time_ms").asInstanceOf[Double]),
                "violation_count" -> Json.fromInt(metrics("violation_count").asInstanceOf[Int]),
                "last_violation" -> Json.fromString(metrics("last_violation").asInstanceOf[String]),
                "current_memory_mb" -> Json.fromLong(metrics("current_memory_mb").asInstanceOf[Long]),
                "peak_memory_mb" -> Json.fromLong(metrics("peak_memory_mb").asInstanceOf[Long]),
                "requests_last_minute" -> Json.fromInt(metrics("requests_last_minute").asInstanceOf[Int]),
                "requests_last_hour" -> Json.fromInt(metrics("requests_last_hour").asInstanceOf[Int])
              )
            }.toSeq*
          )
          complete(json.toString)
        }
      },

      path("admin" / "set" / Segment / Segment) { (resourceStr, valueStr) =>
        put {
          tenantScope { tenantId =>
            (for {
              resource <- ResourceType.values.find(_.toString.equalsIgnoreCase(resourceStr))
              value <- Try(valueStr.toDouble).toOption
              if value > 0
            } yield {
              resourceService.getLimits(tenantId).foreach { limits =>
                val newLimit = ResourceLimit(value, LimitType.Hard, Some(60), None)
                val updated = limits.withLimit(resource, newLimit)
                resourceService.setLimits(tenantId, updated)
              }
              complete(StatusCodes.OK, s"Set $resource limit to $value")
            }).getOrElse(complete(StatusCodes.BadRequest, "Invalid resource or value"))
          }
        }
      },

      path("health") {
        get {
          complete(StatusCodes.OK, "Resource limits service is healthy")
        }
      }
    )
  }

  private def tenantScope: Directive1[String] =
    extractRequest.flatMap { _ =>
      val tenantIdObj = TenantContext.getCurrentTenantId
      if (tenantIdObj != null && tenantIdObj.toString.nonEmpty) {
        provide(tenantIdObj.toString)
      } else {
        complete(StatusCodes.BadRequest, "X-Tenant-Id header required")
      }
    }
}

object TenantResourceRoutes {
  // Existing encoders...
  implicit val resourceTypeEncoder: Encoder[ResourceType] = Encoder.encodeString.contramap(_.toString)
  implicit val limitTypeEncoder: Encoder[LimitType] = Encoder.encodeString.contramap(_.toString)

  implicit val resourceLimitEncoder: Encoder[ResourceLimit] =
    Encoder.forProduct4("value", "type", "windowSeconds", "description")(l =>
      (l.value, l.limitType, l.windowSeconds, l.description)
    )

  // Add this decoder for ResourceLimit
  implicit val resourceLimitDecoder: Decoder[ResourceLimit] =
    Decoder.forProduct4("value", "type", "windowSeconds", "description")(ResourceLimit.apply)

  // Add this decoder for Map[ResourceType, ResourceLimit]
  implicit val limitsMapDecoder: Decoder[Map[ResourceType, ResourceLimit]] =
    new Decoder[Map[ResourceType, ResourceLimit]] {
      final def apply(c: HCursor): Decoder.Result[Map[ResourceType, ResourceLimit]] = {
        c.keys match {
          case Some(keys) =>
            val result = keys.foldLeft(Right(Map.empty): Either[String, Map[ResourceType, ResourceLimit]]) {
              case (acc, key) =>
                acc.flatMap { map =>
                  c.downField(key).as[ResourceLimit] match {
                    case Right(limit) =>
                      ResourceType.values.find(_.toString == key) match {
                        case Some(resourceType) =>
                          Right(map + (resourceType -> limit))
                        case None =>
                          Left(s"Invalid resource type key: $key")
                      }
                    case Left(error) =>
                      Left(error.message)
                  }
                }
            }
            result.left.map(DecodingFailure(_, c.history))
          case None =>
            Right(Map.empty)
        }
      }
    }

  // Add this decoder for TenantResourceLimits
  implicit val tenantResourceLimitsDecoder: Decoder[TenantResourceLimits] =
    Decoder.forProduct5("tenantId", "limits", "updatedAt", "updatedBy", "version") {
      (tenantId: String, limits: Map[ResourceType, ResourceLimit], updatedAt: String, updatedBy: Option[String], version: Long) =>
        TenantResourceLimits(tenantId, limits, Instant.parse(updatedAt), updatedBy, version)
    }

  // Keep your existing encoder
  implicit val tenantResourceLimitsEncoder: Encoder[TenantResourceLimits] =
    Encoder.forProduct5("tenantId", "limits", "updatedAt", "updatedBy", "version")(l =>
      (l.tenantId, l.limits.map { case (k, v) => k.toString -> v }, l.updatedAt, l.updatedBy, l.version)
    )
}