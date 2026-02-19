package com.aurora.api

import akka.http.scaladsl.server.Directives.*
import akka.http.scaladsl.server.{ Directive0, Rejection, RejectionHandler, Route }
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.RawHeader
import com.aurora.tenant.{ TenantContext, TenantResourceService }

/**
 * Rejection for rate limiting
 */
case class RateLimitRejection(
                               tenantId: String,
                               resource: String,
                               limit: Double,
                               current: Double,
                               retryAfterSeconds: Int
                             ) extends Rejection

/**
 * Middleware for enforcing tenant resource limits
 */
class TenantResourceMiddleware(val resourceService: TenantResourceService) {

  // Simple logging helpers
  private def logInfo(msg: String): Unit = println(s"[INFO] [TenantResourceMiddleware] $msg")
  private def logWarn(msg: String): Unit = println(s"[WARN] [TenantResourceMiddleware] $msg")
  private def logError(msg: String): Unit = println(s"[ERROR] [TenantResourceMiddleware] $msg")

  /**
   * Directive that enforces resource limits for the current tenant
   */
  def withResourceLimit: Directive0 =
    extractRequest.flatMap { _ =>
      val tenantId = TenantContext.getCurrentTenantId

      if (tenantId == null || tenantId.isEmpty) {
        logWarn("No tenant context found for request")
        complete(StatusCodes.BadRequest, "X-Tenant-Id header required")
      } else {
        try {
          resourceService.checkRequestAllowed(tenantId) match {
            case Left(error) =>
              logInfo(s"Rate limit exceeded for tenant $tenantId: $error")
              val headers = List(RawHeader("Retry-After", "60"))
              complete((StatusCodes.TooManyRequests, headers, s"Rate limit exceeded: $error"))

            case Right(()) =>
              mapResponse { response =>
                try {
                  resourceService.trackRequestComplete(tenantId)
                } catch {
                  case e: Exception =>
                    logError(s"Error tracking request completion: ${e.getMessage}")
                }
                response
              } & pass
          }
        } catch {
          case e: Exception =>
            logError(s"Exception in rate limiter for tenant $tenantId: ${e.getMessage}")
            e.printStackTrace()
            // Fail open - allow the request but log error
            complete(StatusCodes.OK, "Request processed (rate limiter error)")
        }
      }
    }

  /**
   * Directive to track memory usage (call periodically)
   */
  def withMemoryTracking: Directive0 =
    extractRequest.flatMap { _ =>
      val tenantId = TenantContext.getCurrentTenantId

      if (tenantId != null && tenantId.nonEmpty) {
        try {
          resourceService.updateMemoryUsage(tenantId)
        } catch {
          case e: Exception =>
            logError(s"Error updating memory usage: ${e.getMessage}")
        }
      }
      pass
    }

  /**
   * Wrap routes with resource limit enforcement
   */
  def withResourceLimits(inner: Route): Route =
    withResourceLimit {
      inner
    }
}

/**
 * Custom rejection handler for rate limiting
 */
object RateLimitRejectionHandler {

  private def logInfo(msg: String): Unit = println(s"[INFO] [RateLimitRejectionHandler] $msg")

  val handler: RejectionHandler = RejectionHandler.newBuilder()
    .handle {
      case RateLimitRejection(tenantId, resource, limit, current, retryAfter) =>
        logInfo(s"Rate limit rejection for tenant $tenantId: $resource $current/$limit")
        complete((
          StatusCodes.TooManyRequests,
          List(RawHeader("Retry-After", retryAfter.toString)),
          s"Rate limit exceeded for $resource: $current/$limit"
        ))
    }
    .result()
}