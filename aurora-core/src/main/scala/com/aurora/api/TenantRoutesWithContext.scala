package com.aurora.api

import akka.http.scaladsl.server.Directives.*
import akka.http.scaladsl.server.Route
import com.aurora.tenant.TenantContext
import scala.util.control.NonFatal

object TenantRoutesWithContext {

  /**
   * Wraps a route with tenant context.
   * Uses "X-Tenant-Id" header if present, otherwise defaults to "system".
   */
  def routesWithTenant(routes: Route): Route = {
    extractRequest { request =>
      val tenantId = request.headers
        .find(_.name() == "X-Tenant-Id")
        .map(_.value())
        .getOrElse("system")

      // Use mapInnerRoute to safely set/clear ThreadLocal for async execution
      mapInnerRoute { innerRoute => ctx =>
        try {
          TenantContext.setFullContext(tenantId)
          innerRoute(ctx) // execute the request
        } catch {
          case NonFatal(ex) =>
            ctx.fail(ex)
        } finally {
          TenantContext.clearContext()
        }
      } apply routes
    }
  }
}
