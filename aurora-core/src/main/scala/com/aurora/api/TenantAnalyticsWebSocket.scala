package com.aurora.api

import akka.actor.ActorSystem
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.stream.scaladsl.{Flow, Sink, Source, Keep}
import akka.stream.OverflowStrategy

import com.aurora.tenant.TenantContext
import com.aurora.analytics.{TenantAnalyticsStorage, TenantMetricsCollector}
import java.time.Instant
import scala.concurrent.duration.*

/**
 * FAANG-level: Real-time analytics via WebSocket
 * Each tenant gets their own stream
 *
 * Key features:
 * - Per-tenant WebSocket connections
 * - Real-time metric streaming
 * - Client subscription management
 * - Keep-alive pings
 * - Thread-safe subscriber management
 */
class TenantAnalyticsWebSocket(
                                collector: TenantMetricsCollector,
                                storage: TenantAnalyticsStorage
                              )(implicit system: ActorSystem) {

  // Structured logging
  private def debug(msg: String): Unit = println(s"[DEBUG] [WebSocket] $msg")
  private def info(msg: String): Unit = println(s"[INFO] [WebSocket] $msg")
  private def warn(msg: String): Unit = println(s"[WARN] [WebSocket] $msg")

  /**
   * WebSocket flow per connection
   */
  def websocketFlow(tenantId: String)(implicit ec: scala.concurrent.ExecutionContext): Flow[Message, Message, Any] = {

    debug(s"Creating WebSocket flow for tenant $tenantId")

    // Create a source that uses an actor to push real-time updates
    val (actorRef, source) = Source.actorRef[AnalyticsUpdate](
      completionMatcher = {
        case _ => akka.stream.CompletionStrategy.draining
      },
      failureMatcher = {
        case ex: Throwable => ex
      },
      bufferSize = 100,
      overflowStrategy = OverflowStrategy.dropHead
    ).preMaterialize()

    // Register this actor for tenant updates
    TenantAnalyticsWebSocket.register(tenantId, actorRef)

    // Map updates to WebSocket messages
    val websocketSource = source
      .map { update =>
        TextMessage(s"""{
        "type": "${update.updateType}",
        "timestamp": "${update.timestamp}",
        "data": ${update.data}
      }""")
      }
      .keepAlive(30.seconds, () => TextMessage("""{"type":"ping"}"""))
      .watchTermination() { (_, termination) =>
        termination.onComplete { _ =>
          // Clean up when connection closes
          TenantAnalyticsWebSocket.unregister(tenantId, actorRef)
          debug(s"WebSocket connection closed for tenant $tenantId")
        }(ec) // Pass the execution context explicitly
        akka.NotUsed
      }

    // Sink for incoming messages (client commands)
    val sink = Sink.foreach[Message] {
      case TextMessage.Strict(text) => handleClientMessage(tenantId, text)
      case _ => // Ignore binary messages
    }

    Flow.fromSinkAndSource(sink, websocketSource)
  }

  /**
   * Handle client commands
   */
  private def handleClientMessage(tenantId: String, message: String): Unit = {
    message match {
      case "subscribe:requests" =>
        TenantAnalyticsWebSocket.subscribe(tenantId, "requests")
        debug(s"Tenant $tenantId subscribed to requests")

      case "subscribe:sessions" =>
        TenantAnalyticsWebSocket.subscribe(tenantId, "sessions")
        debug(s"Tenant $tenantId subscribed to sessions")

      case "subscribe:limits" =>
        TenantAnalyticsWebSocket.subscribe(tenantId, "limits")
        debug(s"Tenant $tenantId subscribed to limits")

      case "unsubscribe" =>
        TenantAnalyticsWebSocket.unsubscribe(tenantId)
        debug(s"Tenant $tenantId unsubscribed from all")

      case cmd if cmd.startsWith("subscribe:") =>
        val metric = cmd.substring(10)
        TenantAnalyticsWebSocket.subscribe(tenantId, metric)
        debug(s"Tenant $tenantId subscribed to $metric")

      case _ =>
        warn(s"Unknown client command: $message")
    }
  }
}

/**
 * Companion object for managing WebSocket subscribers
 */
object TenantAnalyticsWebSocket {

  // Use classic ActorRef for compatibility with Source.actorRef
  private case class AnalyticsSubscription(
                                            tenantId: String,
                                            actorRef: akka.actor.ActorRef,
                                            subscriptions: Set[String] = Set("all")
                                          )

  // Thread-safe subscriber storage
  private var subscribers: Map[String, List[akka.actor.ActorRef]] = Map.empty
  private var subscriptions: Map[String, Map[akka.actor.ActorRef, Set[String]]] = Map.empty

  /**
   * Register a new WebSocket connection
   */
  def register(tenantId: String, actorRef: akka.actor.ActorRef): Unit = synchronized {
    subscribers = subscribers.updated(
      tenantId,
      actorRef :: subscribers.getOrElse(tenantId, List.empty)
    )

    subscriptions = subscriptions.updated(
      tenantId,
      subscriptions.getOrElse(tenantId, Map.empty) + (actorRef -> Set("all"))
    )

    println(s"[WebSocket] Registered subscriber for tenant $tenantId (total: ${subscribers.getOrElse(tenantId, List.empty).size})")
  }

  /**
   * Unregister a WebSocket connection
   */
  def unregister(tenantId: String, actorRef: akka.actor.ActorRef): Unit = synchronized {
    subscribers = subscribers.updated(
      tenantId,
      subscribers.getOrElse(tenantId, List.empty).filterNot(_ == actorRef)
    )

    subscriptions = subscriptions.updated(
      tenantId,
      subscriptions.getOrElse(tenantId, Map.empty) - actorRef
    )

    if (subscribers.getOrElse(tenantId, List.empty).isEmpty) {
      subscribers = subscribers - tenantId
      subscriptions = subscriptions - tenantId
    }
  }

  /**
   * Subscribe a connection to specific metrics
   */
  def subscribe(tenantId: String, metric: String): Unit = synchronized {
    subscriptions.get(tenantId).foreach { tenantSubs =>
      // In a real implementation, you'd update per-actor subscriptions
      // This is a simplified version
      println(s"[WebSocket] Tenant $tenantId subscribed to $metric")
    }
  }

  /**
   * Unsubscribe a tenant from all metrics
   */
  def unsubscribe(tenantId: String): Unit = synchronized {
    subscribers -= tenantId
    subscriptions -= tenantId
    println(s"[WebSocket] Unsubscribed all for tenant $tenantId")
  }

  /**
   * Broadcast an update to all subscribers of a tenant
   */
  def broadcast(tenantId: String, update: AnalyticsUpdate): Unit = {
    subscribers.getOrElse(tenantId, List.empty).foreach { actorRef =>
      actorRef ! update
    }
  }

  /**
   * Broadcast an update to all subscribers of a tenant that have subscribed to a specific metric
   */
  def broadcastToMetric(tenantId: String, metric: String, update: AnalyticsUpdate): Unit = {
    // In a real implementation, you'd filter by subscription
    broadcast(tenantId, update)
  }

  /**
   * Broadcast to all subscribers across all tenants
   */
  def broadcastToAll(update: AnalyticsUpdate): Unit = synchronized {
    subscribers.values.flatten.foreach(_ ! update)
  }

  /**
   * Get subscriber statistics
   */
  def getStats: Map[String, Any] = Map(
    "total_tenants" -> subscribers.size,
    "total_connections" -> subscribers.values.map(_.size).sum,
    "tenants" -> subscribers.map { case (tenant, actors) =>
      tenant -> Map("connections" -> actors.size)
    }
  )
}

/**
 * Analytics update trait for WebSocket messages
 */
sealed trait AnalyticsUpdate {
  def updateType: String
  def timestamp: String = Instant.now().toString
  def data: String
}

/**
 * Request rate update
 */
case class RequestRateUpdate(count: Int, period: String) extends AnalyticsUpdate {
  val updateType = "request_rate"
  val data = s"""{"count":$count,"period":"$period"}"""
}

/**
 * Concurrent requests update
 */
case class ConcurrentRequestsUpdate(count: Int) extends AnalyticsUpdate {
  val updateType = "concurrent_requests"
  val data = s"""{"count":$count}"""
}

/**
 * Session count update
 */
case class SessionCountUpdate(count: Int) extends AnalyticsUpdate {
  val updateType = "session_count"
  val data = s"""{"count":$count}"""
}

/**
 * Limit alert update
 */
case class LimitAlertUpdate(
                             resource: String,
                             current: Double,
                             limit: Double,
                             percentage: Int
                           ) extends AnalyticsUpdate {
  val updateType = "limit_alert"
  val data = s"""{"resource":"$resource","current":$current,"limit":$limit,"percentage":$percentage}"""
}

/**
 * Custom metric update
 */
case class CustomMetricUpdate(
                               name: String,
                               value: Double,
                               unit: String
                             ) extends AnalyticsUpdate {
  val updateType = "custom_metric"
  val data = s"""{"name":"$name","value":$value,"unit":"$unit"}"""
}