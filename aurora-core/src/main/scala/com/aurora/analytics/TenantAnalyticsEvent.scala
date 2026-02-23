package com.aurora.analytics

import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

/**
 * FAANG-level: Lock-free, immutable events with schema versioning
 */
sealed trait AnalyticsEvent {
  def eventId: String
  def tenantId: String
  def timestamp: Instant
  def eventType: AnalyticsEventType
  def schemaVersion: Int = 1
  def metadata: Map[String, String]

  /**
   * For event sampling - allows dropping events under load
   */
  def shouldSample(samplingRate: Double): Boolean = {
    if (samplingRate >= 1.0) true
    else math.abs(eventId.hashCode % 100) / 100.0 < samplingRate
  }
}

object AnalyticsEvent {
  private val idGenerator = new AtomicLong(0)

  def generateEventId(): String =
    s"evt_${System.currentTimeMillis()}_${idGenerator.incrementAndGet()}"
}

/**
 * Event types with cardinality hints for storage optimization
 */
sealed trait AnalyticsEventType {
  def cardinality: Cardinality
  def entryName: String
}

object AnalyticsEventType {

  case object API_REQUEST extends AnalyticsEventType {
    val cardinality = Cardinality.HIGH
    val entryName = "api_request"
  }

  case object PAGE_VIEW extends AnalyticsEventType {
    val cardinality = Cardinality.HIGH
    val entryName = "page_view"
  }

  case object USER_ACTION extends AnalyticsEventType {
    val cardinality = Cardinality.HIGH
    val entryName = "user_action"
  }

  case object SESSION_START extends AnalyticsEventType {
    val cardinality = Cardinality.MEDIUM
    val entryName = "session_start"
  }

  case object SESSION_END extends AnalyticsEventType {
    val cardinality = Cardinality.MEDIUM
    val entryName = "session_end"
  }

  case object FEATURE_TOGGLE extends AnalyticsEventType {
    val cardinality = Cardinality.MEDIUM
    val entryName = "feature_toggle"
  }

  case object CONFIG_CHANGE extends AnalyticsEventType {
    val cardinality = Cardinality.LOW
    val entryName = "config_change"
  }

  case object LIMIT_VIOLATION extends AnalyticsEventType {
    val cardinality = Cardinality.LOW
    val entryName = "limit_violation"
  }

  case object ALERT_TRIGGERED extends AnalyticsEventType {
    val cardinality = Cardinality.LOW
    val entryName = "alert_triggered"
  }

  val values: List[AnalyticsEventType] = List(
    API_REQUEST, PAGE_VIEW, USER_ACTION, SESSION_START, SESSION_END,
    FEATURE_TOGGLE, CONFIG_CHANGE, LIMIT_VIOLATION, ALERT_TRIGGERED
  )

  def fromString(s: String): Option[AnalyticsEventType] = s match {
    case "api_request" => Some(API_REQUEST)
    case "page_view" => Some(PAGE_VIEW)
    case "user_action" => Some(USER_ACTION)
    case "session_start" => Some(SESSION_START)
    case "session_end" => Some(SESSION_END)
    case "feature_toggle" => Some(FEATURE_TOGGLE)
    case "config_change" => Some(CONFIG_CHANGE)
    case "limit_violation" => Some(LIMIT_VIOLATION)
    case "alert_triggered" => Some(ALERT_TRIGGERED)
    case _ => None
  }
}

sealed trait Cardinality {
  def retentionDays: Int
  def samplingRate: Double
}

object Cardinality {

  case object HIGH extends Cardinality {
    val retentionDays = 7
    val samplingRate = 0.1  // Sample 10% of high cardinality events
  }

  case object MEDIUM extends Cardinality {
    val retentionDays = 30
    val samplingRate = 0.5  // Sample 50% of medium
  }

  case object LOW extends Cardinality {
    val retentionDays = 90
    val samplingRate = 1.0  // Keep all low cardinality
  }

  val values: List[Cardinality] = List(HIGH, MEDIUM, LOW)
}

/**
 * API Request event with performance metrics
 */
case class ApiRequestEvent(
                            eventId: String = AnalyticsEvent.generateEventId(),
                            tenantId: String,
                            timestamp: Instant = Instant.now(),
                            sessionId: Option[String] = None,
                            userId: Option[String] = None,
                            method: String,
                            path: String,
                            statusCode: Int,
                            responseTimeMs: Long,
                            cpuTimeMs: Option[Long] = None,
                            memoryDeltaMb: Option[Int] = None,
                            requestSize: Option[Long] = None,
                            responseSize: Option[Long] = None,
                            userAgent: Option[String] = None,
                            ipAddress: Option[String] = None,
                            errorType: Option[String] = None,
                            metadata: Map[String, String] = Map.empty
                          ) extends AnalyticsEvent {
  val eventType: AnalyticsEventType = AnalyticsEventType.API_REQUEST

  /**
   * FAANG-level: Pre-compute derived metrics for fast queries
   */
  lazy val isError: Boolean = statusCode >= 400
  lazy val isSlowRequest: Boolean = responseTimeMs > 1000
}

/**
 * Session tracking event
 */
case class SessionEvent(
                         eventId: String = AnalyticsEvent.generateEventId(),
                         tenantId: String,
                         timestamp: Instant = Instant.now(),
                         sessionId: String,
                         userId: Option[String] = None,
                         eventType: AnalyticsEventType, // SESSION_START or SESSION_END
                         durationSeconds: Option[Long] = None,
                         pageViews: Option[Int] = None,
                         actions: Option[Int] = None,
                         entryPath: Option[String] = None,
                         exitPath: Option[String] = None,
                         deviceType: Option[String] = None,
                         browser: Option[String] = None,
                         os: Option[String] = None,
                         country: Option[String] = None,
                         metadata: Map[String, String] = Map.empty
                       ) extends AnalyticsEvent

/**
 * Feature usage event
 */
case class FeatureUsageEvent(
                              eventId: String = AnalyticsEvent.generateEventId(),
                              tenantId: String,
                              timestamp: Instant = Instant.now(),
                              sessionId: Option[String] = None,
                              userId: Option[String] = None,
                              feature: String,
                              action: String,
                              durationMs: Option[Long] = None,
                              success: Boolean = true,
                              errorType: Option[String] = None,
                              metadata: Map[String, String] = Map.empty
                            ) extends AnalyticsEvent {
  val eventType: AnalyticsEventType = AnalyticsEventType.USER_ACTION
}

/**
 * Resource limit violation event
 */
case class LimitViolationEvent(
                                eventId: String = AnalyticsEvent.generateEventId(),
                                tenantId: String,
                                timestamp: Instant = Instant.now(),
                                resourceType: String,
                                currentValue: Double,
                                limitValue: Double,
                                rejected: Boolean,
                                metadata: Map[String, String] = Map.empty
                              ) extends AnalyticsEvent {
  val eventType: AnalyticsEventType = AnalyticsEventType.LIMIT_VIOLATION
}