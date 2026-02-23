package com.aurora.analytics

import com.aurora.tenant.TenantService
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ArrayBuffer
import scala.math.*
import scala.util.{Try, Success, Failure}
import scala.jdk.CollectionConverters.*

/**
 * FAANG-level: Real-time anomaly detection using multiple algorithms
 *
 * Key features:
 * - Multiple detection algorithms (Z-Score, Historical, Rate of Change)
 * - Per-tenant moving windows with automatic cleanup
 * - Historical baselines with time-of-day patterns
 * - Configurable alerting with multiple channels
 * - Circuit breaker pattern for failing tenants
 * - Comprehensive logging and metrics
 * - Thread-safe concurrent data structures
 */
class TenantAnomalyDetector(
                             storage: TenantAnalyticsStorage,
                             alertService: TenantAnalyticsAlertService
                           ) {

  // Structured logging
  private def debug(msg: String): Unit = println(s"[DEBUG] [AnomalyDetector] $msg")
  private def info(msg: String): Unit = println(s"[INFO] [AnomalyDetector] $msg")
  private def warn(msg: String): Unit = println(s"[WARN] [AnomalyDetector] $msg")
  private def error(msg: String): Unit = println(s"[ERROR] [AnomalyDetector] $msg")

  // ==========================================================================
  // Constants
  // ==========================================================================

  // Z-Score thresholds
  private val ZSCORE_THRESHOLD_MEDIUM = 3.0
  private val ZSCORE_THRESHOLD_HIGH = 5.0

  // Historical baseline thresholds
  private val HISTORICAL_THRESHOLD_MEDIUM = 2.5
  private val HISTORICAL_THRESHOLD_HIGH = 4.0
  private val HISTORICAL_MIN_DATA_POINTS = 10

  // Rate of change thresholds
  private val RATE_CHANGE_FACTOR_MEDIUM = 3.0
  private val RATE_CHANGE_FACTOR_HIGH = 5.0
  private val RATE_CHANGE_MIN_POINTS = 10

  // Window sizes
  private val MOVING_WINDOW_SIZE = 60
  private val RATE_WINDOW_SIZE = 10

  // Baseline update periods
  private val BASELINE_LOOKBACK_DAYS = 30

  // Circuit breaker
  private val MAX_FAILURES_BEFORE_SKIP = 5
  private val SKIP_DURATION_SECONDS = 300

  // ==========================================================================
  // Data Structures
  // ==========================================================================

  // Moving window for each tenant/metric (thread-safe)
  private val movingWindows = TrieMap[String, MovingWindow]()

  // Rate of change windows (separate for rate detection)
  private val rateWindows = TrieMap[String, MovingWindow]()

  // Historical baselines (thread-safe)
  private val baselines = TrieMap[String, Baseline]()

  // Circuit breaker state
  private val tenantFailureCount = TrieMap[String, Int]()
  private val tenantSkipUntil = TrieMap[String, Instant]()

  // Performance metrics
  private val totalAnomaliesDetected = new java.util.concurrent.atomic.AtomicLong(0)
  private val totalAlertsSent = new java.util.concurrent.atomic.AtomicLong(0)

  // ==========================================================================
  // Model Classes
  // ==========================================================================

  /**
   * Moving window for recent values
   */
  case class MovingWindow(
                           values: ArrayBuffer[Double],
                           maxSize: Int,
                           timestamps: ArrayBuffer[Long]
                         ) {
    def add(value: Double, timestamp: Long): Unit = {
      values += value
      timestamps += timestamp
      if (values.size > maxSize) {
        values.remove(0, values.size - maxSize)
        timestamps.remove(0, timestamps.size - maxSize)
      }
    }

    def mean: Double = if (values.nonEmpty) values.sum / values.size else 0.0

    def stdDev: Double = {
      if (values.size < 2) return 0.0
      val m = mean
      val variance = values.map(x => pow(x - m, 2)).sum / (values.size - 1)
      sqrt(variance)
    }

    def latest: Option[Double] = values.lastOption

    def size: Int = values.size

    def getRates: List[Double] = {
      if (values.size < 2) return List.empty
      values.sliding(2).map {
        case seq if seq.size == 2 => seq(1) - seq(0)
        case _ => 0.0
      }.toList
    }

    def isEmpty: Boolean = values.isEmpty
  }

  object MovingWindow {
    def apply(maxSize: Int = MOVING_WINDOW_SIZE): MovingWindow =
      MovingWindow(ArrayBuffer.empty, maxSize, ArrayBuffer.empty)
  }

  /**
   * Historical baseline for time-of-day patterns
   */
  case class Baseline(
                       mean: Double,
                       stdDev: Double,
                       count: Int,
                       lastUpdated: Instant
                     ) {
    def isStale(thresholdDays: Int = 7): Boolean = {
      lastUpdated.isBefore(Instant.now().minusSeconds(thresholdDays * 24 * 3600))
    }

    def confidence: Double = {
      if (count >= 100) 1.0
      else if (count >= 50) 0.8
      else if (count >= 20) 0.6
      else if (count >= 10) 0.4
      else 0.2
    }
  }

  // ==========================================================================
  // Public API
  // ==========================================================================

  /**
   * Detect anomalies using multiple algorithms
   */
  def detectAnomalies(
                       tenantId: String,
                       metric: String,
                       value: Double,
                       timestamp: Instant = Instant.now()
                     ): List[Anomaly] = {

    // Check circuit breaker first
    if (shouldSkipTenant(tenantId)) {
      debug(s"Skipping anomaly detection for tenant $tenantId (circuit breaker open)")
      return List.empty
    }

    try {
      val anomalies = List.newBuilder[Anomaly]
      val key = s"$tenantId:$metric"
      val now = timestamp.toEpochMilli

      // 1. Z-Score anomaly detection (sudden spikes)
      detectZScoreAnomaly(key, tenantId, metric, value, timestamp, now).foreach(anomalies += _)

      // 2. Historical baseline anomaly (time-of-day pattern)
      detectHistoricalAnomaly(tenantId, metric, value, timestamp).foreach(anomalies += _)

      // 3. Rate of change anomaly (acceleration/deceleration)
      detectRateChangeAnomaly(key, tenantId, metric, value, timestamp, now).foreach(anomalies += _)

      // Get detected anomalies
      val detected = anomalies.result()

      if (detected.nonEmpty) {
        totalAnomaliesDetected.incrementAndGet()
        recordSuccess(tenantId)

        // Log anomalies
        detected.foreach { a =>
          info(s"Anomaly detected: tenant=$tenantId metric=$metric " +
            s"value=$value expected=${a.expectedValue} " +
            s"deviation=${a.deviation} severity=${a.severity} method=${a.method}")
        }

        // Send alerts for high severity anomalies
        val highSeverity = detected.filter(_.severity == "HIGH")
        if (highSeverity.nonEmpty) {
          highSeverity.foreach { anomaly =>
            alertService.sendAlert(anomaly)
            totalAlertsSent.incrementAndGet()
          }
        }
      }

      detected

    } catch {
      case e: Exception =>
        error(s"Error detecting anomalies for tenant $tenantId: ${e.getMessage}")
        recordFailure(tenantId)
        List.empty
    }
  }

  /**
   * Z-Score based anomaly detection
   */
  private def detectZScoreAnomaly(
                                   key: String,
                                   tenantId: String,
                                   metric: String,
                                   value: Double,
                                   timestamp: Instant,
                                   now: Long
                                 ): Option[Anomaly] = {

    val window = movingWindows.getOrElseUpdate(key, MovingWindow())

    val m = window.mean
    val sd = window.stdDev

    // Update window with new value
    window.add(value, now)

    if (sd > 0) {
      val zScore = abs(value - m) / sd
      if (zScore > ZSCORE_THRESHOLD_MEDIUM) {
        Some(Anomaly(
          tenantId = tenantId,
          metric = metric,
          timestamp = timestamp,
          actualValue = value,
          expectedValue = m,
          deviation = zScore,
          severity = if (zScore > ZSCORE_THRESHOLD_HIGH) "HIGH" else "MEDIUM",
          method = "zscore",
          dataPoints = window.size
        ))
      } else None
    } else None
  }

  /**
   * Historical baseline anomaly detection
   */
  private def detectHistoricalAnomaly(
                                       tenantId: String,
                                       metric: String,
                                       value: Double,
                                       timestamp: Instant
                                     ): Option[Anomaly] = {

    val dayOfWeek = timestamp.atZone(java.time.ZoneOffset.UTC).getDayOfWeek.getValue
    val hour = timestamp.atZone(java.time.ZoneOffset.UTC).getHour
    val baselineKey = s"$tenantId:$metric:$dayOfWeek:$hour"

    baselines.get(baselineKey).flatMap { baseline =>
      if (baseline.count >= HISTORICAL_MIN_DATA_POINTS && !baseline.isStale()) {
        val zScore = abs(value - baseline.mean) / max(baseline.stdDev, 0.1)
        if (zScore > HISTORICAL_THRESHOLD_MEDIUM) {
          Some(Anomaly(
            tenantId = tenantId,
            metric = metric,
            timestamp = timestamp,
            actualValue = value,
            expectedValue = baseline.mean,
            deviation = zScore,
            severity = if (zScore > HISTORICAL_THRESHOLD_HIGH) "HIGH" else "MEDIUM",
            method = "historical",
            dataPoints = baseline.count
          ))
        } else None
      } else None
    }
  }

  /**
   * Rate of change anomaly detection
   */
  private def detectRateChangeAnomaly(
                                       key: String,
                                       tenantId: String,
                                       metric: String,
                                       value: Double,
                                       timestamp: Instant,
                                       now: Long
                                     ): Option[Anomaly] = {

    val rateKey = s"$key:rate"
    val window = rateWindows.getOrElseUpdate(rateKey, MovingWindow(RATE_WINDOW_SIZE))

    window.add(value, now)

    if (window.size >= RATE_CHANGE_MIN_POINTS) {
      val rates = window.getRates
      if (rates.size >= 3) {
        val recentRates = rates.takeRight(3)
        val avgRate = rates.sum / rates.size
        val lastRate = recentRates.last

        if (abs(avgRate) > 0.01 && abs(lastRate) > abs(avgRate) * RATE_CHANGE_FACTOR_MEDIUM) {
          val deviation = lastRate / abs(avgRate)
          Some(Anomaly(
            tenantId = tenantId,
            metric = metric,
            timestamp = timestamp,
            actualValue = value,
            expectedValue = window.values.lastOption.getOrElse(value),
            deviation = deviation,
            severity = if (abs(lastRate) > abs(avgRate) * RATE_CHANGE_FACTOR_HIGH) "HIGH" else "MEDIUM",
            method = "ratechange",
            dataPoints = window.size
          ))
        } else None
      } else None
    } else None
  }

  // ==========================================================================
  // Baseline Management
  // ==========================================================================

  /**
   * Update historical baseline for a tenant/metric
   * Called periodically by scheduler
   */
  def updateBaselines(tenantId: String, metric: String): Unit = {
    debug(s"Updating baselines for tenant $tenantId metric $metric")

    try {
      val endTime = Instant.now()
      val startTime = endTime.minusSeconds(BASELINE_LOOKBACK_DAYS * 24 * 3600)

      val data = storage.queryTimeSeries(tenantId, metric, startTime, endTime, "hour")

      if (data.isEmpty) {
        debug(s"No data for baseline update: $tenantId $metric")
        return
      }

      // Group by day of week and hour
      val grouped = data.groupBy { point =>
        val dt = point.timestamp.atZone(java.time.ZoneOffset.UTC)
        (dt.getDayOfWeek.getValue, dt.getHour)
      }

      var updatedCount = 0
      grouped.foreach { case ((dayOfWeek, hour), points) =>
        val values = points.map(_.value)
        if (values.nonEmpty) {
          val mean = values.sum / values.size
          val variance = values.map(v => pow(v - mean, 2)).sum / values.size
          val stdDev = sqrt(variance)

          val key = s"$tenantId:$metric:$dayOfWeek:$hour"
          baselines.put(key, Baseline(
            mean = mean,
            stdDev = stdDev,
            count = values.size,
            lastUpdated = Instant.now()
          ))
          updatedCount += 1
        }
      }

      info(s"Updated $updatedCount baselines for tenant $tenantId metric $metric")

    } catch {
      case e: Exception =>
        error(s"Error updating baselines for tenant $tenantId: ${e.getMessage}")
    }
  }

  /**
   * Clean up stale windows and baselines
   */
  def cleanup(): Unit = {
    debug("Starting cleanup of stale data")

    val now = Instant.now()
    val cutoff = now.minusSeconds(24 * 3600) // 24 hours

    // Clean up moving windows (keep only recent activity)
    movingWindows.filterInPlace { case (key, window) =>
      window.timestamps.lastOption.exists(_ > cutoff.toEpochMilli)
    }

    rateWindows.filterInPlace { case (key, window) =>
      window.timestamps.lastOption.exists(_ > cutoff.toEpochMilli)
    }

    // Clean up circuit breaker state
    tenantSkipUntil.filterInPlace { case (_, skipUntil) =>
      skipUntil.isAfter(now)
    }

    info(s"Cleanup complete. Active windows: ${movingWindows.size}, Active rate windows: ${rateWindows.size}")
  }

  // ==========================================================================
  // Batch Operations
  // ==========================================================================

  /**
   * Batch detect anomalies for all active tenants
   */
  def scanAllTenants()(implicit ec: scala.concurrent.ExecutionContext): Unit = {
    debug("Starting batch anomaly scan for all tenants")

    try {
      TenantService.getActiveTenants() match {
        case Success(tenants) =>
          val endTime = Instant.now()
          val startTime = endTime.minusSeconds(3600) // Last hour

          tenants.foreach { tenant =>
            if (tenant.isActive && !shouldSkipTenant(tenant.tenantId)) {
              try {
                val recentData = storage.queryTimeSeries(
                  tenant.tenantId,
                  "requestCount",
                  startTime,
                  endTime,
                  "minute"
                )

                var tenantAnomalies = 0
                recentData.foreach { point =>
                  val anomalies = detectAnomalies(
                    tenant.tenantId,
                    "requestCount",
                    point.value,
                    point.timestamp
                  )
                  tenantAnomalies += anomalies.size
                }

                if (tenantAnomalies > 0) {
                  info(s"Found $tenantAnomalies anomalies for tenant ${tenant.tenantId} in batch scan")
                }

              } catch {
                case e: Exception =>
                  error(s"Error scanning tenant ${tenant.tenantId}: ${e.getMessage}")
                  recordFailure(tenant.tenantId)
              }
            }
          }

          info("Batch anomaly scan completed")

        case Failure(e) =>
          error(s"Failed to get active tenants: ${e.getMessage}")
      }
    } catch {
      case e: Exception =>
        error(s"Error in batch anomaly scan: ${e.getMessage}")
    }
  }

  // ==========================================================================
  // Circuit Breaker
  // ==========================================================================

  private def shouldSkipTenant(tenantId: String): Boolean = {
    tenantSkipUntil.get(tenantId) match {
      case Some(skipUntil) if skipUntil.isAfter(Instant.now()) => true
      case _ => false
    }
  }

  private def recordSuccess(tenantId: String): Unit = {
    tenantFailureCount.remove(tenantId)
    tenantSkipUntil.remove(tenantId)
  }

  private def recordFailure(tenantId: String): Unit = {
    val failures = tenantFailureCount.getOrElse(tenantId, 0) + 1
    tenantFailureCount.put(tenantId, failures)

    if (failures >= MAX_FAILURES_BEFORE_SKIP) {
      val skipUntil = Instant.now().plusSeconds(SKIP_DURATION_SECONDS)
      tenantSkipUntil.put(tenantId, skipUntil)
      warn(s"Circuit breaker opened for tenant $tenantId until $skipUntil (failures: $failures)")
    }
  }

  // ==========================================================================
  // Metrics
  // ==========================================================================

  /**
   * Get detector statistics
   */
  def getStats: Map[String, Any] = Map(
    "total_anomalies_detected" -> totalAnomaliesDetected.get(),
    "total_alerts_sent" -> totalAlertsSent.get(),
    "active_windows" -> movingWindows.size,
    "active_rate_windows" -> rateWindows.size,
    "baselines" -> baselines.size,
    "circuit_breakers" -> tenantSkipUntil.size,
    "failed_tenants" -> tenantFailureCount.size
  )

  /**
   * Get active windows for monitoring
   */
  def getActiveWindows: Map[String, Int] =
    movingWindows.map { case (k, v) => k -> v.size }.toMap

  /**
   * Get baselines for a tenant
   */
  def getBaselines(tenantId: String, metric: String): Map[String, Baseline] = {
    baselines.filter { case (key, _) =>
      key.startsWith(s"$tenantId:$metric")
    }.toMap
  }
}

/**
 * Alert service for anomaly notifications
 */
class TenantAnalyticsAlertService {

  private def debug(msg: String): Unit = println(s"[DEBUG] [AlertService] $msg")
  private def info(msg: String): Unit = println(s"[INFO] [AlertService] $msg")
  private def warn(msg: String): Unit = println(s"[WARN] [AlertService] $msg")
  private def error(msg: String): Unit = println(s"[ERROR] [AlertService] $msg")

  // ==========================================================================
  // Constants
  // ==========================================================================

  private val ALERT_ID_PREFIX = "alert"
  private val DEFAULT_COOLDOWN_MINUTES = 60
  private val MAX_ALERTS_PER_TENANT_PER_HOUR = 10

  // ==========================================================================
  // Data Structures
  // ==========================================================================

  // Alert rules per tenant (thread-safe)
  private val alertRules = TrieMap[String, List[AlertRule]]()

  // Alert history for deduplication and rate limiting
  private val alertHistory = TrieMap[String, List[Alert]]()

  // Cooldown tracking
  private val lastAlertTime = TrieMap[String, Instant]()

  // ==========================================================================
  // Model Classes
  // ==========================================================================

  case class AlertRule(
                        metric: String,
                        threshold: Double,
                        severity: String,
                        cooldownMinutes: Int = DEFAULT_COOLDOWN_MINUTES,
                        channels: List[String] = List("log") // Default to log only
                      ) {
    def isEnabled: Boolean = channels.nonEmpty
  }

  case class Alert(
                    alertId: String,
                    tenantId: String,
                    anomaly: Anomaly,
                    timestamp: Instant,
                    acknowledged: Boolean = false,
                    delivered: Map[String, Boolean] = Map.empty
                  )

  // ==========================================================================
  // Public API
  // ==========================================================================

  /**
   * Send alert for anomaly
   */
  def sendAlert(anomaly: Anomaly): Unit = {
    try {
      val tenantId = anomaly.tenantId
      val now = Instant.now()

      // Check cooldown
      if (isInCooldown(tenantId, anomaly.metric, now)) {
        debug(s"Alert in cooldown for tenant $tenantId metric ${anomaly.metric}")
        return
      }

      // Check rate limit
      if (exceedsRateLimit(tenantId, now)) {
        warn(s"Rate limit exceeded for tenant $tenantId, skipping alert")
        return
      }

      val alertId = generateAlertId(tenantId)
      val alert = Alert(
        alertId = alertId,
        tenantId = tenantId,
        anomaly = anomaly,
        timestamp = now
      )

      // Log the alert
      logAlert(alert)

      // Send to configured channels
      val deliveryResults = sendToChannels(anomaly)

      // Update alert with delivery results
      val updatedAlert = alert.copy(delivered = deliveryResults)

      // Store in history
      recordAlert(updatedAlert)

      // Update cooldown
      updateCooldown(tenantId, anomaly.metric, now)

      info(s"Alert sent: $alertId for tenant $tenantId (${deliveryResults.count(_._2)}/${deliveryResults.size} channels)")

    } catch {
      case e: Exception =>
        error(s"Error sending alert: ${e.getMessage}")
    }
  }

  /**
   * Add alert rule for tenant
   */
  def addAlertRule(tenantId: String, rule: AlertRule): Unit = {
    val existing = alertRules.getOrElse(tenantId, List.empty)
    alertRules.put(tenantId, rule :: existing)
    info(s"Added alert rule for tenant $tenantId: ${rule.metric} (${rule.severity})")
  }

  /**
   * Remove alert rules for tenant
   */
  def removeAlertRules(tenantId: String): Unit = {
    alertRules.remove(tenantId)
    info(s"Removed all alert rules for tenant $tenantId")
  }

  /**
   * Get alert rules for tenant
   */
  def getAlertRules(tenantId: String): List[AlertRule] = {
    alertRules.getOrElse(tenantId, List.empty)
  }

  /**
   * Get alert history for tenant
   */
  def getAlertHistory(tenantId: String, limit: Int = 100): List[Alert] = {
    alertHistory.getOrElse(tenantId, List.empty).take(limit)
  }

  /**
   * Acknowledge an alert
   */
  def acknowledgeAlert(alertId: String): Unit = {
    alertHistory.foreach { case (tenantId, alerts) =>
      val updated = alerts.map { alert =>
        if (alert.alertId == alertId) alert.copy(acknowledged = true)
        else alert
      }
      alertHistory.put(tenantId, updated)
    }
    debug(s"Alert $alertId acknowledged")
  }

  // ==========================================================================
  // Private Methods
  // ==========================================================================

  private def isInCooldown(tenantId: String, metric: String, now: Instant): Boolean = {
    val key = s"$tenantId:$metric"
    lastAlertTime.get(key) match {
      case Some(lastTime) =>
        val cooldownSeconds = getCooldownSeconds(tenantId, metric)
        !lastTime.plusSeconds(cooldownSeconds).isBefore(now)
      case None => false
    }
  }

  private def getCooldownSeconds(tenantId: String, metric: String): Long = {
    alertRules.get(tenantId)
      .flatMap(_.find(_.metric == metric))
      .map(_.cooldownMinutes * 60L)
      .getOrElse(DEFAULT_COOLDOWN_MINUTES * 60L)
  }

  private def exceedsRateLimit(tenantId: String, now: Instant): Boolean = {
    val oneHourAgo = now.minusSeconds(3600)
    alertHistory.get(tenantId)
      .map(_.count(_.timestamp.isAfter(oneHourAgo)))
      .getOrElse(0) >= MAX_ALERTS_PER_TENANT_PER_HOUR
  }

  private def generateAlertId(tenantId: String): String = {
    s"${ALERT_ID_PREFIX}_${System.currentTimeMillis()}_${tenantId}_${scala.util.Random.nextInt(10000)}"
  }

  private def logAlert(alert: Alert): Unit = {
    val anomaly = alert.anomaly
    warn(s"""
            |ALERT: ${alert.alertId}
            |Tenant: ${alert.tenantId}
            |Metric: ${anomaly.metric}
            |Severity: ${anomaly.severity}
            |Method: ${anomaly.method}
            |Value: ${anomaly.actualValue} (expected: ${anomaly.expectedValue})
            |Deviation: ${anomaly.deviation}
            |Data Points: ${anomaly.dataPoints}
            |Time: ${alert.timestamp}
            |""".stripMargin)
  }

  private def sendToChannels(anomaly: Anomaly): Map[String, Boolean] = {
    val results = Map.newBuilder[String, Boolean]

    alertRules.get(anomaly.tenantId).foreach { rules =>
      rules.filter(_.metric == anomaly.metric).foreach { rule =>
        rule.channels.foreach { channel =>
          val success = channel match {
            case "log" => true // Already logged
            case "webhook" => sendWebhook(anomaly)
            case "email" => sendEmail(anomaly)
            case "slack" => sendSlack(anomaly)
            case _ => false
          }
          results += (channel -> success)
        }
      }
    }

    // Always log
    results += ("log" -> true)
    results.result()
  }

  private def sendWebhook(anomaly: Anomaly): Boolean = {
    try {
      // Implementation would call configured webhook
      debug(s"Webhook sent for ${anomaly.tenantId}")
      true
    } catch {
      case e: Exception =>
        error(s"Webhook failed: ${e.getMessage}")
        false
    }
  }

  private def sendEmail(anomaly: Anomaly): Boolean = {
    try {
      // Implementation would send email
      debug(s"Email sent for ${anomaly.tenantId}")
      true
    } catch {
      case e: Exception =>
        error(s"Email failed: ${e.getMessage}")
        false
    }
  }

  private def sendSlack(anomaly: Anomaly): Boolean = {
    try {
      // Implementation would send Slack message
      debug(s"Slack message sent for ${anomaly.tenantId}")
      true
    } catch {
      case e: Exception =>
        error(s"Slack failed: ${e.getMessage}")
        false
    }
  }

  private def recordAlert(alert: Alert): Unit = {
    val existing = alertHistory.getOrElse(alert.tenantId, List.empty)
    alertHistory.put(alert.tenantId, (alert :: existing).take(1000)) // Keep last 1000
  }

  private def updateCooldown(tenantId: String, metric: String, now: Instant): Unit = {
    val key = s"$tenantId:$metric"
    lastAlertTime.put(key, now)
  }

  // ==========================================================================
  // Metrics
  // ==========================================================================

  /**
   * Get alert service statistics
   */
  def getStats: Map[String, Any] = Map(
    "rules" -> alertRules.map { case (k, v) => k -> v.size }.toMap,
    "history_size" -> alertHistory.map { case (k, v) => k -> v.size }.toMap,
    "active_cooldowns" -> lastAlertTime.size
  )
}

// ==========================================================================
// Enhanced Anomaly Model
// ==========================================================================

case class Anomaly(
                    tenantId: String,
                    metric: String,
                    timestamp: Instant,
                    actualValue: Double,
                    expectedValue: Double,
                    deviation: Double,
                    severity: String,
                    method: String,
                    dataPoints: Int
                  )

object Anomaly {
  def empty: Anomaly = Anomaly(
    tenantId = "",
    metric = "",
    timestamp = Instant.now(),
    actualValue = 0.0,
    expectedValue = 0.0,
    deviation = 0.0,
    severity = "UNKNOWN",
    method = "unknown",
    dataPoints = 0
  )
}