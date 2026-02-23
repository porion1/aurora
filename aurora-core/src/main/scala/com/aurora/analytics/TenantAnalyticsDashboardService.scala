package com.aurora.analytics

import com.aurora.tenant.{TenantContext, TenantResourceService, TenantResourceUsageManager}
import java.time.{Instant, Duration}
import scala.concurrent.{ExecutionContext, Future}
import scala.collection.concurrent.TrieMap
import scala.util.{Try, Success, Failure}

/**
 * FAANG-level: Dashboard data aggregation service
 *
 * Key features:
 * - Multi-level caching with TTL
 * - Parallel data fetching
 * - Real-time dashboard support
 * - Configurable widgets
 * - Comprehensive error handling
 * - Performance monitoring
 * - Type-safe responses
 */
class TenantAnalyticsDashboardService(
                                       storage: TenantAnalyticsStorage,
                                       queryService: TenantAnalyticsQueryService,
                                       predictor: TenantPredictiveAnalytics,
                                       anomalyDetector: TenantAnomalyDetector
                                     ) {

  // Structured logging
  private def debug(msg: String): Unit = println(s"[DEBUG] [DashboardService] $msg")
  private def info(msg: String): Unit = println(s"[INFO] [DashboardService] $msg")
  private def warn(msg: String): Unit = println(s"[WARN] [DashboardService] $msg")
  private def error(msg: String): Unit = println(s"[ERROR] [DashboardService] $msg")

  // ==========================================================================
  // Constants
  // ==========================================================================

  private val CACHE_TTL_MS = 5 * 60 * 1000 // 5 minutes
  private val CACHE_MAX_SIZE = 1000
  private val DEFAULT_TIME_RANGE = "24h"
  private val FORECAST_PERIODS = 24

  // Widget types
  private val WIDGET_REQUESTS_TIMESERIES = "requests.timeseries"
  private val WIDGET_RESPONSE_TIME_PERCENTILES = "responseTime.percentiles"
  private val WIDGET_SESSIONS_ACTIVE = "sessions.active"
  private val WIDGET_LIMITS_STATUS = "limits.status"
  private val WIDGET_ERROR_RATE = "error.rate"
  private val WIDGET_TOP_ENDPOINTS = "top.endpoints"

  // Thresholds
  private val RESPONSE_TIME_WARNING_MS = 500
  private val RESPONSE_TIME_CRITICAL_MS = 1000

  // ==========================================================================
  // Cache
  // ==========================================================================

  private val dashboardCache = TrieMap[String, CachedDashboard]()
  private val widgetCache = TrieMap[String, CachedWidget]()

  // Cache statistics
  private val cacheHits = new java.util.concurrent.atomic.AtomicLong(0)
  private val cacheMisses = new java.util.concurrent.atomic.AtomicLong(0)

  case class CachedDashboard(
                              data: DashboardData,
                              timestamp: Long
                            ) {
    def isExpired: Boolean = System.currentTimeMillis() - timestamp > CACHE_TTL_MS
    def ageMs: Long = System.currentTimeMillis() - timestamp
  }

  case class CachedWidget(
                           data: Map[String, Any],
                           timestamp: Long
                         ) {
    def isExpired: Boolean = System.currentTimeMillis() - timestamp > CACHE_TTL_MS
  }

  // ==========================================================================
  // Public API - Main Dashboard
  // ==========================================================================

  /**
   * Get main dashboard for tenant with caching
   */
  def getMainDashboard(
                        tenantId: String,
                        timeRange: String = DEFAULT_TIME_RANGE
                      )(implicit ec: ExecutionContext): Future[DashboardData] = {

    val cacheKey = s"$tenantId:main:$timeRange"
    val startTime = System.currentTimeMillis()

    // Check cache first
    dashboardCache.get(cacheKey) match {
      case Some(cached) if !cached.isExpired =>
        cacheHits.incrementAndGet()
        debug(s"Cache hit for dashboard $cacheKey (age: ${cached.ageMs}ms)")
        return Future.successful(cached.data)

      case _ =>
        cacheMisses.incrementAndGet()
        debug(s"Cache miss for dashboard $cacheKey")
    }

    // Calculate time range
    val (endTime, startTimeObj) = calculateTimeRange(timeRange)

    // Fetch all dashboard components in parallel
    val summaryFuture = getSummaryFuture(tenantId, startTimeObj, endTime)
    val timeSeriesFuture = getTimeSeriesWidgetsFuture(tenantId, startTimeObj, endTime)
    val topEndpointsFuture = getTopEndpointsFuture(tenantId, startTimeObj, endTime)
    val forecastFuture = getForecastWidgetFuture(tenantId)
    val anomaliesFuture = getRecentAnomaliesFuture(tenantId, startTimeObj, endTime)
    val comparisonFuture = getComparisonWidgetFuture(tenantId, startTimeObj, endTime)
    val limitsFuture = getLimitsWidgetFuture(tenantId)

    // Combine all futures
    for {
      summary <- summaryFuture
      timeSeries <- timeSeriesFuture
      topEndpoints <- topEndpointsFuture
      forecast <- forecastFuture
      anomalies <- anomaliesFuture
      comparison <- comparisonFuture
      limits <- limitsFuture
    } yield {
      val dashboardData = DashboardData(
        tenantId = tenantId,
        timeRange = timeRange,
        summary = summary,
        timeSeries = timeSeries,
        topEndpoints = topEndpoints,
        forecast = forecast,
        anomalies = anomalies,
        comparison = comparison,
        limits = limits,
        lastUpdated = Instant.now(),
        responseTimeMs = System.currentTimeMillis() - startTime
      )

      // Manage cache size
      if (dashboardCache.size >= CACHE_MAX_SIZE) {
        evictOldestCacheEntries()
      }

      dashboardCache.put(cacheKey, CachedDashboard(dashboardData, System.currentTimeMillis()))
      info(s"Dashboard generated for tenant $tenantId in ${dashboardData.responseTimeMs}ms")

      dashboardData
    }
  }

  /**
   * Calculate time range based on string
   */
  private def calculateTimeRange(timeRange: String): (Instant, Instant) = {
    val endTime = Instant.now()
    val startTime = timeRange match {
      case "1h" => endTime.minusSeconds(3600)
      case "24h" => endTime.minusSeconds(24 * 3600)
      case "7d" => endTime.minusSeconds(7 * 24 * 3600)
      case "30d" => endTime.minusSeconds(30 * 24 * 3600)
      case _ => endTime.minusSeconds(24 * 3600)
    }
    (endTime, startTime)
  }

  // ==========================================================================
  // Public API - Real-time Dashboard
  // ==========================================================================

  /**
   * Get real-time dashboard (low-latency, no caching)
   */
  def getRealtimeDashboard(tenantId: String): RealtimeDashboard = {
    try {
      val usage = TenantResourceUsageManager.get(tenantId)
      val context = TenantAnalyticsContext

      RealtimeDashboard(
        tenantId = tenantId,
        activeSessions = context.getActiveSessionsCount,
        concurrentRequests = usage.map(_.getConcurrentRequests).getOrElse(0),
        requestsLastMinute = usage.map(_.getRequestRate(60)).getOrElse(0),
        requestsLastHour = usage.map(_.getRequestRate(3600)).getOrElse(0),
        currentMemoryMb = usage.map(_.getCurrentMemoryMb).getOrElse(0),
        peakMemoryMb = usage.map(_.getPeakMemoryMb).getOrElse(0),
        timestamp = Instant.now()
      )
    } catch {
      case e: Exception =>
        error(s"Error getting real-time dashboard: ${e.getMessage}")
        RealtimeDashboard.empty(tenantId)
    }
  }

  // ==========================================================================
  // Public API - Widgets
  // ==========================================================================

  /**
   * Get individual widget data with caching
   */
  def getWidget(
                 tenantId: String,
                 widgetType: String,
                 params: Map[String, String] = Map.empty
               ): Map[String, Any] = {

    val cacheKey = s"$tenantId:widget:$widgetType:${params.hashCode()}"
    val startTime = System.currentTimeMillis()

    // Check cache
    widgetCache.get(cacheKey) match {
      case Some(cached) if !cached.isExpired =>
        cacheHits.incrementAndGet()
        return cached.data
      case _ =>
        cacheMisses.incrementAndGet()
    }

    // Generate widget data
    val widgetData = widgetType match {
      case WIDGET_REQUESTS_TIMESERIES =>
        generateRequestsTimeseriesWidget(tenantId, params)
      case WIDGET_RESPONSE_TIME_PERCENTILES =>
        generateResponseTimeWidget(tenantId, params)
      case WIDGET_SESSIONS_ACTIVE =>
        generateSessionsActiveWidget(tenantId, params)
      case WIDGET_LIMITS_STATUS =>
        generateLimitsStatusWidget(tenantId, params)
      case WIDGET_ERROR_RATE =>
        generateErrorRateWidget(tenantId, params)
      case WIDGET_TOP_ENDPOINTS =>
        generateTopEndpointsWidget(tenantId, params)
      case _ =>
        Map("error" -> s"Unknown widget type: $widgetType")
    }

    // Cache if not error
    if (!widgetData.contains("error")) {
      widgetCache.put(cacheKey, CachedWidget(widgetData, System.currentTimeMillis()))
    }

    val responseTime = System.currentTimeMillis() - startTime
    if (responseTime > 100) {
      debug(s"Widget $widgetType took ${responseTime}ms")
    }

    widgetData
  }

  /**
   * Generate requests timeseries widget
   */
  private def generateRequestsTimeseriesWidget(
                                                tenantId: String,
                                                params: Map[String, String]
                                              ): Map[String, Any] = {

    val hours = params.get("hours").flatMap(h => Try(h.toInt).toOption).getOrElse(24)
    val granularity = params.getOrElse("granularity", "minute")
    val endTime = Instant.now()
    val startTime = endTime.minusSeconds(hours * 3600)

    val data = storage.queryTimeSeries(tenantId, "requestCount", startTime, endTime, granularity)

    if (data.isEmpty) {
      return Map(
        "type" -> "line",
        "title" -> s"Requests (last $hours hours)",
        "labels" -> List.empty,
        "datasets" -> List(Map(
          "label" -> "Requests",
          "data" -> List.empty
        )),
        "warning" -> "No data available"
      )
    }

    Map(
      "type" -> "line",
      "title" -> s"Requests (last $hours hours)",
      "labels" -> data.map(_.timestamp.toString),
      "datasets" -> List(Map(
        "label" -> "Requests",
        "data" -> data.map(_.value),
        "borderColor" -> "#3b82f6",
        "backgroundColor" -> "rgba(59, 130, 246, 0.1)"
      )),
      "metadata" -> Map(
        "total" -> data.map(_.value).sum,
        "average" -> data.map(_.value).sum / data.size,
        "peak" -> data.map(_.value).max,
        "dataPoints" -> data.size
      )
    )
  }

  /**
   * Generate response time percentiles widget
   */
  private def generateResponseTimeWidget(
                                          tenantId: String,
                                          params: Map[String, String]
                                        ): Map[String, Any] = {

    val hours = params.get("hours").flatMap(h => Try(h.toInt).toOption).getOrElse(24)
    val endTime = Instant.now()
    val startTime = endTime.minusSeconds(hours * 3600)

    val data = storage.queryTimeSeries(tenantId, "avgResponseTime", startTime, endTime, "minute")

    if (data.isEmpty) {
      return Map(
        "type" -> "gauge",
        "title" -> "Response Time",
        "value" -> 0.0,
        "thresholds" -> Map("warning" -> RESPONSE_TIME_WARNING_MS, "critical" -> RESPONSE_TIME_CRITICAL_MS),
        "unit" -> "ms",
        "warning" -> "No data available"
      )
    }

    val sortedTimes = data.map(_.value).sorted
    val p95 = sortedTimes((sortedTimes.size * 0.95).toInt)
    val p99 = sortedTimes((sortedTimes.size * 0.99).toInt)
    val current = data.lastOption.map(_.value).getOrElse(0.0)

    Map(
      "type" -> "gauge",
      "title" -> "Response Time",
      "value" -> current,
      "p95" -> p95,
      "p99" -> p99,
      "thresholds" -> Map("warning" -> RESPONSE_TIME_WARNING_MS, "critical" -> RESPONSE_TIME_CRITICAL_MS),
      "unit" -> "ms",
      "status" -> getResponseTimeStatus(current)
    )
  }

  /**
   * Generate active sessions widget
   */
  private def generateSessionsActiveWidget(
                                            tenantId: String,
                                            params: Map[String, String]
                                          ): Map[String, Any] = {

    val activeSessions = TenantAnalyticsContext.getActiveSessionsCount
    val sessions = TenantAnalyticsContext.getActiveSessions

    Map(
      "type" -> "counter",
      "title" -> "Active Sessions",
      "value" -> activeSessions,
      "unit" -> "sessions",
      "details" -> Map(
        "authenticated" -> sessions.count(_._2.userId.isDefined),
        "anonymous" -> sessions.count(_._2.userId.isEmpty),
        "totalPageViews" -> sessions.values.map(_.pageViews).sum
      )
    )
  }

  /**
   * Generate limits status widget
   */
  private def generateLimitsStatusWidget(
                                          tenantId: String,
                                          params: Map[String, String]
                                        ): Map[String, Any] = {

    val status = TenantResourceService.getLimitStatus(tenantId)

    if (status.isEmpty) {
      return Map(
        "type" -> "progress",
        "title" -> "Resource Usage",
        "resources" -> List.empty,
        "warning" -> "No limits configured"
      )
    }

    Map(
      "type" -> "progress",
      "title" -> "Resource Usage",
      "resources" -> status.map { case (resource, metrics) =>
        Map(
          "name" -> resource,
          "current" -> metrics("current"),
          "limit" -> metrics("limit"),
          "percentage" -> metrics("percentage"),
          "status" -> metrics("status"),
          "unit" -> metrics("unit")
        )
      }.toList.sortBy(-_("percentage").asInstanceOf[Double])
    )
  }

  /**
   * Generate error rate widget
   */
  private def generateErrorRateWidget(
                                       tenantId: String,
                                       params: Map[String, String]
                                     ): Map[String, Any] = {

    val hours = params.get("hours").flatMap(h => Try(h.toInt).toOption).getOrElse(1)
    val endTime = Instant.now()
    val startTime = endTime.minusSeconds(hours * 3600)

    val requests = storage.queryTimeSeries(tenantId, "requestCount", startTime, endTime, "minute")
    val errors = storage.queryTimeSeries(tenantId, "errorCount", startTime, endTime, "minute")

    val totalRequests = requests.map(_.value).sum
    val totalErrors = errors.map(_.value).sum
    val errorRate = if (totalRequests > 0) (totalErrors / totalRequests) * 100 else 0.0

    Map(
      "type" -> "pie",
      "title" -> s"Error Rate (last $hours hour)",
      "data" -> List(
        Map("label" -> "Successful", "value" -> (totalRequests - totalErrors), "color" -> "#10b981"),
        Map("label" -> "Errors", "value" -> totalErrors, "color" -> "#ef4444")
      ),
      "errorRate" -> errorRate,
      "totalRequests" -> totalRequests.toLong,
      "totalErrors" -> totalErrors.toLong
    )
  }

  /**
   * Generate top endpoints widget
   */
  private def generateTopEndpointsWidget(
                                          tenantId: String,
                                          params: Map[String, String]
                                        ): Map[String, Any] = {

    val hours = params.get("hours").flatMap(h => Try(h.toInt).toOption).getOrElse(24)
    val limit = params.get("limit").flatMap(l => Try(l.toInt).toOption).getOrElse(10)
    val endTime = Instant.now()
    val startTime = endTime.minusSeconds(hours * 3600)

    // This would need to query endpoint statistics from storage
    // For now, return placeholder
    Map(
      "type" -> "table",
      "title" -> s"Top $limit Endpoints",
      "headers" -> List("Endpoint", "Requests", "Avg Response Time", "Error Rate"),
      "rows" -> List.empty,
      "warning" -> "Endpoint tracking not fully implemented"
    )
  }

  // ==========================================================================
  // Private Helper Methods for Dashboard Components
  // ==========================================================================

  private def getSummaryFuture(
                                tenantId: String,
                                startTime: Instant,
                                endTime: Instant
                              )(implicit ec: ExecutionContext): Future[TenantAnalyticsSummary] = Future {
    storage.getTenantSummary(tenantId, hoursBetween(startTime, endTime))
      .getOrElse(TenantAnalyticsSummary.empty(tenantId))
  }

  private def getTimeSeriesWidgetsFuture(
                                          tenantId: String,
                                          startTime: Instant,
                                          endTime: Instant
                                        )(implicit ec: ExecutionContext): Future[Map[String, List[TimeSeriesDataPoint]]] = Future {
    Map(
      "requests" -> storage.queryTimeSeries(tenantId, "requestCount", startTime, endTime, "minute"),
      "errors" -> storage.queryTimeSeries(tenantId, "errorCount", startTime, endTime, "minute"),
      "latency" -> storage.queryTimeSeries(tenantId, "avgResponseTime", startTime, endTime, "minute")
    )
  }

  private def getTopEndpointsFuture(
                                     tenantId: String,
                                     startTime: Instant,
                                     endTime: Instant
                                   )(implicit ec: ExecutionContext): Future[List[Map[String, Any]]] = Future {
    // This would query top endpoints from storage
    List.empty
  }

  private def getForecastWidgetFuture(
                                       tenantId: String
                                     )(implicit ec: ExecutionContext): Future[Option[ForecastResult]] = Future {
    Try(predictor.forecastUsage(tenantId, "requestCount", FORECAST_PERIODS)) match {
      case Success(forecast) => Some(forecast)
      case Failure(e) =>
        error(s"Failed to generate forecast: ${e.getMessage}")
        None
    }
  }

  private def getRecentAnomaliesFuture(
                                        tenantId: String,
                                        startTime: Instant,
                                        endTime: Instant
                                      )(implicit ec: ExecutionContext): Future[List[Anomaly]] = Future {
    // This would query recent anomalies
    List.empty
  }

  private def getComparisonWidgetFuture(
                                         tenantId: String,
                                         startTime: Instant,
                                         endTime: Instant
                                       )(implicit ec: ExecutionContext): Future[Map[String, ComparisonResult]] = Future {
    Map(
      "requests" -> queryService.getComparisonMetrics(tenantId, "requestCount", (startTime, endTime)),
      "errors" -> queryService.getComparisonMetrics(tenantId, "errorCount", (startTime, endTime)),
      "latency" -> queryService.getComparisonMetrics(tenantId, "avgResponseTime", (startTime, endTime))
    )
  }

  private def getLimitsWidgetFuture(
                                     tenantId: String
                                   )(implicit ec: ExecutionContext): Future[Map[String, Any]] = Future {
    generateLimitsStatusWidget(tenantId, Map.empty)
  }

  // ==========================================================================
  // Utility Methods
  // ==========================================================================

  private def hoursBetween(start: Instant, end: Instant): Int = {
    Duration.between(start, end).toHours.toInt
  }

  private def getResponseTimeStatus(responseTime: Double): String = {
    if (responseTime >= RESPONSE_TIME_CRITICAL_MS) "CRITICAL"
    else if (responseTime >= RESPONSE_TIME_WARNING_MS) "WARNING"
    else "HEALTHY"
  }

  private def evictOldestCacheEntries(): Unit = {
    val entriesToRemove = dashboardCache.toList
      .sortBy(_._2.timestamp)
      .take(100) // Remove 100 oldest
      .map(_._1)

    entriesToRemove.foreach(dashboardCache.remove)
    debug(s"Evicted ${entriesToRemove.size} oldest cache entries")
  }

  // ==========================================================================
  // Cache Statistics
  // ==========================================================================

  def getCacheStats: Map[String, Any] = {
    val hits = cacheHits.get()
    val misses = cacheMisses.get()
    val total = hits + misses

    Map(
      "dashboardCache" -> Map(
        "size" -> dashboardCache.size,
        "hits" -> hits,
        "misses" -> misses,
        "hitRatio" -> (if (total > 0) hits.toDouble / total else 0.0)
      ),
      "widgetCache" -> Map(
        "size" -> widgetCache.size
      )
    )
  }

  def clearCache(): Unit = {
    dashboardCache.clear()
    widgetCache.clear()
    cacheHits.set(0)
    cacheMisses.set(0)
    info("Dashboard cache cleared")
  }
}

// ==========================================================================
// Model Classes
// ==========================================================================

case class DashboardData(
                          tenantId: String,
                          timeRange: String,
                          summary: TenantAnalyticsSummary,
                          timeSeries: Map[String, List[TimeSeriesDataPoint]],
                          topEndpoints: List[Map[String, Any]],
                          forecast: Option[ForecastResult],
                          anomalies: List[Anomaly],
                          comparison: Map[String, ComparisonResult],
                          limits: Map[String, Any],
                          lastUpdated: Instant,
                          responseTimeMs: Long
                        )

case class RealtimeDashboard(
                              tenantId: String,
                              activeSessions: Int,
                              concurrentRequests: Int,
                              requestsLastMinute: Int,
                              requestsLastHour: Int,
                              currentMemoryMb: Long,
                              peakMemoryMb: Long,
                              timestamp: Instant
                            )

object RealtimeDashboard {
  def empty(tenantId: String): RealtimeDashboard = RealtimeDashboard(
    tenantId = tenantId,
    activeSessions = 0,
    concurrentRequests = 0,
    requestsLastMinute = 0,
    requestsLastHour = 0,
    currentMemoryMb = 0,
    peakMemoryMb = 0,
    timestamp = Instant.now()
  )
}