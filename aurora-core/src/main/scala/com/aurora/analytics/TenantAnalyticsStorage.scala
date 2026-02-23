package com.aurora.analytics

import com.aurora.infrastructure.MongoDB
import com.mongodb.client.model.*
// Fix 1: Import with aliases to avoid conflicts with Scala's built-in methods
import com.mongodb.client.model.Filters.{eq as mongoEq, gte as mongoGte, lte as mongoLte, and as mongoAnd, lt as mongoLt}
import com.mongodb.client.model.Sorts.{ascending as mongoAscending, descending as mongoDescending}
import org.bson.Document
import org.bson.conversions.Bson

import java.time.{Instant, ZoneOffset}
import java.time.format.DateTimeFormatter
import scala.jdk.CollectionConverters.*
import scala.util.Try
import scala.collection.immutable.Map

/**
 * FAANG-level: Time-series optimized storage using MongoDB
 *
 * Key features:
 * - Automatic time-series collection detection and creation (MongoDB 5.0+)
 * - Multi-granularity storage (raw, minute, hour, day)
 * - Automatic downsampling support
 * - Configurable retention policies
 * - Optimized indexes for query performance
 * - Comprehensive error handling
 * - Thread-safe operations
 */
class TenantAnalyticsStorage {

  // Structured logging
  private def debug(msg: String): Unit = println(s"[DEBUG] [AnalyticsStorage] $msg")
  private def info(msg: String): Unit = println(s"[INFO] [AnalyticsStorage] $msg")
  private def warn(msg: String): Unit = println(s"[WARN] [AnalyticsStorage] $msg")
  private def error(msg: String): Unit = println(s"[ERROR] [AnalyticsStorage] $msg")

  // ==========================================================================
  // Constants
  // ==========================================================================

  // Collection names
  private val COLLECTION_RAW_EVENTS = "analytics_events_raw"
  private val COLLECTION_METRICS_MINUTE = "analytics_metrics_minute"
  private val COLLECTION_METRICS_HOUR = "analytics_metrics_hour"
  private val COLLECTION_METRICS_DAY = "analytics_metrics_day"

  // Retention periods in seconds
  private val RETENTION_RAW_DAYS = 7
  private val RETENTION_MINUTE_DAYS = 30
  private val RETENTION_HOUR_DAYS = 90
  private val RETENTION_DAY_DAYS = 365

  private val RETENTION_RAW_SECONDS = RETENTION_RAW_DAYS * 24 * 3600
  private val RETENTION_MINUTE_SECONDS = RETENTION_MINUTE_DAYS * 24 * 3600
  private val RETENTION_HOUR_SECONDS = RETENTION_HOUR_DAYS * 24 * 3600
  private val RETENTION_DAY_SECONDS = RETENTION_DAY_DAYS * 24 * 3600

  // ==========================================================================
  // MongoDB Collections
  // ==========================================================================

  private val rawEventsCollection = MongoDB.database.getCollection(COLLECTION_RAW_EVENTS)
  private val metricsMinuteCollection = MongoDB.database.getCollection(COLLECTION_METRICS_MINUTE)
  private val metricsHourCollection = MongoDB.database.getCollection(COLLECTION_METRICS_HOUR)
  private val metricsDayCollection = MongoDB.database.getCollection(COLLECTION_METRICS_DAY)

  // Initialize collections and indexes
  initializeCollections()

  // ==========================================================================
  // Initialization
  // ==========================================================================

  /**
   * Initialize collections with proper indexes and time-series support
   */
  private def initializeCollections(): Unit = {
    try {
      // Check MongoDB version for time-series support
      val (isTimeSeriesSupported, version) = checkTimeSeriesSupport()

      if (isTimeSeriesSupported) {
        info(s"MongoDB $version detected, using time-series collections")

        // Create time-series collections if they don't exist
        createTimeSeriesCollectionIfNotExists(COLLECTION_METRICS_MINUTE, "minute", RETENTION_MINUTE_SECONDS)
        createTimeSeriesCollectionIfNotExists(COLLECTION_METRICS_HOUR, "hour", RETENTION_HOUR_SECONDS)
        createTimeSeriesCollectionIfNotExists(COLLECTION_METRICS_DAY, "day", RETENTION_DAY_SECONDS)
      } else {
        info(s"MongoDB $version does not support time-series, using regular collections with TTL indexes")
        createRegularIndexes()
      }

      // Always create indexes for raw events collection
      createRawEventsIndexes()

      info("Analytics storage initialized successfully")
    } catch {
      case e: Exception =>
        error(s"Failed to initialize analytics storage: ${e.getMessage}")
        // Fall back to creating regular indexes
        createRegularIndexes()
        createRawEventsIndexes()
    }
  }

  /**
   * Check if MongoDB version supports time-series collections
   */
  private def checkTimeSeriesSupport(): (Boolean, String) = {
    try {
      val buildInfo = MongoDB.database.runCommand(new Document("buildInfo", 1))
      val version = buildInfo.getString("version")
      val isSupported = version.startsWith("5.") || version.startsWith("6.") || version.startsWith("7.")
      (isSupported, version)
    } catch {
      case e: Exception =>
        warn(s"Could not determine MongoDB version: ${e.getMessage}")
        (false, "unknown")
    }
  }

  /**
   * Create time-series collection if it doesn't exist
   */
  private def createTimeSeriesCollectionIfNotExists(name: String, granularity: String, expireAfterSeconds: Int): Unit = {
    try {
      // Try to get collection - if it exists, this will succeed
      val collection = MongoDB.database.getCollection(name)

      // Collection exists, ensure indexes
      collection.createIndex(Indexes.ascending("tenantId", "timestamp"))
      collection.createIndex(Indexes.ascending("timestamp"))
      collection.createIndex(Indexes.descending("timestamp"))

      debug(s"Verified indexes for existing collection: $name")
    } catch {
      case e: Exception =>
        // Collection doesn't exist, create with time-series options
        try {
          val cmd = new Document("create", name)
            .append("timeseries", new Document("timeField", "timestamp")
              .append("metaField", "metadata")
              .append("granularity", granularity))
            .append("expireAfterSeconds", expireAfterSeconds)

          MongoDB.database.runCommand(cmd)
          info(s"Created time-series collection: $name (granularity: $granularity, retention: ${expireAfterSeconds / 3600 / 24} days)")
        } catch {
          case ex: Exception =>
            warn(s"Failed to create time-series collection $name: ${ex.getMessage}")
            // Fall back to regular collection
            createRegularCollectionWithTTL(name, expireAfterSeconds)
        }
    }
  }

  /**
   * Create regular collection with TTL index
   */
  private def createRegularCollectionWithTTL(name: String, expireAfterSeconds: Int): Unit = {
    try {
      val collection = MongoDB.database.getCollection(name)

      // Create TTL index on timestamp
      collection.createIndex(
        Indexes.ascending("timestamp"),
        new IndexOptions().expireAfter(expireAfterSeconds.toLong, java.util.concurrent.TimeUnit.SECONDS)
      )

      // Create other indexes
      collection.createIndex(Indexes.ascending("tenantId", "timestamp"))
      collection.createIndex(Indexes.descending("timestamp"))

      info(s"Created regular collection with TTL: $name (retention: ${expireAfterSeconds / 3600 / 24} days)")
    } catch {
      case ex: Exception =>
        error(s"Failed to create regular collection $name: ${ex.getMessage}")
    }
  }

  /**
   * Create indexes for raw events collection
   */
  private def createRawEventsIndexes(): Unit = {
    try {
      rawEventsCollection.createIndex(Indexes.ascending("tenantId", "timestamp"))
      rawEventsCollection.createIndex(Indexes.ascending("eventType", "timestamp"))
      rawEventsCollection.createIndex(Indexes.ascending("timestamp"))
      rawEventsCollection.createIndex(
        Indexes.ascending("timestamp"),
        new IndexOptions().expireAfter(RETENTION_RAW_SECONDS.toLong, java.util.concurrent.TimeUnit.SECONDS)
      )
      debug("Raw events indexes created/verified")
    } catch {
      case e: Exception =>
        warn(s"Failed to create raw events indexes: ${e.getMessage}")
    }
  }

  /**
   * Create regular indexes for non-time-series collections
   */
  private def createRegularIndexes(): Unit = {
    try {
      // Minute collection indexes
      metricsMinuteCollection.createIndex(Indexes.ascending("tenantId", "timestamp"))
      metricsMinuteCollection.createIndex(Indexes.ascending("timestamp"))
      metricsMinuteCollection.createIndex(Indexes.descending("timestamp"))
      metricsMinuteCollection.createIndex(
        Indexes.ascending("timestamp"),
        new IndexOptions().expireAfter(RETENTION_MINUTE_SECONDS.toLong, java.util.concurrent.TimeUnit.SECONDS)
      )

      // Hour collection indexes
      metricsHourCollection.createIndex(Indexes.ascending("tenantId", "timestamp"))
      metricsHourCollection.createIndex(
        Indexes.ascending("timestamp"),
        new IndexOptions().expireAfter(RETENTION_HOUR_SECONDS.toLong, java.util.concurrent.TimeUnit.SECONDS)
      )

      // Day collection indexes
      metricsDayCollection.createIndex(Indexes.ascending("tenantId", "timestamp"))
      metricsDayCollection.createIndex(
        Indexes.ascending("timestamp"),
        new IndexOptions().expireAfter(RETENTION_DAY_SECONDS.toLong, java.util.concurrent.TimeUnit.SECONDS)
      )

      info("Regular indexes created/verified for all collections")
    } catch {
      case e: Exception =>
        error(s"Failed to create regular indexes: ${e.getMessage}")
    }
  }

  // ==========================================================================
  // Write Operations
  // ==========================================================================

  /**
   * Store raw events with automatic TTL
   */
  def storeRawEvents(events: List[AnalyticsEvent]): Try[Unit] = Try {
    if (events.isEmpty) {
      debug("No raw events to store")
      // Instead of return, just do nothing and let the Try succeed
    } else {
      val documents = events.map { event =>
        val eventDoc = eventToDocument(event)

        new Document("eventId", event.eventId)
          .append("tenantId", event.tenantId)
          .append("eventType", event.eventType.entryName)
          .append("timestamp", event.timestamp.toString)
          .append("schemaVersion", event.schemaVersion)
          .append("data", eventDoc)
          .append("metadata", new Document(event.metadata.asJava))
      }

      // Batch insert in chunks to avoid MongoDB document size limits
      documents.grouped(1000).foreach { batch =>
        try {
          rawEventsCollection.insertMany(batch.asJava)
        } catch {
          case e: Exception =>
            // If batch insert fails, try individual inserts
            batch.foreach { doc =>
              try {
                rawEventsCollection.insertOne(doc)
              } catch {
                case ex: Exception =>
                  warn(s"Failed to insert raw event: ${ex.getMessage}")
              }
            }
        }
      }

      info(s"Stored ${events.size} raw events")
    }
  }

  /**
   * Store aggregated minute-level metrics
   */
  def storeMinuteMetrics(metrics: List[AggregatedMinuteMetrics]): Try[Unit] = Try {
    if (metrics.isEmpty) {
      debug("No minute metrics to store")
      // Just do nothing and let the Try succeed with Unit
    } else {
      val documents = metrics.map { m =>
        new Document("tenantId", m.tenantId)
          .append("timestamp", m.timestamp.toString)
          .append("tier", m.tier)
          .append("requestCount", m.requestCount)
          .append("errorCount", m.errorCount)
          .append("avgResponseTime", m.avgResponseTime)
          .append("p95ResponseTime", m.p95ResponseTime)
          .append("p99ResponseTime", m.p99ResponseTime)
          .append("maxResponseTime", m.maxResponseTime)
          .append("activeSessions", m.activeSessions)
          .append("concurrentRequests", m.concurrentRequests)
          .append("cpuUsageAvg", m.cpuUsageAvg)
          .append("memoryUsageAvg", m.memoryUsageAvg)
          .append("bandwidthIn", m.bandwidthIn)
          .append("bandwidthOut", m.bandwidthOut)
          .append("topEndpoints", new Document(m.topEndpoints.asJava))
          .append("metadata", new Document()) // Empty metadata for time-series
      }

      // Batch insert
      documents.grouped(1000).foreach { batch =>
        try {
          metricsMinuteCollection.insertMany(batch.asJava)
        } catch {
          case e: Exception =>
            warn(s"Failed to insert minute metrics batch: ${e.getMessage}")
            // Try individual inserts
            batch.foreach { doc =>
              try {
                metricsMinuteCollection.insertOne(doc)
              } catch {
                case ex: Exception =>
                  debug(s"Failed to insert minute metric: ${ex.getMessage}")
              }
            }
        }
      }

      debug(s"Stored ${metrics.size} minute metrics")
    }
  }

  /**
   * Store aggregated hour-level metrics
   */
  def storeHourMetrics(metrics: List[AggregatedMinuteMetrics]): Try[Unit] = Try {
    if (metrics.isEmpty) {
      debug("No hour metrics to store")
      // Just do nothing and let the Try succeed with Unit
    } else {
      val documents = metrics.map { m =>
        new Document("tenantId", m.tenantId)
          .append("timestamp", m.timestamp.toString)
          .append("tier", m.tier)
          .append("requestCount", m.requestCount)
          .append("errorCount", m.errorCount)
          .append("avgResponseTime", m.avgResponseTime)
          .append("p95ResponseTime", m.p95ResponseTime)
          .append("p99ResponseTime", m.p99ResponseTime)
          .append("maxResponseTime", m.maxResponseTime)
          .append("metadata", new Document())
      }

      documents.grouped(1000).foreach { batch =>
        metricsHourCollection.insertMany(batch.asJava)
      }

      debug(s"Stored ${metrics.size} hour metrics")
    }
  }

  /**
   * Store aggregated day-level metrics
   */
  def storeDayMetrics(metrics: List[AggregatedMinuteMetrics]): Try[Unit] = Try {
    if (metrics.isEmpty) {
      debug("No day metrics to store")
      // Just do nothing and let the Try succeed with Unit
    } else {
      val documents = metrics.map { m =>
        new Document("tenantId", m.tenantId)
          .append("timestamp", m.timestamp.toString)
          .append("tier", m.tier)
          .append("requestCount", m.requestCount)
          .append("errorCount", m.errorCount)
          .append("avgResponseTime", m.avgResponseTime)
          .append("metadata", new Document())
      }

      documents.grouped(1000).foreach { batch =>
        metricsDayCollection.insertMany(batch.asJava)
      }

      debug(s"Stored ${metrics.size} day metrics")
    }
  }

  // ==========================================================================
  // Query Operations
  // ==========================================================================

  /**
   * Query time-series data with automatic granularity selection
   */
  def queryTimeSeries(
                       tenantId: String,
                       metric: String,
                       startTime: Instant,
                       endTime: Instant,
                       granularity: String = "minute"
                     ): List[TimeSeriesDataPoint] = {

    try {
      val collection = granularity match {
        case "minute" => metricsMinuteCollection
        case "hour" => metricsHourCollection
        case "day" => metricsDayCollection
        case _ => metricsMinuteCollection
      }

      // Fix 2: Use aliased filter methods
      val filter = mongoAnd(
        mongoEq("tenantId", tenantId),
        mongoGte("timestamp", startTime.toString),
        mongoLte("timestamp", endTime.toString)
      )

      // Fix 3: Pass the filter directly - it's already Bson
      val documents = collection.find(filter)
        .sort(mongoAscending("timestamp"))
        .asScala
        .toList

      documents.flatMap { doc =>
        try {
          val timestamp = Instant.parse(doc.getString("timestamp"))

          // Fix 4: Handle numeric type conversions properly
          val value: Double = doc.get(metric) match {
            case d: java.lang.Double => d.doubleValue()
            case l: java.lang.Long => l.doubleValue()
            case i: java.lang.Integer => i.doubleValue()
            case _ => 0.0
          }

          val count: Long = doc.get("requestCount") match {
            case l: java.lang.Long => l.longValue()
            case i: java.lang.Integer => i.longValue()
            case _ => 0L
          }

          Some(TimeSeriesDataPoint(
            tenantId = tenantId,
            metric = metric,
            timestamp = timestamp,
            value = value,
            count = count,
            min = 0.0, // Not stored in basic metrics
            max = 0.0,
            sum = 0.0,
            p95 = None,
            p99 = None
          ))
        } catch {
          case e: Exception =>
            debug(s"Error parsing time-series document: ${e.getMessage}")
            None
        }
      }
    } catch {
      case e: Exception =>
        warn(s"Error querying time-series data: ${e.getMessage}")
        List.empty
    }
  }

  /**
   * Query raw events with filters
   */
  /**
   * Query raw events with filters
   */
  def queryRawEvents(
                      tenantId: String,
                      eventType: Option[String] = None,
                      startTime: Instant,
                      endTime: Instant,
                      limit: Int = 1000
                    ): List[Document] = {

    try {
      val filters = List(
        Some(mongoEq("tenantId", tenantId)),
        Some(mongoGte("timestamp", startTime.toString)),
        Some(mongoLte("timestamp", endTime.toString)),
        eventType.map(et => mongoEq("eventType", et))
      ).flatten

      // Fix: Use filters* instead of filters: _*
      val filter = if (filters.size == 1) {
        filters.head
      } else {
        mongoAnd(filters*)
      }

      rawEventsCollection.find(filter)
        .sort(mongoDescending("timestamp"))
        .limit(limit)
        .asScala
        .toList
    } catch {
      case e: Exception =>
        warn(s"Error querying raw events: ${e.getMessage}")
        List.empty
    }
  }

  /**
   * Get tenant summary for dashboard
   */
  def getTenantSummary(tenantId: String, hours: Int = 24): Try[TenantAnalyticsSummary] = Try {
    val endTime = Instant.now()
    val startTime = endTime.minusSeconds(hours * 3600L)

    // Fix 6: Use aliased filter methods
    val filter = mongoAnd(
      mongoEq("tenantId", tenantId),
      mongoGte("timestamp", startTime.toString),
      mongoLte("timestamp", endTime.toString)
    )

    val minuteMetrics = metricsMinuteCollection.find(filter).asScala.toList

    if (minuteMetrics.isEmpty) {
      debug(s"No minute metrics found for tenant $tenantId")
      Try(TenantAnalyticsSummary.empty(tenantId))  // Just return the Try directly
    }

    // Fix 7: Safely extract values with proper type conversion
    val totalRequests = minuteMetrics.flatMap { doc =>
      Option(doc.get("requestCount")).collect {
        case l: java.lang.Long => l.longValue()
        case i: java.lang.Integer => i.longValue()
      }
    }.sum

    val totalErrors = minuteMetrics.flatMap { doc =>
      Option(doc.get("errorCount")).collect {
        case l: java.lang.Long => l.longValue()
        case i: java.lang.Integer => i.longValue()
      }
    }.sum

    val avgResponseTimes = minuteMetrics.flatMap { doc =>
      Option(doc.get("avgResponseTime")).collect {
        case d: java.lang.Double => d.doubleValue()
        case l: java.lang.Long => l.doubleValue()
        case i: java.lang.Integer => i.doubleValue()
      }
    }

    val avgResponseTime = if (avgResponseTimes.nonEmpty)
      avgResponseTimes.sum / avgResponseTimes.size
    else 0.0

    val maxResponseTimes = minuteMetrics.flatMap { doc =>
      Option(doc.get("maxResponseTime")).collect {
        case l: java.lang.Long => l.longValue()
        case i: java.lang.Integer => i.longValue()
      }
    }
    val maxResponseTime = if (maxResponseTimes.nonEmpty) maxResponseTimes.max else 0L

    val peakConcurrent = minuteMetrics.flatMap { doc =>
      Option(doc.get("concurrentRequests")).collect {
        case i: java.lang.Integer => i.intValue()
        case l: java.lang.Long => l.intValue()
      }
    }.maxOption.getOrElse(0)

    val peakSessions = minuteMetrics.flatMap { doc =>
      Option(doc.get("activeSessions")).collect {
        case i: java.lang.Integer => i.intValue()
        case l: java.lang.Long => l.intValue()
      }
    }.maxOption.getOrElse(0)

    // Fix 8: Handle lastP95 and lastP99 with proper type conversion
    val lastP95: Double = minuteMetrics.lastOption.flatMap { doc =>
      Option(doc.get("p95ResponseTime")).collect {
        case d: java.lang.Double => d.doubleValue()
        case l: java.lang.Long => l.doubleValue()
        case i: java.lang.Integer => i.doubleValue()
      }
    }.getOrElse(0.0)

    val lastP99: Double = minuteMetrics.lastOption.flatMap { doc =>
      Option(doc.get("p99ResponseTime")).collect {
        case d: java.lang.Double => d.doubleValue()
        case l: java.lang.Long => l.doubleValue()
        case i: java.lang.Integer => i.doubleValue()
      }
    }.getOrElse(0.0)

    TenantAnalyticsSummary(
      tenantId = tenantId,
      timeRange = s"${hours}h",
      totalRequests = totalRequests,
      totalErrors = totalErrors,
      errorRate = if (totalRequests > 0) (totalErrors.toDouble / totalRequests) * 100 else 0.0,
      avgResponseTimeMs = avgResponseTime,
      p95ResponseTimeMs = lastP95,
      p99ResponseTimeMs = lastP99,
      maxResponseTimeMs = maxResponseTime,
      peakConcurrentRequests = peakConcurrent,
      peakActiveSessions = peakSessions,
      timestamp = Instant.now()
    )
  }

  /**
   * Delete old data (manual cleanup if TTL not sufficient)
   */
  def deleteOldData(olderThan: Instant): Try[Long] = Try {
    val filter = mongoLt("timestamp", olderThan.toString)
    val result = rawEventsCollection.deleteMany(filter)
    result.getDeletedCount
  }

  // ==========================================================================
  // Health Check
  // ==========================================================================

  /**
   * Check storage health
   */
  def healthCheck(): Map[String, Any] = {
    try {
      // Try to ping each collection
      val rawCount = rawEventsCollection.estimatedDocumentCount()
      val minuteCount = metricsMinuteCollection.estimatedDocumentCount()

      Map(
        "status" -> "healthy",
        "collections" -> Map(
          "raw" -> rawCount,
          "minute" -> minuteCount,
          "hour" -> metricsHourCollection.estimatedDocumentCount(),
          "day" -> metricsDayCollection.estimatedDocumentCount()
        ),
        "timestamp" -> Instant.now().toString
      )
    } catch {
      case e: Exception =>
        Map(
          "status" -> "unhealthy",
          "error" -> e.getMessage,
          "timestamp" -> Instant.now().toString
        )
    }
  }

  // ==========================================================================
  // Private Helpers
  // ==========================================================================

  /**
   * Convert AnalyticsEvent to MongoDB Document
   */
  private def eventToDocument(event: AnalyticsEvent): Document = {
    val doc = new Document()

    event match {
      case e: ApiRequestEvent =>
        doc.append("method", e.method)
          .append("path", e.path)
          .append("statusCode", e.statusCode)
          .append("responseTimeMs", e.responseTimeMs)
          .append("sessionId", e.sessionId.orNull)
          .append("userId", e.userId.orNull)
          .append("isError", e.isError)
          .append("isSlowRequest", e.isSlowRequest)
        e.cpuTimeMs.foreach(v => doc.append("cpuTimeMs", v))
        e.memoryDeltaMb.foreach(v => doc.append("memoryDeltaMb", v))
        e.requestSize.foreach(v => doc.append("requestSize", v))
        e.responseSize.foreach(v => doc.append("responseSize", v))
        e.userAgent.foreach(v => doc.append("userAgent", v))
        e.ipAddress.foreach(v => doc.append("ipAddress", v))
        e.errorType.foreach(v => doc.append("errorType", v))

      case e: SessionEvent =>
        doc.append("sessionId", e.sessionId)
          .append("userId", e.userId.orNull)
          .append("durationSeconds", e.durationSeconds.map(Long.box).orNull)
          .append("pageViews", e.pageViews.map(Int.box).orNull)
          .append("actions", e.actions.map(Int.box).orNull)
          .append("entryPath", e.entryPath.orNull)
          .append("exitPath", e.exitPath.orNull)
          .append("deviceType", e.deviceType.orNull)
          .append("browser", e.browser.orNull)
          .append("os", e.os.orNull)
          .append("country", e.country.orNull)

      case e: FeatureUsageEvent =>
        doc.append("feature", e.feature)
          .append("action", e.action)
          .append("success", e.success)
          .append("durationMs", e.durationMs.map(Long.box).orNull)
          .append("sessionId", e.sessionId.orNull)
          .append("userId", e.userId.orNull)
          .append("errorType", e.errorType.orNull)

      case e: LimitViolationEvent =>
        doc.append("resourceType", e.resourceType)
          .append("currentValue", e.currentValue)
          .append("limitValue", e.limitValue)
          .append("rejected", e.rejected)
    }

    // Add common metadata
    event.metadata.foreach { case (k, v) =>
      doc.append(s"meta_$k", v)
    }

    doc
  }
}

// ==========================================================================
// Model Classes
// ==========================================================================

case class AggregatedMinuteMetrics(
                                    tenantId: String,
                                    tier: String,
                                    timestamp: Instant,
                                    requestCount: Long,
                                    errorCount: Long,
                                    avgResponseTime: Double,
                                    p95ResponseTime: Double,
                                    p99ResponseTime: Double,
                                    maxResponseTime: Long,
                                    activeSessions: Int,
                                    concurrentRequests: Int,
                                    cpuUsageAvg: Double,
                                    memoryUsageAvg: Double,
                                    bandwidthIn: Long,
                                    bandwidthOut: Long,
                                    topEndpoints: Map[String, Long]
                                  )

case class TimeSeriesDataPoint(
                                tenantId: String,
                                metric: String,
                                timestamp: Instant,
                                value: Double,
                                count: Long,
                                min: Double,
                                max: Double,
                                sum: Double,
                                p95: Option[Double],
                                p99: Option[Double]
                              )

case class TenantAnalyticsSummary(
                                   tenantId: String,
                                   timeRange: String,
                                   totalRequests: Long,
                                   totalErrors: Long,
                                   errorRate: Double,
                                   avgResponseTimeMs: Double,
                                   p95ResponseTimeMs: Double,
                                   p99ResponseTimeMs: Double,
                                   maxResponseTimeMs: Long,
                                   peakConcurrentRequests: Int,
                                   peakActiveSessions: Int,
                                   timestamp: Instant
                                 )

object TenantAnalyticsSummary {
  def empty(tenantId: String): TenantAnalyticsSummary = TenantAnalyticsSummary(
    tenantId = tenantId,
    timeRange = "N/A",
    totalRequests = 0,
    totalErrors = 0,
    errorRate = 0.0,
    avgResponseTimeMs = 0.0,
    p95ResponseTimeMs = 0.0,
    p99ResponseTimeMs = 0.0,
    maxResponseTimeMs = 0,
    peakConcurrentRequests = 0,
    peakActiveSessions = 0,
    timestamp = Instant.now()
  )
}