package com.aurora.tenant

import com.mongodb.client.model.*
import com.mongodb.client.model.Filters.*
import com.mongodb.client.model.Indexes.*
import org.bson.Document
import com.aurora.infrastructure.MongoDB

import java.lang.management.ManagementFactory
import java.time.{Duration, Instant}
import java.util.concurrent.{ConcurrentHashMap, atomic}
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*
import scala.util.Try

// Import for onboarding service
import com.aurora.tenant.TenantOnboardingService

// Import DefaultLimits from TenantResourceLimits (remove the duplicate definition below)
import com.aurora.tenant.DefaultLimits

/**
 * Production-ready service for enforcing tenant resource limits
 */
class TenantResourceService(
                             usageManager: TenantResourceUsageManager.type = TenantResourceUsageManager
                           ) {

  // Simple logging helpers
  private def logInfo(msg: String): Unit = println(s"[INFO] [TenantResourceService] $msg")
  private def logWarn(msg: String): Unit = println(s"[WARN] [TenantResourceService] $msg")
  private def logError(msg: String): Unit = println(s"[ERROR] [TenantResourceService] $msg")
  private def logDebug(msg: String): Unit = println(s"[DEBUG] [TenantResourceService] $msg")

  // MongoDB collection for resource limits (sync driver)
  private val limitsCollection =
    MongoDB.database.getCollection("tenant_resource_limits")

  // Ensure indexes on startup
  initializeIndexes()

  // ==========================================================================
  // In-Memory Caching with TTL
  // ==========================================================================

  private case class CachedLimits(
                                   limits: TenantResourceLimits,
                                   timestamp: Instant
                                 )

  private val limitsCache = new ConcurrentHashMap[String, CachedLimits]().asScala
  private val cacheTTL = Duration.ofMinutes(5)

  private def isCacheValid(cached: CachedLimits): Boolean = {
    Duration.between(cached.timestamp, Instant.now()).toMinutes < cacheTTL.toMinutes
  }

  // ==========================================================================
  // Rate Limiters with Atomic Operations
  // ==========================================================================

  private class RateLimiter(maxRequests: Int, windowSeconds: Int) {
    private val requests = new ConcurrentHashMap[Long, Boolean]().asScala
    private val lock = new Object()

    def tryAcquire(): Boolean = lock.synchronized {
      val now = System.currentTimeMillis()
      val windowStart = now - (windowSeconds * 1000L)

      // Remove expired entries
      requests.keys.foreach { timestamp =>
        if (timestamp < windowStart) requests.remove(timestamp)
      }

      if (requests.size < maxRequests) {
        requests.put(now, true)
        true
      } else {
        false
      }
    }

    def getCurrentCount(): Int = {
      val now = System.currentTimeMillis()
      val windowStart = now - (windowSeconds * 1000L)
      requests.keys.count(_ > windowStart)
    }

    def cleanup(): Unit = {
      val now = System.currentTimeMillis()
      val windowStart = now - (windowSeconds * 1000L)
      requests.keys.foreach { timestamp =>
        if (timestamp < windowStart) requests.remove(timestamp)
      }
    }
  }

  private class ConcurrentLimiter(max: Int) {
    private val current = new atomic.AtomicInteger(0)

    def tryAcquire(): Boolean = {
      while true do
        val value = current.get()
        if value >= max then return false
        if current.compareAndSet(value, value + 1) then return true
      false
    }

    def release(): Unit = current.decrementAndGet()
    def getCurrent(): Int = current.get()
  }

  private val rateLimiters = new ConcurrentHashMap[String, RateLimiter]().asScala
  private val concurrentLimiters = new ConcurrentHashMap[String, ConcurrentLimiter]().asScala

  // ==========================================================================
  // Metrics Collection
  // ==========================================================================

  private case class TenantMetrics(
                                    var totalRequests: Long = 0L,
                                    var rejectedRequests: Long = 0L,
                                    var peakConcurrent: Int = 0,
                                    var lastViolation: Option[String] = None,
                                    var violationCount: Int = 0,
                                    var totalResponseTimeMs: Long = 0L,
                                    var requestCount: Long = 0L
                                  ) {
    def avgResponseTimeMs: Double =
      if requestCount > 0 then totalResponseTimeMs.toDouble / requestCount else 0.0
  }

  private val metricsMap = new ConcurrentHashMap[String, TenantMetrics]().asScala

  // JVM memory bean for monitoring
  private val memoryBean = ManagementFactory.getMemoryMXBean

  // ==========================================================================
  // Public API Methods
  // ==========================================================================

  /**
   * Check if a tenant can process a new request with atomic operations
   * @return Right(Unit) if allowed, Left(error message) if limit exceeded
   */
  def checkRequestAllowed(tenantId: String): Either[String, Unit] = {
    val startTime = System.currentTimeMillis()
    val metrics = getOrCreateMetrics(tenantId)

    try {
      getLimits(tenantId) match {
        case None =>
          // No limits configured, just track
          usageManager.getOrCreate(tenantId).trackRequestStart()
          metrics.totalRequests += 1
          Right(())

        case Some(limits) =>
          val usage = usageManager.getOrCreate(tenantId)

          // 1. Check concurrent requests (fastest, most critical)
          val concurrentLimit = limits.getLimit(ResourceType.ConcurrentRequests)
          val concurrentLimiter = concurrentLimiters.getOrElseUpdate(tenantId,
            new ConcurrentLimiter(concurrentLimit.map(_.value.toInt).getOrElse(Int.MaxValue)))

          if !concurrentLimiter.tryAcquire() then
            metrics.rejectedRequests += 1
            metrics.violationCount += 1
            metrics.lastViolation = Some("Concurrent limit exceeded")

            val current = concurrentLimiter.getCurrent()
            val limit = concurrentLimit.map(_.value.toInt).getOrElse(0)
            Left(s"Concurrent requests limit exceeded: $current > $limit")
          else
            // Update peak concurrent
            val currentConcurrent = concurrentLimiter.getCurrent()
            if currentConcurrent > metrics.peakConcurrent then
              metrics.peakConcurrent = currentConcurrent

            // 2. Check rate limit
            val rateLimit = limits.getLimit(ResourceType.APIRequests)

            // Process rate limit check
            val rateCheckResult = rateLimit match {
              case Some(limit) =>
                val rateLimiter = rateLimiters.getOrElseUpdate(tenantId,
                  new RateLimiter(limit.value.toInt, limit.windowSeconds.getOrElse(60)))

                if !rateLimiter.tryAcquire() then
                  concurrentLimiter.release() // Rollback concurrent count
                  metrics.rejectedRequests += 1
                  metrics.violationCount += 1
                  metrics.lastViolation = Some("Rate limit exceeded")

                  val current = rateLimiter.getCurrentCount()
                  Left(s"Rate limit exceeded: $current > ${limit.value} requests per ${limit.windowSeconds.getOrElse(60)} seconds")
                else
                  Right(())

              case None =>
                Right(())
            }

            rateCheckResult match {
              case Left(error) =>
                Left(error)

              case Right(_) =>
                // 3. Check memory (soft limit - warn only)
                limits.getLimit(ResourceType.Memory).foreach { limit =>
                  if usage.getCurrentMemoryMb > limit.value then
                    logWarn(s"Tenant $tenantId exceeding memory limit: ${usage.getCurrentMemoryMb}MB > ${limit.value}MB")
                    if limit.limitType == LimitType.Hard then
                      // Hard memory limit - we should reject but can't easily enforce in JVM
                      logError(s"Hard memory limit exceeded for tenant $tenantId but cannot enforce in JVM")
                }

                // 4. Track the request
                usage.trackRequestStart()
                metrics.totalRequests += 1
                metrics.requestCount += 1
                metrics.totalResponseTimeMs += (System.currentTimeMillis() - startTime)

                Right(())
            }
      }
    } catch {
      case e: Exception =>
        logError(s"Error checking request limits for tenant $tenantId: ${e.getMessage}")
        // Fail open - allow request but log error
        Right(())
    }
  }

  /**
   * Track request completion (release concurrent limit)
   */
  def trackRequestComplete(tenantId: String, responseTimeMs: Long = 0): Unit = {
    try {
      usageManager.get(tenantId).foreach(_.trackRequestEnd())
      concurrentLimiters.get(tenantId).foreach(_.release())

      metricsMap.get(tenantId).foreach { metrics =>
        metrics.totalResponseTimeMs += responseTimeMs
        metrics.requestCount += 1
      }
    } catch {
      case e: Exception =>
        logError(s"Error tracking request completion for tenant $tenantId: ${e.getMessage}")
    }
  }

  /**
   * Set resource limits with MongoDB persistence
   */
  def setLimits(
                 tenantId: String,
                 limits: TenantResourceLimits
               )(implicit ec: ExecutionContext): Future[Boolean] = {
    // Validate limits first
    validateLimits(limits) match {
      case Left(error) =>
        logWarn(s"Invalid limits for tenant $tenantId: $error")
        return Future.successful(false)
      case Right(_) => // Continue
    }

    // Build MongoDB document correctly (nested Document for limits)
    val limitsInner = new Document()
    limits.limits.foreach { case (resource, limit) =>
      limitsInner.append(resource.toString, new Document()
        .append("value", limit.value)
        .append("type", limit.limitType.toString)
        .append("windowSeconds", limit.windowSeconds.orNull)
        .append("description", limit.description.orNull))
    }

    val limitsDoc = new Document("tenantId", tenantId)
      .append("limits", limitsInner)
      .append("version", limits.version)
      .append("updatedAt", limits.updatedAt.toString)
      .append("updatedBy", limits.updatedBy.orNull)

    // Execute synchronously and wrap in Future
    Future {
      Try {
        val replaceOptions = new ReplaceOptions().upsert(true)
        limitsCollection.replaceOne(
          Filters.eq("tenantId", tenantId),
          limitsDoc,
          replaceOptions
        )

        // Invalidate cache
        limitsCache.remove(tenantId)

        logInfo(s"Successfully updated limits for tenant $tenantId")
        true
      }.getOrElse {
        logError(s"Failed to set limits for tenant $tenantId")
        false
      }
    }
  }

  /**
   * Get resource limits with caching
   */
  def getLimits(tenantId: String): Option[TenantResourceLimits] = {
    // Try cache first
    limitsCache.get(tenantId) match {
      case Some(cached) if isCacheValid(cached) =>
        Some(cached.limits)
      case _ =>
        // Fetch from MongoDB
        fetchLimitsFromDb(tenantId) match {
          case Some(limits) =>
            // Update cache
            limitsCache.put(tenantId, CachedLimits(limits, Instant.now()))
            Some(limits)
          case None =>
            // Fallback to tier-based defaults
            val defaultLimits = getTierBasedLimits(tenantId)
            defaultLimits.foreach { limits =>
              limitsCache.put(tenantId, CachedLimits(limits, Instant.now()))
            }
            defaultLimits
        }
    }
  }

  /**
   * Get current usage statistics
   */
  def getUsage(tenantId: String): Option[Map[String, Any]] = {
    usageManager.get(tenantId).map { usage =>
      usage.getStats ++ Map(
        "metrics" -> getTenantMetrics(tenantId),
        "limits" -> getLimits(tenantId).map(_.limits.map { case (k, v) =>
          k.toString -> Map(
            "value" -> v.value,
            "type" -> v.limitType.toString,
            "windowSeconds" -> v.windowSeconds,
            "description" -> v.description
          )
        })
      )
    }
  }

  /**
   * Get limit status with percentages (returning Any for string fields)
   */
  def getLimitStatus(tenantId: String): Map[String, Map[String, Any]] = {
    getLimits(tenantId).map { limits =>
      val usage = usageManager.getOrCreate(tenantId)

      limits.limits.flatMap { (resource, limit) =>
        val current = resource match {
          case ResourceType.CPU => usage.getConcurrentRequests.toDouble
          case ResourceType.Memory => usage.getCurrentMemoryMb.toDouble
          case ResourceType.APIRequests =>
            rateLimiters.get(tenantId).map(_.getCurrentCount().toDouble).getOrElse(0.0)
          case ResourceType.ConcurrentRequests =>
            concurrentLimiters.get(tenantId).map(_.getCurrent().toDouble).getOrElse(0.0)
        }

        val percentage = if limit.value > 0 then (current / limit.value) * 100 else 0.0
        val status =
          if percentage >= 100 then "CRITICAL"
          else if percentage >= 80 then "WARNING"
          else if percentage >= 50 then "MONITOR"
          else "NORMAL"

        Some(resource.toString -> Map(
          "current" -> current,
          "limit" -> limit.value,
          "percentage" -> percentage,
          "status" -> status,
          "unit" -> resource.unit
        ))
      }
    }.getOrElse(Map.empty)
  }

  /**
   * Get comprehensive metrics for a tenant
   */
  def getTenantMetrics(tenantId: String): Map[String, Any] = {
    val metrics = getOrCreateMetrics(tenantId)
    val usage = usageManager.getOrCreate(tenantId)

    Map(
      "total_requests" -> metrics.totalRequests,
      "rejected_requests" -> metrics.rejectedRequests,
      "rejection_rate" -> (if metrics.totalRequests > 0 then
        (metrics.rejectedRequests.toDouble / metrics.totalRequests) * 100
      else 0.0),
      "peak_concurrent" -> metrics.peakConcurrent,
      "current_concurrent" -> usage.getConcurrentRequests,
      "avg_response_time_ms" -> metrics.avgResponseTimeMs,
      "violation_count" -> metrics.violationCount,
      "last_violation" -> metrics.lastViolation.orNull,
      "current_memory_mb" -> usage.getCurrentMemoryMb,
      "peak_memory_mb" -> usage.getPeakMemoryMb,
      "requests_last_minute" -> usage.getRequestRate(60),
      "requests_last_hour" -> usage.getRequestRate(3600)
    )
  }

  /**
   * Get metrics for all tenants
   */
  def getAllTenantMetrics: Map[String, Map[String, Any]] = {
    usageManager.getAllUsage.keys.map { tenantId =>
      tenantId -> getTenantMetrics(tenantId)
    }.toMap
  }

  /**
   * Health check endpoint
   */
  def healthCheck(): Map[String, Any] = {
    Map(
      "status" -> "healthy",
      "total_tenants_tracked" -> usageManager.getAllUsage.size,
      "cache_size" -> limitsCache.size,
      "active_rate_limiters" -> rateLimiters.size,
      "active_concurrent_limiters" -> concurrentLimiters.size,
      "timestamp" -> Instant.now().toString,
      "jvm_heap_mb" -> (memoryBean.getHeapMemoryUsage.getUsed / (1024 * 1024)),
      "jvm_heap_max_mb" -> (memoryBean.getHeapMemoryUsage.getMax / (1024 * 1024))
    )
  }

  /**
   * Reset usage counters for a tenant
   */
  def resetUsage(tenantId: String): Unit = {
    usageManager.cleanup(tenantId)
    rateLimiters.remove(tenantId).foreach(_.cleanup())
    concurrentLimiters.remove(tenantId)
    metricsMap.remove(tenantId)
    limitsCache.remove(tenantId)

    logInfo(s"Reset all resource usage for tenant $tenantId")
  }

  // ==========================================================================
  // Private Helper Methods
  // ==========================================================================

  private def initializeIndexes(): Unit = {
    try {
      // Create indexes if they don't exist
      limitsCollection.createIndex(Indexes.ascending("tenantId"))
      limitsCollection.createIndex(Indexes.ascending("updatedAt"))
      logInfo("Resource limits indexes created/verified")
    } catch {
      case e: Exception =>
        logError(s"Failed to create indexes: ${e.getMessage}")
    }
  }

  private def fetchLimitsFromDb(tenantId: String): Option[TenantResourceLimits] = {
    try {
      val doc = limitsCollection.find(Filters.eq("tenantId", tenantId)).first()

      if doc == null then None
      else {
        val limitsDoc = doc.get("limits", classOf[Document])
        val limitsMap = if limitsDoc != null then
          limitsDoc.keySet().asScala.flatMap { key =>
            ResourceType.values.find(_.toString == key).map { resourceType =>
              val limitDoc = limitsDoc.get(key, classOf[Document])
              resourceType -> ResourceLimit(
                value = limitDoc.getDouble("value"),
                limitType = LimitType.valueOf(limitDoc.getString("type")),
                windowSeconds = Option(limitDoc.getString("windowSeconds")).map(_.toInt),
                description = Option(limitDoc.getString("description"))
              )
            }
          }.toMap
        else Map.empty

        Some(TenantResourceLimits(
          tenantId = tenantId,
          limits = limitsMap,
          updatedAt = Instant.parse(doc.getString("updatedAt")),
          updatedBy = Option(doc.getString("updatedBy")),
          version = doc.getLong("version")
        ))
      }
    } catch {
      case e: Exception =>
        logDebug(s"No limits found in DB for tenant $tenantId: ${e.getMessage}")
        None
    }
  }

  private def getTierBasedLimits(tenantId: String): Option[TenantResourceLimits] = {
    tenantId match {
      case id if id.startsWith("enterprise") =>
        Some(DefaultLimits.enterpriseTier.copy(tenantId = tenantId))
      case id if id.startsWith("pro") =>
        Some(DefaultLimits.proTier.copy(tenantId = tenantId))
      case id if id.startsWith("free") =>
        Some(DefaultLimits.freeTier.copy(tenantId = tenantId))
      case _ =>
        // Default to free tier for unknown tenants
        Some(DefaultLimits.freeTier.copy(tenantId = tenantId))
    }
  }

  private def getOrCreateMetrics(tenantId: String): TenantMetrics = {
    metricsMap.getOrElseUpdate(tenantId, TenantMetrics())
  }

  private def validateLimits(limits: TenantResourceLimits): Either[String, Unit] = {
    // Use a recursive function or fold to check each limit
    def checkLimit(remaining: List[(ResourceType, ResourceLimit)]): Either[String, Unit] = {
      remaining match {
        case Nil => Right(())

        case (resource, limit) :: tail =>
          resource match {
            case ResourceType.CPU =>
              if limit.value <= 0 || limit.value > 64 then
                Left(s"Invalid CPU limit: ${limit.value}. Must be between 0.1 and 64 cores")
              else
                checkLimit(tail)

            case ResourceType.Memory =>
              if limit.value <= 0 || limit.value > 131072 then
                Left(s"Invalid memory limit: ${limit.value}MB. Must be between 1 and 131072 MB")
              else
                checkLimit(tail)

            case ResourceType.APIRequests =>
              if limit.value <= 0 || limit.value > 100000 then
                Left(s"Invalid rate limit: ${limit.value}. Must be between 1 and 100000 requests")
              else if limit.windowSeconds.isEmpty || limit.windowSeconds.get < 1 || limit.windowSeconds.get > 3600 then
                Left(s"Invalid window: ${limit.windowSeconds}. Must be between 1 and 3600 seconds")
              else
                checkLimit(tail)

            case ResourceType.ConcurrentRequests =>
              if limit.value <= 0 || limit.value > 1000 then
                Left(s"Invalid concurrent limit: ${limit.value}. Must be between 1 and 1000")
              else
                checkLimit(tail)
          }
      }
    }

    // Convert the map to a list and check
    checkLimit(limits.limits.toList)
  }

  // ==========================================================================
  // Cleanup Scheduler
  // ==========================================================================

  import scala.concurrent.ExecutionContext

  /**
   * Start the cleanup scheduler (call during service initialization)
   */
  def startCleanupScheduler(implicit ec: ExecutionContext): java.util.concurrent.ScheduledFuture[_] = {
    val scheduler = java.util.concurrent.Executors.newSingleThreadScheduledExecutor()

    scheduler.scheduleAtFixedRate(
      () => cleanup(),
      1, 1, java.util.concurrent.TimeUnit.HOURS
    )
  }

  private def cleanup(): Unit = {
    try {
      logDebug("Starting resource limits cleanup")

      // Clean expired cache entries
      limitsCache.filterInPlace { case (_, cached) =>
        isCacheValid(cached)
      }

      // Clean old rate limiter data
      rateLimiters.values.foreach(_.cleanup())

      // Log cleanup stats
      logInfo(s"Cleanup completed. Active tenants: ${usageManager.getAllUsage.size}")
    } catch {
      case e: Exception =>
        logError(s"Error during cleanup: ${e.getMessage}")
    }
  }

  /**
   * Update memory usage tracking for a tenant
   * This can be called periodically or on each request
   */
  def updateMemoryUsage(tenantId: String): Unit = {
    try {
      val heapUsage = memoryBean.getHeapMemoryUsage.getUsed / (1024 * 1024) // Convert to MB
      usageManager.getOrCreate(tenantId).updateMemoryUsage(heapUsage)

      // Check if memory limit is exceeded (warning only)
      getLimits(tenantId).foreach { limits =>
        limits.getLimit(ResourceType.Memory).foreach { limit =>
          if (heapUsage > limit.value) {
            logWarn(s"Tenant $tenantId memory usage ${heapUsage}MB exceeds limit ${limit.value}MB")
          }
        }
      }
    } catch {
      case e: Exception =>
        logError(s"Failed to update memory usage for tenant $tenantId: ${e.getMessage}")
    }
  }

  /**
   * Initialize resource limits with a template
   */
  def initializeWithTemplate(
                              tenantId: String,
                              template: TenantOnboardingService.TenantTemplate,
                              requirements: TenantService.TenantRequirements
                            )(implicit ec: ExecutionContext): Future[TenantResourceLimits] = {

    logInfo(s"Initializing resource limits for tenant $tenantId with template ${template.tier}")

    // Start with template limits
    var limits = template.limitsTemplate.copy(tenantId = tenantId)

    // Override with requirements
    if (requirements.enterprise) {
      // Enterprise tenants get higher limits
      limits = limits.copy(
        limits = limits.limits ++ Map(
          ResourceType.APIRequests -> ResourceLimit(
            value = 10000.0,
            limitType = LimitType.Hard,
            windowSeconds = Some(60),
            description = Some("Enterprise rate limit")
          ),
          ResourceType.ConcurrentRequests -> ResourceLimit(
            value = 100.0,
            limitType = LimitType.Hard,
            windowSeconds = None,
            description = Some("Enterprise concurrent limit")
          ),
          ResourceType.Memory -> ResourceLimit(
            value = 8192.0,
            limitType = LimitType.Soft,
            windowSeconds = None,
            description = Some("Enterprise memory limit")
          )
        )
      )
    }

    if (requirements.expectedUsers > 1000) {
      // Scale limits based on expected users
      limits.limits.get(ResourceType.APIRequests).foreach { limit =>
        val scaledValue = limit.value * (requirements.expectedUsers / 100.0)
        limits = limits.copy(
          limits = limits.limits + (ResourceType.APIRequests -> limit.copy(
            value = scaledValue.min(100000) // Cap at 100k
          ))
        )
      }
    }

    // Save to MongoDB
    setLimits(tenantId, limits).map { _ =>
      logInfo(s"Resource limits initialized for tenant $tenantId")
      limits
    }
  }
}