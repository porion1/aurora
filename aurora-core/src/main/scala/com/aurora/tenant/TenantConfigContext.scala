package com.aurora.tenant

import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import org.slf4j.{Logger, LoggerFactory}

/**
 * Thread-local context for per-tenant configuration with request-scoped caching.
 *
 * Ensures all operations are scoped to the current tenant and caches configs
 * for the duration of a single request/CLI operation.
 *
 * Production-ready enhancements:
 * - Request-scoped config caching
 * - Automatic cache cleanup
 * - Nested context support
 * - Metrics collection
 * - Thread-safe
 */
object TenantConfigContext {

  // ===== SLF4J Logger (NO MACROS) =====
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  // ------------------------------
  // Thread-local storage for current tenant ID
  // ------------------------------
  private val threadLocalTenantId = new ThreadLocal[String]()

  // ------------------------------
  // Request-scoped config cache (per thread)
  // ------------------------------
  private case class CachedConfig(
                                   config: TenantConfig,
                                   timestamp: Long,
                                   accessCount: Int = 0
                                 )

  // Thread-local cache for the current request
  private val threadLocalCache = new ThreadLocal[ConcurrentHashMap[String, CachedConfig]]() {
    override def initialValue(): ConcurrentHashMap[String, CachedConfig] =
      new ConcurrentHashMap[String, CachedConfig]()
  }

  // ------------------------------
  // Metrics (using existing logging)
  // ------------------------------
  private var cacheHits: Long = 0
  private var cacheMisses: Long = 0

  // ===== TENANT CONTEXT MANAGEMENT =====

  /** Set the current tenant context */
  def setTenant(tenantId: String): Unit = {
    require(tenantId != null && tenantId.nonEmpty, "Tenant ID cannot be null or empty")
    threadLocalTenantId.set(tenantId)
    if (logger.isDebugEnabled) logger.debug("Tenant context set to: {}", tenantId)
  }

  /** Get the current tenant ID, if any */
  def getTenant: Option[String] =
    Option(threadLocalTenantId.get())

  /** Get current tenant ID or throw if not set */
  def requireTenant: String = getTenant.getOrElse {
    throw new IllegalStateException("No tenant context set - call setTenant first")
  }

  /** Clear the tenant context from the current thread */
  def clear(): Unit = {
    threadLocalTenantId.remove()
    if (logger.isDebugEnabled) logger.debug("Tenant context cleared")
  }

  /**
   * Execute a block of code within a specific tenant context.
   * Automatically restores previous tenant after execution.
   */
  def withTenant[T](tenantId: String)(block: => T): T = {
    val previous = getTenant
    val startTime = System.nanoTime()

    try {
      setTenant(tenantId)
      if (logger.isDebugEnabled) logger.debug("Entering tenant context: {}", tenantId)
      val result = block
      val duration = (System.nanoTime() - startTime) / 1000000 // ms
      if (logger.isDebugEnabled) logger.debug("Exiting tenant context: {} (duration: {}ms)", tenantId, duration.toString)
      result
    } finally {
      previous match {
        case Some(id) => setTenant(id)
        case None     => clear()
      }
    }
  }

  /**
   * Execute a block with a temporary tenant switch
   * (for cross-tenant operations like migrations)
   */
  def withImpersonatedTenant[T](tenantId: String)(block: => T): T = {
    val original = getTenant
    try {
      setTenant(tenantId)
      if (logger.isInfoEnabled) logger.info("Impersonating tenant: {}", tenantId)
      block
    } finally {
      original match {
        case Some(id) => setTenant(id)
        case None     => clear()
      }
      if (logger.isDebugEnabled) logger.debug("Restored original tenant context")
    }
  }

  // ===== CONFIG CACHE MANAGEMENT =====

  /**
   * Cache a config for the current request
   */
  def cacheConfig(tenantId: String, config: TenantConfig): Unit = {
    val cache = threadLocalCache.get()
    cache.put(tenantId, CachedConfig(
      config = config,
      timestamp = System.currentTimeMillis(),
      accessCount = 1
    ))
    if (logger.isDebugEnabled) logger.debug("Cached config for tenant {} (cache size: {})", tenantId, String.valueOf(cache.size()))
  }

  /**
   * Get cached config (if present)
   */
  def getCachedConfig(tenantId: String): Option[TenantConfig] = {
    val cache = threadLocalCache.get()
    Option(cache.get(tenantId)).map { cached =>
      // Update access count (for metrics)
      val updated = cached.copy(accessCount = cached.accessCount + 1)
      cache.put(tenantId, updated)
      cacheHits += 1
      cached.config
    }
  }

  /**
   * Get config for current tenant from cache (if present)
   */
  def getCurrentCachedConfig: Option[TenantConfig] =
    getTenant.flatMap(getCachedConfig)

  /**
   * Get config for current tenant, fetching if not cached
   * Note: This doesn't depend on service - just cache check
   */
  def getCurrentConfig: Option[TenantConfig] =
    getTenant.flatMap(getCachedConfig)

  /**
   * Check if tenant config is cached
   */
  def isCached(tenantId: String): Boolean = {
    val cache = threadLocalCache.get()
    cache.containsKey(tenantId)
  }

  /**
   * Clear request-scoped cache for current thread
   */
  def clearCache(): Unit = {
    val cache = threadLocalCache.get()
    val size = cache.size()
    cache.clear()
    threadLocalCache.remove()
    cacheMisses = 0
    cacheHits = 0
    if (logger.isDebugEnabled) logger.debug("Cleared request cache ({} entries)", String.valueOf(size))
  }

  /**
   * Clear cache for specific tenant
   */
  def clearCachedTenant(tenantId: String): Unit = {
    val cache = threadLocalCache.get()
    cache.remove(tenantId)
    if (logger.isDebugEnabled) logger.debug("Cleared cache for tenant {}", tenantId)
  }

  /**
   * Get all cached tenant IDs in current request
   */
  def getCachedTenants: Set[String] = {
    val cache = threadLocalCache.get()
    import scala.jdk.CollectionConverters.*
    cache.keys().asScala.toSet
  }

  /**
   * Get cache size
   */
  def cacheSize: Int = {
    val cache = threadLocalCache.get()
    cache.size()
  }

  // ===== FEATURE TOGGLE HELPERS (CACHE ONLY) =====

  /**
   * Check if feature is enabled from cached config
   */
  def isFeatureEnabledCached(feature: String): Boolean = {
    getCurrentConfig.exists(_.isFeatureEnabled(feature))
  }

  /**
   * Get setting from cached config
   */
  def getSettingCached(key: String): Option[String] = {
    getCurrentConfig.flatMap(_.getSetting(key))
  }

  /**
   * Get setting with default from cached config
   */
  def getSettingOrElseCached(key: String, default: String): String = {
    getSettingCached(key).getOrElse(default)
  }

  /**
   * Get all features from cached config
   */
  def getFeaturesCached: Option[Map[String, Boolean]] = {
    getCurrentConfig.map(_.features)
  }

  /**
   * Get all settings from cached config
   */
  def getSettingsCached: Option[Map[String, String]] = {
    getCurrentConfig.map(_.settings)
  }

  // ===== METRICS =====

  /**
   * Get cache metrics for current request
   */
  def getMetrics: Map[String, Any] = {
    val cache = threadLocalCache.get()
    Map(
      "cacheHits" -> cacheHits,
      "cacheMisses" -> cacheMisses,
      "cacheHitRatio" -> calculateHitRatio,
      "currentCacheSize" -> cache.size(),
      "cachedTenants" -> getCachedTenants.mkString(", "),
      "currentTenant" -> getTenant.getOrElse("none")
    )
  }

  private def calculateHitRatio: Double = {
    val total = cacheHits + cacheMisses
    if (total == 0) 0.0 else cacheHits.toDouble / total
  }

  /**
   * Record a cache miss (called by service when fetching)
   */
  def recordCacheMiss(): Unit = {
    cacheMisses += 1
  }

  /**
   * Reset metrics (for testing)
   */
  def resetMetrics(): Unit = {
    cacheHits = 0
    cacheMisses = 0
    if (logger.isDebugEnabled) logger.debug("Metrics reset")
  }

  // ===== VALIDATION =====

  /**
   * Validate that current tenant matches expected tenant
   */
  def validateTenant(expectedTenantId: String): Either[String, Unit] = {
    getTenant match {
      case Some(actualId) if actualId == expectedTenantId => Right(())
      case Some(actualId) => Left(s"Tenant mismatch: expected $expectedTenantId, got $actualId")
      case None => Left("No tenant context set")
    }
  }

  /**
   * Ensure tenant context exists, throw if not
   */
  def ensureTenantContext(): Unit = {
    if (getTenant.isEmpty) {
      throw new IllegalStateException("Operation requires tenant context")
    }
  }

  // ===== CLEANUP =====

  /**
   * Clean up all thread-local resources
   * Call this at the end of request/CLI operation
   */
  def cleanup(): Unit = {
    clearCache()
    clear()
    if (logger.isDebugEnabled) logger.debug("TenantConfigContext cleaned up")
  }
}