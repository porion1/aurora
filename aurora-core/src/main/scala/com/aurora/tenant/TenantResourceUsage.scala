package com.aurora.tenant

import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import scala.collection.concurrent.TrieMap
import scala.jdk.CollectionConverters.*

/**
 * Tracks real-time resource usage for a tenant
 */
class TenantResourceUsage(tenantId: String):
  // CPU tracking (approximated by active requests)
  private val activeRequests = new AtomicInteger(0)

  // Memory tracking (approximated by peak usage)
  private val peakMemoryMb = new AtomicLong(0)
  private val currentMemoryMb = new AtomicLong(0)

  // Request rate tracking with sliding windows
  private val requestTimestamps = ConcurrentHashMap.newKeySet[Long]().asScala

  // Total requests served
  private val totalRequests = new AtomicLong(0)

  // Last reset time for rate windows
  @volatile private var lastCleanup = Instant.now()

  /**
   * Track an incoming request
   * @return Current concurrent request count
   */
  def trackRequestStart(): Int =
    val concurrent = activeRequests.incrementAndGet()
    val now = System.currentTimeMillis()
    requestTimestamps.add(now)
    totalRequests.incrementAndGet()
    cleanupOldWindows()
    concurrent

  /**
   * Track request completion
   */
  def trackRequestEnd(): Unit =
    activeRequests.decrementAndGet()

  /**
   * Update memory usage (called periodically or on significant changes)
   */
  def updateMemoryUsage(mb: Long): Unit =
    currentMemoryMb.set(mb)
    var peak = peakMemoryMb.get()
    while mb > peak && !peakMemoryMb.compareAndSet(peak, mb) do
      peak = peakMemoryMb.get()

  /**
   * Get current concurrent requests
   */
  def getConcurrentRequests: Int = activeRequests.get()

  /**
   * Get request rate for the last N seconds
   */
  def getRequestRate(windowSeconds: Int): Int =
    cleanupOldWindows()
    val cutoff = System.currentTimeMillis() - (windowSeconds * 1000L)
    requestTimestamps.count(_ > cutoff)

  /**
   * Get peak memory usage
   */
  def getPeakMemoryMb: Long = peakMemoryMb.get()

  /**
   * Get current memory usage
   */
  def getCurrentMemoryMb: Long = currentMemoryMb.get()

  /**
   * Get total requests served
   */
  def getTotalRequests: Long = totalRequests.get()

  /**
   * Get usage statistics
   */
  def getStats: Map[String, Any] = Map(
    "tenantId" -> tenantId,
    "concurrentRequests" -> getConcurrentRequests,
    "requestsLastMinute" -> getRequestRate(60),
    "requestsLastHour" -> getRequestRate(3600),
    "peakMemoryMb" -> getPeakMemoryMb,
    "currentMemoryMb" -> getCurrentMemoryMb,
    "totalRequests" -> getTotalRequests,
    "timestamp" -> Instant.now().toString
  )

  /**
   * Clean up old request timestamps
   */
  private def cleanupOldWindows(): Unit =
    val now = Instant.now()
    if now.getEpochSecond - lastCleanup.getEpochSecond > 60 then // Cleanup every minute
      val cutoff = System.currentTimeMillis() - (3600 * 1000L) // Keep last hour
      requestTimestamps.retain(_ > cutoff)
      lastCleanup = now

  /**
   * Reset all counters (for testing or tenant reset)
   */
  def reset(): Unit =
    activeRequests.set(0)
    peakMemoryMb.set(0)
    currentMemoryMb.set(0)
    requestTimestamps.clear()
    totalRequests.set(0)
    lastCleanup = Instant.now()

/**
 * Manager for all tenant resource usage trackers
 */
object TenantResourceUsageManager:
  private val usageTrackers = new TrieMap[String, TenantResourceUsage]()

  def getOrCreate(tenantId: String): TenantResourceUsage =
    usageTrackers.getOrElseUpdate(tenantId, new TenantResourceUsage(tenantId))

  def get(tenantId: String): Option[TenantResourceUsage] = usageTrackers.get(tenantId)

  def remove(tenantId: String): Unit = usageTrackers.remove(tenantId)

  def getAllUsage: Map[String, Map[String, Any]] =
    usageTrackers.map { case (id, tracker) => id -> tracker.getStats }.toMap

  def cleanup(tenantId: String): Unit =
    usageTrackers.get(tenantId).foreach(_.reset())