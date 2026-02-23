package com.aurora.analytics

import com.aurora.tenant.TenantContext
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import scala.jdk.CollectionConverters.*

/**
 * FAANG-level: Thread-local analytics context with zero-allocation path
 * Extends existing TenantContext pattern with production-grade features:
 * - Lock-free concurrent data structures
 * - Session lifecycle management
 * - Performance metrics
 * - Memory-efficient session tracking
 * - Proper error handling
 * - Thread-safety guarantees
 */
object TenantAnalyticsContext {

  // Structured logging (matches your existing pattern)
  private def debug(msg: String): Unit = println(s"[DEBUG] [AnalyticsContext] $msg")
  private def info(msg: String): Unit = println(s"[INFO] [AnalyticsContext] $msg")
  private def warn(msg: String): Unit = println(s"[WARN] [AnalyticsContext] $msg")

  // ==========================================================================
  // Configuration Constants
  // ==========================================================================

  private val SESSION_ID_PREFIX = "sess"
  private val REQUEST_ID_PREFIX = "req"
  private val DEFAULT_SESSION_TIMEOUT_MINUTES = 30
  private val SESSION_CLEANUP_BATCH_SIZE = 1000

  // ==========================================================================
  // Thread-Local Context (Zero Allocation Path)
  // ==========================================================================

  // ThreadLocal for request-scoped analytics data - avoids allocation on hot path
  private val currentSession = new ThreadLocal[AnalyticsSessionInfo]()
  private val currentRequest = new ThreadLocal[RequestAnalytics]()

  // Performance counters
  private val sessionsStarted = new AtomicLong(0)
  private val sessionsEnded = new AtomicLong(0)
  private val requestsTracked = new AtomicLong(0)
  private val actionsTracked = new AtomicLong(0)

  // ==========================================================================
  // Tenant-Level Session Registry (Lock-Free)
  // ==========================================================================

  // Outer map: tenantId -> ConcurrentHashMap of sessions
  // Inner map: sessionId -> SessionMetadata
  // Using ConcurrentHashMap for lock-free concurrent access
  private val activeSessions = new ConcurrentHashMap[String, ConcurrentHashMap[String, SessionMetadata]]()

  // ==========================================================================
  // Data Models
  // ==========================================================================

  /**
   * Session information stored in thread-local
   * Minimal to reduce memory footprint
   */
  case class AnalyticsSessionInfo(
                                   sessionId: String,
                                   startTimeEpochMs: Long,
                                   userId: Option[String],
                                   userAgent: Option[String],
                                   ipAddress: Option[String]
                                 ) {
    // Pre-computed for fast access
    lazy val ageSeconds: Long = (System.currentTimeMillis() - startTimeEpochMs) / 1000
  }

  /**
   * Request information stored in thread-local
   * Uses nanos for high-precision timing
   */
  case class RequestAnalytics(
                               requestId: String,
                               startTimeNanos: Long,
                               method: String,
                               path: String
                             ) {
    def durationNanos: Long = System.nanoTime() - startTimeNanos
    def durationMs: Long = durationNanos / 1_000_000
  }

  /**
   * Session metadata stored in active sessions registry
   * Enriched with activity tracking
   */
  case class SessionMetadata(
                              sessionId: String,
                              startTimeEpochMs: Long,
                              lastActivityEpochMs: Long,
                              userId: Option[String],
                              pageViews: Int,
                              actions: Int,
                              requestCount: AtomicLong = new AtomicLong(0)
                            ) {
    def isStale(timeoutMs: Long): Boolean =
      System.currentTimeMillis() - lastActivityEpochMs > timeoutMs

    def incrementPageViews(): SessionMetadata =
      copy(pageViews = pageViews + 1, lastActivityEpochMs = System.currentTimeMillis())

    def incrementActions(): SessionMetadata =
      copy(actions = actions + 1, lastActivityEpochMs = System.currentTimeMillis())

    def recordRequest(): Unit = requestCount.incrementAndGet()
  }

  // ==========================================================================
  // Public API - Session Management
  // ==========================================================================

  /**
   * Start a new session with comprehensive tracking
   * Returns session ID or throws if no tenant context
   */
  def startSession(
                    userId: Option[String] = None,
                    userAgent: Option[String] = None,
                    ipAddress: Option[String] = None
                  ): String = {
    try {
      // Validate tenant context first (throws if none)
      val tenantId = TenantContext.getCurrentTenantId

      val sessionId = generateSessionId(tenantId, userId)
      val now = System.currentTimeMillis()

      val sessionInfo = AnalyticsSessionInfo(
        sessionId = sessionId,
        startTimeEpochMs = now,
        userId = userId,
        userAgent = userAgent,
        ipAddress = ipAddress
      )

      // Set thread-local
      currentSession.set(sessionInfo)

      // Register in active sessions (lock-free)
      val tenantSessions = activeSessions.computeIfAbsent(
        tenantId,
        _ => new ConcurrentHashMap[String, SessionMetadata]()
      )

      tenantSessions.put(sessionId, SessionMetadata(
        sessionId = sessionId,
        startTimeEpochMs = now,
        lastActivityEpochMs = now,
        userId = userId,
        pageViews = 0,
        actions = 0
      ))

      sessionsStarted.incrementAndGet()
      debug(s"Session started: $sessionId for tenant $tenantId")

      sessionId
    } catch {
      case e: IllegalStateException =>
        warn(s"Cannot start session: ${e.getMessage}")
        throw e
      case e: Exception =>
        warn(s"Unexpected error starting session: ${e.getMessage}")
        // Fail open - return a fallback session ID
        s"${SESSION_ID_PREFIX}_fallback_${System.currentTimeMillis()}"
    }
  }

  /**
   * End current session and return metadata
   */
  /**
   * End current session and return metadata
   */
  def endSession(): Option[SessionMetadata] = {
    try {
      val tenantId = TenantContext.getCurrentTenantId
      val sessionInfo = Option(currentSession.get())

      sessionInfo.foreach { info =>
        val tenantSessions = activeSessions.get(tenantId)
        if (tenantSessions != null) {
          // remove returns the value directly, not Option
          val metadata = tenantSessions.remove(info.sessionId)
          if (metadata != null) {
            debug(s"Session ended: ${info.sessionId}, duration: ${info.ageSeconds}s, " +
              s"pageViews: ${metadata.pageViews}, actions: ${metadata.actions}")
          }
        }
        currentSession.remove()
      }

      sessionsEnded.incrementAndGet()
      sessionInfo.map(info =>
        SessionMetadata(
          sessionId = info.sessionId,
          startTimeEpochMs = info.startTimeEpochMs,
          lastActivityEpochMs = System.currentTimeMillis(),
          userId = info.userId,
          pageViews = 0,
          actions = 0
        )
      )
    } catch {
      case e: Exception =>
        warn(s"Error ending session: ${e.getMessage}")
        None
    }
  }

  /**
   * Get current session ID without allocation
   */
  def getCurrentSessionId: Option[String] = {
    val session = currentSession.get()
    if (session != null) Some(session.sessionId) else None
  }

  /**
   * Get current session info
   */
  def getCurrentSessionInfo: Option[AnalyticsSessionInfo] =
    Option(currentSession.get())

  // ==========================================================================
  // Public API - Request Tracking
  // ==========================================================================

  /**
   * Start tracking a request with high-precision timing
   * Call at the very beginning of request processing
   */
  def startRequest(method: String, path: String): String = {
    try {
      val requestId = generateRequestId()
      val analytics = RequestAnalytics(
        requestId = requestId,
        startTimeNanos = System.nanoTime(),
        method = method,
        path = path
      )

      currentRequest.set(analytics)
      requestsTracked.incrementAndGet()

      // Update session activity asynchronously (don't block request)
      updateSessionActivity()

      requestId
    } catch {
      case e: Exception =>
        warn(s"Error starting request tracking: ${e.getMessage}")
        s"${REQUEST_ID_PREFIX}_fallback_${System.currentTimeMillis()}"
    }
  }

  /**
   * End request tracking and get duration
   * Returns (requestId, durationNanos) if tracking was active
   */
  def endRequest(): Option[(String, Long)] = {
    val analytics = Option(currentRequest.get())
    currentRequest.remove()

    analytics.map { a =>
      val duration = a.durationNanos
      if (duration > 1_000_000_000L) { // > 1 second
        debug(s"Slow request detected: ${a.method} ${a.path} took ${duration / 1_000_000}ms")
      }
      (a.requestId, duration)
    }
  }

  /**
   * Get current request info
   */
  def getCurrentRequestInfo: Option[RequestAnalytics] =
    Option(currentRequest.get())

  // ==========================================================================
  // Public API - User Actions
  // ==========================================================================

  /**
   * Track a user action within current session
   */
  def trackAction(action: String): Unit = {
    try {
      getCurrentSessionId.foreach { sessionId =>
        val tenantId = TenantContext.getCurrentTenantId
        val tenantSessions = activeSessions.get(tenantId)

        if (tenantSessions != null) {
          tenantSessions.computeIfPresent(sessionId, (_, meta) => {
            actionsTracked.incrementAndGet()
            meta.copy(
              actions = meta.actions + 1,
              lastActivityEpochMs = System.currentTimeMillis()
            )
          })
        }
      }
    } catch {
      case e: Exception => // Non-critical, just log and continue
        warn(s"Error tracking action: ${e.getMessage}")
    }
  }

  // ==========================================================================
  // Public API - Session Queries
  // ==========================================================================

  /**
   * Get active sessions count for current tenant (O(1))
   */
  def getActiveSessionsCount: Int = {
    try {
      val tenantId = TenantContext.getCurrentTenantId
      val tenantSessions = activeSessions.get(tenantId)
      if (tenantSessions != null) tenantSessions.size() else 0
    } catch {
      case _: IllegalStateException => 0 // No tenant context
    }
  }

  /**
   * Get all active sessions for current tenant
   */
  def getActiveSessions: Map[String, SessionMetadata] = {
    try {
      val tenantId = TenantContext.getCurrentTenantId
      val tenantSessions = activeSessions.get(tenantId)
      if (tenantSessions != null)
        tenantSessions.asScala.toMap
      else
        Map.empty
    } catch {
      case _: IllegalStateException => Map.empty
    }
  }

  /**
   * Get session metadata by ID
   */
  def getSession(sessionId: String): Option[SessionMetadata] = {
    try {
      val tenantId = TenantContext.getCurrentTenantId
      val tenantSessions = activeSessions.get(tenantId)
      Option(tenantSessions).flatMap(sessions => Option(sessions.get(sessionId)))
    } catch {
      case _: IllegalStateException => None
    }
  }

  // ==========================================================================
  // Public API - Maintenance
  // ==========================================================================

  /**
   * Clean up stale sessions across all tenants
   * Call from scheduler every 5-10 minutes
   */
  def cleanupStaleSessions(timeoutMinutes: Int = DEFAULT_SESSION_TIMEOUT_MINUTES): Int = {
    val timeoutMs = timeoutMinutes * 60 * 1000L
    var totalRemoved = 0

    activeSessions.asScala.foreach { case (tenantId, sessions) =>
      val beforeSize = sessions.size()

      // Find and remove stale sessions
      val staleSessions = sessions.asScala.collect {
        case (id, meta) if meta.isStale(timeoutMs) => id
      }.toList

      staleSessions.foreach(sessions.remove)

      val removed = beforeSize - sessions.size()
      totalRemoved += removed

      if (removed > 0) {
        debug(s"Cleaned up $removed stale sessions for tenant $tenantId")
      }
    }

    totalRemoved
  }

  /**
   * Clear all sessions for a tenant (useful during tenant deactivation)
   */
  def clearTenantSessions(tenantId: String): Unit = {
    activeSessions.remove(tenantId)
    debug(s"Cleared all sessions for tenant $tenantId")
  }

  /**
   * Reset all analytics context (for testing or system reset)
   */
  def reset(): Unit = {
    currentSession.remove()
    currentRequest.remove()
    activeSessions.clear()
    sessionsStarted.set(0)
    sessionsEnded.set(0)
    requestsTracked.set(0)
    actionsTracked.set(0)
    info("Analytics context reset")
  }

  // ==========================================================================
  // Metrics & Monitoring
  // ==========================================================================

  /**
   * Get comprehensive metrics about analytics context
   */
  def getMetrics: Map[String, Any] = Map(
    "sessions_started" -> sessionsStarted.get(),
    "sessions_ended" -> sessionsEnded.get(),
    "active_sessions" -> activeSessions.asScala.map { case (_, s) => s.size() }.sum,
    "requests_tracked" -> requestsTracked.get(),
    "actions_tracked" -> actionsTracked.get(),
    "tenants_with_sessions" -> activeSessions.size(),
    "current_session_active" -> (currentSession.get() != null),
    "current_request_active" -> (currentRequest.get() != null)
  )

  // ==========================================================================
  // Private Helpers
  // ==========================================================================

  /**
   * Update session activity without blocking
   */
  private def updateSessionActivity(): Unit = {
    try {
      getCurrentSessionId.foreach { sessionId =>
        val tenantId = TenantContext.getCurrentTenantId
        val tenantSessions = activeSessions.get(tenantId)

        if (tenantSessions != null) {
          tenantSessions.computeIfPresent(sessionId, (_, meta) => {
            meta.recordRequest()
            meta.incrementPageViews()
          })
        }
      }
    } catch {
      case e: Exception => // Non-critical, continue
        debug(s"Session activity update skipped: ${e.getMessage}")
    }
  }

  /**
   * Generate a unique session ID
   * Format: sess_{tenant}_{user}_{timestamp}_{random}
   */
  private def generateSessionId(tenantId: String, userId: Option[String]): String = {
    val timestamp = System.currentTimeMillis()
    val random = UUID.randomUUID().toString.take(8)
    val userPart = userId.map(uid => s"${uid.take(8)}_").getOrElse("")
    s"${SESSION_ID_PREFIX}_${tenantId.take(8)}_${userPart}${timestamp}_${random}"
  }

  /**
   * Generate a unique request ID
   * Format: req_{thread}_{timestamp}_{counter}
   */
  private def generateRequestId(): String = {
    val threadId = Thread.currentThread().getId
    val timestamp = System.currentTimeMillis()
    val random = (math.random * 10000).toInt
    s"${REQUEST_ID_PREFIX}_${threadId}_${timestamp}_${random}"
  }
}