package com.aurora

import com.aurora.config.ApplicationConfig
import com.aurora.infrastructure.{MongoDB, TenantDatabaseManager}
import com.aurora.tenant.{TenantConfigContext, TenantConfigService, TenantContext, TenantService}
import com.aurora.tenant.TenantService.TenantRequirements
import com.aurora.api.{TenantConfigRoutes, TenantRoutes, TenantRoutesWithContext}
// Resource limits imports
import com.aurora.tenant.{ResourceType, ResourceLimit, LimitType, TenantResourceLimits, TenantResourceService, TenantResourceUsageManager}
import com.aurora.api.{TenantResourceMiddleware, TenantResourceRoutes}
import com.aurora.api.TenantOnboardingRoutes
import akka.actor.typed.scaladsl.adapter.*

// NEW: Analytics imports
import com.aurora.analytics.*
import com.aurora.api.TenantAnalyticsRoutes
import com.aurora.api.TenantAnalyticsWebSocket

import scala.io.StdIn
import scala.util.{Failure, Success, Try}
import java.time.format.DateTimeFormatter
import java.io.{BufferedReader, InputStreamReader}

import org.slf4j.LoggerFactory
import ch.qos.logback.classic.{Level, Logger}

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import scala.concurrent.{ExecutionContextExecutor, Future}
import akka.http.scaladsl.server.RouteConcatenation.*

/**
 * Main entry point for Aurora Tenant Service
 */
object Main {

  // ------------------------------
  // Logging helpers
  // ------------------------------
  private def logInfo(msg: String): Unit  = println(s"[INFO] [Main] $msg")
  private def logWarn(msg: String): Unit  = println(s"[WARN] [Main] $msg")
  private def logError(msg: String): Unit = println(s"[ERROR] [Main] $msg")

  // Resource limits service
  private lazy val resourceService: TenantResourceService = {
    val service = new TenantResourceService()
    import scala.concurrent.ExecutionContext.Implicits.global
    service.startCleanupScheduler
    service
  }

  // ==========================================================================
  // Analytics Components (Initialized in order of dependencies)
  // ==========================================================================

  // Level 1: Core components with no dependencies
  private lazy val analyticsCollector: TenantMetricsCollector = {
    val collector = new TenantMetricsCollector()
    logInfo("Analytics metrics collector initialized")
    collector
  }

  private lazy val analyticsStorage: TenantAnalyticsStorage = {
    val storage = new TenantAnalyticsStorage()
    logInfo("Analytics storage initialized")
    storage
  }

  private lazy val analyticsAlertService: TenantAnalyticsAlertService = {
    new TenantAnalyticsAlertService()
  }

  // Level 2: Components that depend on Level 1
  private lazy val analyticsQueryService: TenantAnalyticsQueryService = {
    val queryService = new TenantAnalyticsQueryService(analyticsStorage)
    logInfo("Analytics query service initialized")
    queryService
  }

  private lazy val analyticsPredictor: TenantPredictiveAnalytics = {
    val predictor = new TenantPredictiveAnalytics(analyticsStorage)
    logInfo("Analytics predictor initialized")
    predictor
  }

  private lazy val analyticsExportService: TenantAnalyticsExportService = {
    val exportService = new TenantAnalyticsExportService(analyticsStorage)
    logInfo("Analytics export service initialized")
    exportService
  }

  // Level 3: Components that depend on Level 2
  private lazy val analyticsAnomalyDetector: TenantAnomalyDetector = {
    val detector = new TenantAnomalyDetector(analyticsStorage, analyticsAlertService)
    logInfo("Anomaly detector initialized")
    detector
  }

  private lazy val analyticsAggregator: TenantMetricsAggregator = {
    val aggregator = new TenantMetricsAggregator(analyticsCollector, analyticsStorage)
    import scala.concurrent.ExecutionContext.Implicits.global
    aggregator.start()
    logInfo("Analytics aggregator started")
    aggregator
  }

  // Level 4: Components that depend on Level 3
  private lazy val analyticsDashboardService: TenantAnalyticsDashboardService = {
    val dashboard = new TenantAnalyticsDashboardService(
      analyticsStorage,
      analyticsQueryService,
      analyticsPredictor,
      analyticsAnomalyDetector
    )
    logInfo("Analytics dashboard service initialized")
    dashboard
  }

  private lazy val analyticsScheduler: TenantAnalyticsScheduler = {
    val scheduler = new TenantAnalyticsScheduler(
      analyticsCollector,
      analyticsAggregator,
      analyticsStorage,
      analyticsPredictor,
      analyticsAnomalyDetector
    )
    import scala.concurrent.ExecutionContext.Implicits.global
    scheduler.start()
    logInfo("Analytics scheduler started")
    scheduler
  }

  // Note: analyticsWebSocket and analyticsRoutes are created in the HTTP server section
  // because they need the implicit ActorSystem

  // =====================================================
  // Application Lifecycle
  // =====================================================
  def main(args: Array[String]): Unit = {
    // Silence MongoDB driver logs
    val rootLogger  = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[Logger]
    rootLogger.setLevel(Level.WARN)
    val mongoLogger = LoggerFactory.getLogger("org.mongodb.driver").asInstanceOf[Logger]
    mongoLogger.setLevel(Level.WARN)

    // Banner
    println(s"""
               |+--------------------------------------------------+
               ||                                                  ||
               ||   Aurora Tenant Service v${ApplicationConfig.App.version}                    ||
               ||   Environment: ${ApplicationConfig.App.environment}                             ||
               ||                                                  ||
               |+--------------------------------------------------+
               |""".stripMargin)

    // Validate config
    ApplicationConfig.validate() match {
      case Left(error) =>
        logError(s"Configuration validation failed: $error")
        System.exit(1)
      case Right(_) =>
        logInfo("Configuration validated successfully")
    }

    // Initialize MongoDB
    if (!MongoDB.ping()) {
      logError("Failed to connect to MongoDB. Exiting...")
      System.exit(1)
    }

    // Set default system tenant context
    TenantContext.setFullContext("system")

    // Initialize indexes for tenants and configs
    TenantService.initialize() match {
      case Success(_) => logInfo("Database indexes initialized")
      case Failure(e) => logWarn(s"Index initialization warning: ${e.getMessage}")
    }

    // Initialize config indexes for system tenant
    TenantConfigService.ensureIndexes("system") match {
      case Success(_) => logInfo("Config indexes initialized")
      case Failure(e) => logWarn(s"Config index initialization warning: ${e.getMessage}")
    }

    // Initialize resource limits service
    logInfo("Resource limits service initialized")
    TenantResourceUsageManager.cleanup("system")

    // Initialize analytics components (force initialization)
    logInfo("Initializing analytics components...")
    analyticsCollector
    analyticsStorage
    analyticsAlertService
    analyticsQueryService
    analyticsPredictor
    analyticsExportService
    analyticsAnomalyDetector
    analyticsAggregator
    analyticsDashboardService
    analyticsScheduler
    logInfo("All analytics components initialized successfully")

    // ------------------------------
    // Start HTTP server with combined routes
    // ------------------------------
    // ------------------------------
    // Start HTTP server with combined routes
    // ------------------------------
    implicit val typedSystem: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "AuroraSystem")
    implicit val ec: ExecutionContextExecutor = typedSystem.executionContext

    // Convert to classic ActorSystem for WebSocket
    implicit val classicSystem: akka.actor.ActorSystem = typedSystem.toClassic

    val host = ApplicationConfig.Http.host
    val port = ApplicationConfig.Http.port

    // Create route components
    val resourceRoutes = new TenantResourceRoutes(resourceService)
    val resourceMiddleware = new TenantResourceMiddleware(resourceService)
    val onboardingRoutes = new TenantOnboardingRoutes()

    // Create analytics WebSocket (uses classicSystem implicitly)
    val analyticsWebSocket = new TenantAnalyticsWebSocket(analyticsCollector, analyticsStorage)
    logInfo("Analytics WebSocket initialized")

    // Create analytics routes
    val analyticsRoutes = new TenantAnalyticsRoutes(
      analyticsCollector,
      analyticsStorage,
      analyticsPredictor,
      analyticsWebSocket
    )(ec) // Pass execution context explicitly
    logInfo("Analytics routes initialized")

    // Combine all routes
    val allRoutes: Route = resourceMiddleware.withResourceLimits(
      TenantRoutes.routes ~
        TenantConfigRoutes.routes ~
        resourceRoutes.routes ~
        onboardingRoutes.routes ~
        analyticsRoutes.routes
    )

    val serverBindingFuture: Future[Http.ServerBinding] =
      startHttpServer(host, port, allRoutes)

    // ------------------------------
    // Start CLI in main thread
    // ------------------------------
    startCli()

    // ------------------------------
    // Shutdown hook
    // ------------------------------
    sys.addShutdownHook {
      logInfo("Shutting down Aurora Tenant Service...")

      // Stop analytics components gracefully
      logInfo("Stopping analytics components...")
      analyticsScheduler.stop()
      analyticsAggregator.stop()
      TenantAnalyticsContext.cleanupStaleSessions(0) // Clear all sessions

      serverBindingFuture.foreach(_.unbind())
      serverBindingFuture.foreach(_ => logInfo("HTTP server stopped"))
      TenantDatabaseManager.close()
      MongoDB.client.close()
      TenantConfigContext.cleanup()

      // Cleanup resource service
      resourceService.resetUsage("system")

      logInfo("Shutdown complete")
    }
  }

  // =====================================================
  // HTTP Server
  // =====================================================
  private def startHttpServer(host: String, port: Int, routes: Route)
                             (implicit system: ActorSystem[Nothing],
                              ec: ExecutionContextExecutor
                             ): Future[Http.ServerBinding] = {
    // Wrap all routes with TenantContext directive
    val wrappedRoutes: Route = TenantRoutesWithContext.routesWithTenant(routes)

    val bindingFuture = Http().newServerAt(host, port).bind(wrappedRoutes)
    bindingFuture.onComplete {
      case Success(binding) =>
        val address = binding.localAddress
        logInfo(s"HTTP server online at http://${address.getHostString}:${address.getPort}/")
      case Failure(ex) =>
        logError(s"Failed to start HTTP server: ${ex.getMessage}")
        system.terminate()
    }
    bindingFuture
  }

  // =====================================================
  // CLI Mode
  // =====================================================
  private def startCli(): Unit = {
    println("\n=== CLI mode started. Type 'help' for commands ===\n")
    var currentTenantId = "system"
    var dedicatedDB = false
    var running = true
    TenantContext.setFullContext(currentTenantId)

    val reader = new BufferedReader(new InputStreamReader(System.in))

    while (running) {
      try {
        val contextIndicator = if (dedicatedDB) "[D]" else "[S]"
        print(s"$contextIndicator [Tenant: $currentTenantId | Mode: ${if (dedicatedDB) "DEDICATED" else "SHARED"}] > ")

        val input = reader.readLine()
        if (input == null) {
          println("\nInput stream closed. Exiting...")
          running = false
        } else input.trim.toLowerCase match {
          // Tenant commands
          case "help" | "h"       => showHelp()
          case "list" | "ls"      => listTenants()
          case "create" | "c"     => createTenantInteractive()
          case "get" | "g"        => getTenantById()
          case "update" | "u"     => updateTenantName()
          case "deactivate" | "d" => deactivateTenant()
          case "activate"         => activateTenant()
          case "delete"           => deleteTenant()
          case "migrate"          => migrateTenantInteractive()
          case "toggle" | "t" =>
            dedicatedDB = !dedicatedDB
            TenantContext.setFullContext(currentTenantId)
            println(s"[OK] Dedicated DB mode: $dedicatedDB")
          case "context" | "ctx" =>
            print("Enter tenant context ID: ")
            currentTenantId = reader.readLine().trim
            TenantContext.setFullContext(currentTenantId)
            println(s"[OK] Tenant context set to: $currentTenantId")
          case "stats"   => showDetailedStats()
          case "health"  => healthCheck()

          // Config commands
          case "config" | "cfg" => showCurrentConfig()
          case "config-get"      => getConfigSetting()
          case "config-set"      => setConfigSetting()
          case "config-del"      => deleteConfigSetting()
          case "feature" | "f"   => showFeatures()
          case "feature-enable"  => enableFeature()
          case "feature-disable" => disableFeature()
          case "config-reset"    => resetConfig()
          case "config-health"   => configHealthCheck()
          case "config-cache"    => showConfigCache()

          // Resource limits commands
          case "limits"           => showResourceLimits()
          case "limits-set"       => setResourceLimit()
          case "limits-usage"     => showResourceUsage()
          case "limits-status"    => showResourceLimitStatus()

          // Analytics commands
          case "analytics-sessions"   => showAnalyticsSessions()
          case "analytics-metrics"    => showAnalyticsMetrics()
          case "analytics-usage"      => showAnalyticsUsage()
          case "analytics-forecast"   => showAnalyticsForecast()
          case "analytics-anomalies"  => showAnalyticsAnomalies()
          case "analytics-summary"    => showAnalyticsSummary()
          case "analytics-cache"      => showAnalyticsCache()

          case "exit" | "quit" | "q" =>
            println("Goodbye!")
            running = false
          case "" => // ignore empty input
          case cmd =>
            println(s"[ERROR] Unknown command: $cmd. Type 'help' for commands.")
        }
        println()
      } catch {
        case e: Exception =>
          println(s"[ERROR] CLI input error: ${e.getMessage}")
          running = false
      }
    }
  }

  // =====================================================
  // CLI Helper Methods
  // =====================================================
  private def showHelp(): Unit = {
    println("""
AVAILABLE COMMANDS:
------------------
TENANT COMMANDS:
  help, h        - Show this help message
  list, ls       - List active tenants
  create, c      - Create a new tenant
  get, g         - Get tenant by ID
  update, u      - Update tenant name
  deactivate, d  - Soft delete a tenant
  activate       - Activate a tenant
  delete         - Hard delete a tenant (admin)
  migrate        - Migrate tenant to dedicated DB
  toggle, t      - Toggle shared/dedicated DB mode
  context, ctx   - Change tenant context ID
  stats          - Show tenant statistics
  health         - Check system health

CONFIG COMMANDS:
  config, cfg    - Show current tenant config
  config-get     - Get a specific setting value
  config-set     - Set a setting value
  config-del     - Delete a setting
  feature, f     - Show all features
  feature-enable - Enable a feature
  feature-disable- Disable a feature
  config-reset   - Reset config to defaults
  config-health  - Check config service health
  config-cache   - Show config cache status

RESOURCE LIMITS COMMANDS:
  limits         - Show current tenant resource limits
  limits-set     - Set a resource limit (CPU, Memory, APIRequests, ConcurrentRequests)
  limits-usage   - Show current resource usage
  limits-status  - Show limit status with percentages

ANALYTICS COMMANDS:
  analytics-sessions   - Show active sessions
  analytics-metrics    - Show real-time metrics
  analytics-usage      - Show usage patterns
  analytics-forecast   - Show usage forecast
  analytics-anomalies  - Show detected anomalies
  analytics-summary    - Show tenant summary
  analytics-cache      - Show analytics cache status

  exit, quit, q  - Exit the application
    """.stripMargin)
  }

  private def listTenants(): Unit = {
    println("--- Fetching active tenants ---")
    TenantService.getActiveTenants(limit = 20) match {
      case Success(tenants) if tenants.nonEmpty =>
        println(f"\n${"ID"}%-36s | ${"Name"}%-30s | ${"Created At"}%-20s")
        println("-" * 90)
        tenants.foreach { t =>
          val createdAt = DateTimeFormatter.ISO_INSTANT.format(t.createdAt).take(19)
          println(f"${t.id}%-36s | ${t.name}%-30s | $createdAt%-20s")
        }
        println(f"\n[OK] Found ${tenants.size} tenant(s)")
      case Success(_) => println("[INFO] No active tenants found.")
      case Failure(e) => println(s"[ERROR] Failed to fetch tenants: ${e.getMessage}")
    }
  }

  private def createTenantInteractive(): Unit = {
    println("--- Create New Tenant ---")
    print("Enter tenant name: ")
    val name = StdIn.readLine().trim
    if (name.isEmpty) { println("[ERROR] Tenant name cannot be empty"); return }

    print("Use auto-detection for DB type? (y/n) [n]: ")
    val useAutoDetect = StdIn.readLine().trim.toLowerCase == "y"

    val result = if (useAutoDetect) {
      print("Expected number of users: ")
      val users = Try(StdIn.readLine().trim.toInt).getOrElse(100)
      print("Enterprise features needed? (y/n) [n]: ")
      val enterprise = StdIn.readLine().trim.toLowerCase == "y"
      print("Compliance features needed? (y/n) [n]: ")
      val compliance = StdIn.readLine().trim.toLowerCase == "y"
      print("Custom domain needed? (y/n) [n]: ")
      val customDomain = StdIn.readLine().trim.toLowerCase == "y"

      val requirements = TenantRequirements(enterprise, compliance, customDomain, users)
      TenantService.createTenantWithRequirements(name, requirements)
    } else TenantService.createTenant(name)

    result match {
      case Success(tenant) => println(s"[OK] Tenant created: ${tenant.name} (ID: ${tenant.id})")
      case Failure(e)      => println(s"[ERROR] Failed to create tenant: ${e.getMessage}")
    }
  }

  private def getTenantById(): Unit = {
    print("Enter tenant ID: ")
    val id = StdIn.readLine().trim
    TenantService.getTenantById(id) match {
      case Success(Some(t)) =>
        println(s"ID: ${t.id}\nTenant ID: ${t.tenantId}\nName: ${t.name}\nActive: ${t.isActive}")
      case Success(None) =>
        println(s"[ERROR] Tenant '$id' not found")
      case Failure(e) =>
        println(s"[ERROR] Failed to fetch tenant: ${e.getMessage}")
    }
  }

  private def updateTenantName(): Unit = {
    print("Enter tenant ID: ")
    val id = StdIn.readLine().trim
    print("Enter new name: ")
    val newName = StdIn.readLine().trim
    TenantService.updateTenantName(id, newName) match {
      case Success(updated) => println(s"[OK] Updated tenant name: ${updated.name}")
      case Failure(e)      => println(s"[ERROR] Failed to update tenant: ${e.getMessage}")
    }
  }

  private def deactivateTenant(): Unit = {
    print("Enter tenant ID to deactivate: ")
    val id = StdIn.readLine().trim
    TenantService.deactivateTenant(id) match {
      case Success(_) => println("[OK] Tenant deactivated")
      case Failure(e) => println(s"[ERROR] Failed to deactivate tenant: ${e.getMessage}")
    }
  }

  private def activateTenant(): Unit = {
    print("Enter tenant ID to activate: ")
    val id = StdIn.readLine().trim
    TenantService.activateTenant(id) match {
      case Success(_) => println("[OK] Tenant activated")
      case Failure(e) => println(s"[ERROR] Failed to activate tenant: ${e.getMessage}")
    }
  }

  private def deleteTenant(): Unit = {
    print("Enter tenant ID to PERMANENTLY DELETE: ")
    val id = StdIn.readLine().trim
    print("Type 'DELETE' to confirm: ")
    val confirm = StdIn.readLine().trim
    if (confirm == "DELETE") println("[ERROR] Hard delete not implemented")
    else println("[INFO] Deletion cancelled")
  }

  private def migrateTenantInteractive(): Unit = {
    TenantService.migrateCurrentTenantToDedicated() match {
      case Success(true)  => println("[OK] Tenant migrated to dedicated DB")
      case Success(false) => println("[INFO] No migration needed")
      case Failure(e)     => println(s"[ERROR] Migration failed: ${e.getMessage}")
    }
  }

  private def showDetailedStats(): Unit = println("[INFO] Statistics feature coming soon")

  private def healthCheck(): Unit = {
    println("--- Health check ---")
    val mongoHealth = MongoDB.ping()
    val dbHealth    = TenantDatabaseManager.pingDatabase(
      TenantDatabaseManager.getDatabase("system", dedicated = false)
    )

    val analyticsHealth = analyticsStorage.healthCheck()

    println(s"MongoDB: ${if (mongoHealth) "[OK]" else "[FAILED]"}")
    println(s"Database Pool: ${if (dbHealth) "[OK]" else "[FAILED]"}")
    println(s"Analytics Storage: ${analyticsHealth.getOrElse("status", "unknown")}")

    if (mongoHealth && dbHealth) println("[OK] All systems operational")
    else println("[ERROR] Some systems are unhealthy")
  }

  // =====================================================
  // Config CLI Commands
  // =====================================================

  private def showCurrentConfig(): Unit = {
    println("--- Current Tenant Config ---")
    val tenantId = TenantContext.getCurrentTenantId
    if (tenantId != null && tenantId.nonEmpty) {
      TenantConfigService.getConfig(tenantId) match {
        case Success(config) =>
          println(s"Tenant ID: ${config.tenantId}")
          println(s"Version: ${config.version}")
          println(s"Updated: ${config.updatedAt}")
          println(s"Created: ${config.createdAt}")
          println("\n--- Settings ---")
          if (config.settings.isEmpty) println("  (no settings)")
          else config.settings.foreach { case (k, v) => println(s"  $k = $v") }
          println("\n--- Features ---")
          if (config.features.isEmpty) println("  (no features)")
          else config.features.foreach { case (k, v) => println(s"  $k = $v") }
        case Failure(e) => println(s"[ERROR] Failed to get config: ${e.getMessage}")
      }
    } else {
      println("[ERROR] No tenant context")
    }
  }

  private def getConfigSetting(): Unit = {
    print("Enter setting key: ")
    val key = StdIn.readLine().trim
    if (key.isEmpty) { println("[ERROR] Key cannot be empty"); return }

    val tenantId = TenantContext.getCurrentTenantId
    if (tenantId != null && tenantId.nonEmpty) {
      TenantConfigService.getSetting(tenantId, key) match {
        case Success(Some(value)) => println(s"$key = $value")
        case Success(None) => println(s"[INFO] Setting '$key' not found")
        case Failure(e) => println(s"[ERROR] Failed to get setting: ${e.getMessage}")
      }
    } else {
      println("[ERROR] No tenant context")
    }
  }

  private def setConfigSetting(): Unit = {
    print("Enter setting key: ")
    val key = StdIn.readLine().trim
    if (key.isEmpty) { println("[ERROR] Key cannot be empty"); return }

    print("Enter setting value: ")
    val value = StdIn.readLine().trim
    if (value.isEmpty) { println("[ERROR] Value cannot be empty"); return }

    val tenantId = TenantContext.getCurrentTenantId
    if (tenantId != null && tenantId.nonEmpty) {
      TenantConfigService.setSetting(tenantId, key, value) match {
        case Success(_) => println(s"[OK] Setting '$key' set to '$value'")
        case Failure(e) => println(s"[ERROR] Failed to set setting: ${e.getMessage}")
      }
    } else {
      println("[ERROR] No tenant context")
    }
  }

  private def deleteConfigSetting(): Unit = {
    print("Enter setting key to delete: ")
    val key = StdIn.readLine().trim
    if (key.isEmpty) { println("[ERROR] Key cannot be empty"); return }

    val tenantId = TenantContext.getCurrentTenantId
    if (tenantId != null && tenantId.nonEmpty) {
      TenantConfigService.deleteSetting(tenantId, key) match {
        case Success(_) => println(s"[OK] Setting '$key' deleted")
        case Failure(e) => println(s"[ERROR] Failed to delete setting: ${e.getMessage}")
      }
    } else {
      println("[ERROR] No tenant context")
    }
  }

  private def showFeatures(): Unit = {
    println("--- Features ---")
    val tenantId = TenantContext.getCurrentTenantId
    if (tenantId != null && tenantId.nonEmpty) {
      TenantConfigService.getAllFeatures(tenantId) match {
        case Success(features) =>
          if (features.isEmpty) println("  (no features)")
          else features.foreach { case (k, v) =>
            val status = if (v) "[ENABLED]" else "[DISABLED]"
            println(s"  $status $k")
          }
        case Failure(e) => println(s"[ERROR] Failed to get features: ${e.getMessage}")
      }
    } else {
      println("[ERROR] No tenant context")
    }
  }

  private def enableFeature(): Unit = {
    print("Enter feature name to enable: ")
    val feature = StdIn.readLine().trim
    if (feature.isEmpty) { println("[ERROR] Feature name cannot be empty"); return }

    val tenantId = TenantContext.getCurrentTenantId
    if (tenantId != null && tenantId.nonEmpty) {
      TenantConfigService.enableFeature(feature, tenantId) match {
        case Success(_) => println(s"[OK] Feature '$feature' enabled")
        case Failure(e) => println(s"[ERROR] Failed to enable feature: ${e.getMessage}")
      }
    } else {
      println("[ERROR] No tenant context")
    }
  }

  private def disableFeature(): Unit = {
    print("Enter feature name to disable: ")
    val feature = StdIn.readLine().trim
    if (feature.isEmpty) { println("[ERROR] Feature name cannot be empty"); return }

    val tenantId = TenantContext.getCurrentTenantId
    if (tenantId != null && tenantId.nonEmpty) {
      TenantConfigService.disableFeature(feature, tenantId) match {
        case Success(_) => println(s"[OK] Feature '$feature' disabled")
        case Failure(e) => println(s"[ERROR] Failed to disable feature: ${e.getMessage}")
      }
    } else {
      println("[ERROR] No tenant context")
    }
  }

  private def resetConfig(): Unit = {
    print("Reset config to defaults? (type 'YES' to confirm): ")
    val confirm = StdIn.readLine().trim
    if (confirm != "YES") { println("[INFO] Reset cancelled"); return }

    val tenantId = TenantContext.getCurrentTenantId
    if (tenantId != null && tenantId.nonEmpty) {
      TenantConfigService.resetToDefaults(tenantId) match {
        case Success(_) => println("[OK] Config reset to defaults")
        case Failure(e) => println(s"[ERROR] Failed to reset config: ${e.getMessage}")
      }
    } else {
      println("[ERROR] No tenant context")
    }
  }

  private def configHealthCheck(): Unit = {
    println("--- Config Service Health ---")
    val tenantId = TenantContext.getCurrentTenantId
    if (tenantId != null && tenantId.nonEmpty) {
      TenantConfigService.healthCheck(tenantId) match {
        case Success(metrics) =>
          metrics.foreach { case (k, v) => println(s"  $k: $v") }
        case Failure(e) => println(s"[ERROR] Health check failed: ${e.getMessage}")
      }
    } else {
      println("[ERROR] No tenant context")
    }
  }

  private def showConfigCache(): Unit = {
    println("--- Config Cache Status ---")
    val metrics = TenantConfigContext.getMetrics
    println(s"Cache hits: ${metrics("cacheHits")}")
    println(s"Cache misses: ${metrics("cacheMisses")}")
    println(s"Hit ratio: ${metrics("cacheHitRatio")}")
    println(s"Current cache size: ${metrics("currentCacheSize")}")
    println(s"Cached tenants: ${metrics("cachedTenants")}")
    println(s"Current tenant: ${metrics("currentTenant")}")
  }

  // =====================================================
  // Resource Limits CLI Commands
  // =====================================================

  private def showResourceLimits(): Unit = {
    println("--- Resource Limits ---")
    val tenantId = TenantContext.getCurrentTenantId
    if (tenantId != null && tenantId.nonEmpty) {
      resourceService.getLimits(tenantId) match {
        case Some(limits) =>
          println(s"Tenant ID: ${limits.tenantId}")
          println(s"Version: ${limits.version}")
          println(s"Updated: ${limits.updatedAt}")
          println("\n--- Limits ---")
          if (limits.limits.isEmpty) println("  (no limits configured)")
          else limits.limits.foreach { case (resource, limit) =>
            val windowStr = limit.windowSeconds.map(w => s" per ${w}s").getOrElse("")
            println(s"  ${resource.toString}: ${limit.value} ${resource.unit}$windowStr (${limit.limitType})")
            limit.description.foreach(desc => println(s"    Description: $desc"))
          }
        case None => println(s"[INFO] No limits found for tenant $tenantId")
      }
    } else {
      println("[ERROR] No tenant context")
    }
  }

  private def setResourceLimit(): Unit = {
    println("--- Set Resource Limit ---")
    println("Resource types: CPU, Memory, APIRequests, ConcurrentRequests")
    print("Enter resource type: ")
    val resourceStr = scala.io.StdIn.readLine().trim
    print("Enter limit value: ")
    val valueStr = scala.io.StdIn.readLine().trim
    print("Enter limit type (Hard/Soft) [Hard]: ")
    val typeStr = scala.io.StdIn.readLine().trim
    print("Enter window seconds (for rate limits, e.g., 60) [60]: ")
    val windowStr = scala.io.StdIn.readLine().trim

    val tenantId = TenantContext.getCurrentTenantId
    if (tenantId != null && tenantId.nonEmpty) {
      (for {
        resource <- ResourceType.values.find(_.toString.equalsIgnoreCase(resourceStr))
        value <- scala.util.Try(valueStr.toDouble).toOption
        if value > 0
        limitType = if (typeStr.equalsIgnoreCase("soft")) LimitType.Soft else LimitType.Hard
        window = if (windowStr.nonEmpty) scala.util.Try(windowStr.toInt).toOption else Some(60)
      } yield {
        resourceService.getLimits(tenantId) match {
          case Some(limits) =>
            val newLimit = ResourceLimit(value, limitType, window, None)
            val updated = limits.withLimit(resource, newLimit)
            import scala.concurrent.ExecutionContext.Implicits.global
            resourceService.setLimits(tenantId, updated).onComplete {
              case scala.util.Success(true) => println(s"[OK] Set ${resource.toString} limit to $value")
              case _ => println("[ERROR] Failed to set limit")
            }
          case None =>
            val newLimits = TenantResourceLimits(tenantId, Map(resource -> ResourceLimit(value, limitType, window, None)))
            import scala.concurrent.ExecutionContext.Implicits.global
            resourceService.setLimits(tenantId, newLimits).onComplete {
              case scala.util.Success(true) => println(s"[OK] Created limits and set ${resource.toString} to $value")
              case _ => println("[ERROR] Failed to set limit")
            }
        }
        println("[INFO] Request submitted")
      }).getOrElse(println("[ERROR] Invalid input"))
    } else {
      println("[ERROR] No tenant context")
    }
  }

  private def showResourceUsage(): Unit = {
    println("--- Resource Usage ---")
    val tenantId = TenantContext.getCurrentTenantId
    if (tenantId != null && tenantId.nonEmpty) {
      resourceService.getUsage(tenantId) match {
        case Some(usage) =>
          usage.foreach { case (key, value) =>
            println(s"  $key: $value")
          }
        case None => println(s"[INFO] No usage data for tenant $tenantId")
      }
    } else {
      println("[ERROR] No tenant context")
    }
  }

  private def showResourceLimitStatus(): Unit = {
    println("--- Resource Limit Status ---")
    val tenantId = TenantContext.getCurrentTenantId
    if (tenantId != null && tenantId.nonEmpty) {
      val status = resourceService.getLimitStatus(tenantId)
      if (status.isEmpty) {
        println("  No limits configured")
      } else {
        status.foreach { case (resource, metrics) =>
          val current = metrics("current").asInstanceOf[Double]
          val limit = metrics("limit").asInstanceOf[Double]
          val percentage = metrics("percentage").asInstanceOf[Double]
          val statusText = metrics("status").asInstanceOf[String]
          val unit = metrics("unit").asInstanceOf[String]

          val indicator = statusText match {
            case "CRITICAL" => "🔴"
            case "WARNING" => "🟡"
            case "MONITOR" => "🟢"
            case _ => "⚪"
          }

          println(f"  $indicator $resource: $current%.1f / $limit%.1f $unit (${percentage%.1f}%%) - $statusText")
        }
      }
    } else {
      println("[ERROR] No tenant context")
    }
  }

  // =====================================================
  // Analytics CLI Commands
  // =====================================================

  private def showAnalyticsSessions(): Unit = {
    println("--- Active Sessions ---")
    val tenantId = TenantContext.getCurrentTenantId
    val activeSessions = TenantAnalyticsContext.getActiveSessionsCount
    val sessions = TenantAnalyticsContext.getActiveSessions

    println(s"Tenant: $tenantId")
    println(s"Active Sessions: $activeSessions")
    println("\nSession Details:")
    sessions.foreach { case (id, meta) =>
      println(s"  Session: $id")
      println(s"    User: ${meta.userId.getOrElse("anonymous")}")
      println(s"    Started: ${meta.startTimeEpochMs}")
      println(s"    Last Activity: ${meta.lastActivityEpochMs}")
      println(s"    Page Views: ${meta.pageViews}")
      println(s"    Actions: ${meta.actions}")
    }
  }

  private def showAnalyticsMetrics(): Unit = {
    println("--- Real-time Metrics ---")
    val tenantId = TenantContext.getCurrentTenantId

    val bufferSizes = analyticsCollector.getBufferSizes
    val stats = analyticsCollector.getStats

    println(s"Tenant: $tenantId")
    println(s"Buffer Size: ${bufferSizes.getOrElse(tenantId, 0)}")
    println(s"Total Events Recorded: ${stats("total_events_recorded")}")
    println(s"Total Events Sampled: ${stats("total_events_sampled")}")
    println(s"Sampling Rate: ${stats("sampling_rate_avg")}")
  }

  private def showAnalyticsUsage(): Unit = {
    println("--- Usage Patterns ---")
    val tenantId = TenantContext.getCurrentTenantId

    val usage = resourceService.getUsage(tenantId)

    usage.foreach { u =>
      println(s"Tenant: $tenantId")
      u.foreach { case (key, value) =>
        println(s"  $key: $value")
      }
    }
  }

  private def showAnalyticsForecast(): Unit = {
    println("--- Usage Forecast ---")
    val tenantId = TenantContext.getCurrentTenantId

    print("Enter metric to forecast (requestCount/errorCount/avgResponseTime) [requestCount]: ")
    val metric = StdIn.readLine().trim
    val metricToUse = if (metric.isEmpty) "requestCount" else metric

    print("Enter forecast periods [24]: ")
    val periods = Try(StdIn.readLine().trim.toInt).getOrElse(24)

    val forecast = analyticsPredictor.forecastUsage(tenantId, metricToUse, periods)

    println(s"\nForecast for $metricToUse (confidence: ${forecast.confidence})")
    println(s"Method: ${forecast.method}")
    println(s"Message: ${forecast.message}")
    println("\nForecast values:")
    forecast.forecast.zipWithIndex.foreach { case (value, i) =>
      println(s"  Period ${i+1}: $value")
    }
  }

  private def showAnalyticsAnomalies(): Unit = {
    println("--- Detected Anomalies ---")
    val tenantId = TenantContext.getCurrentTenantId

    print("Enter metric to check [requestCount]: ")
    val metric = StdIn.readLine().trim
    val metricToUse = if (metric.isEmpty) "requestCount" else metric

    print("Enter window hours [24]: ")
    val window = Try(StdIn.readLine().trim.toInt).getOrElse(24)

    val anomalies = analyticsPredictor.detectAnomalies(tenantId, metricToUse, window)

    if (anomalies.isEmpty) {
      println("No anomalies detected")
    } else {
      println(s"Found ${anomalies.size} anomalies:")
      anomalies.foreach { a =>
        println(s"  Time: ${a.timestamp}")
        println(s"    Actual: ${a.actualValue}")
        println(s"    Expected: ${a.expectedValue}")
        println(s"    Deviation: ${a.deviation}")
        println(s"    Severity: ${a.severity}")
        println(s"    Method: ${a.method}")
      }
    }
  }

  private def showAnalyticsSummary(): Unit = {
    println("--- Tenant Summary ---")
    val tenantId = TenantContext.getCurrentTenantId

    print("Enter hours to analyze [24]: ")
    val hours = Try(StdIn.readLine().trim.toInt).getOrElse(24)

    analyticsStorage.getTenantSummary(tenantId, hours) match {
      case Success(summary) =>
        println(s"Tenant: ${summary.tenantId}")
        println(s"Time Range: ${summary.timeRange}")
        println(s"Total Requests: ${summary.totalRequests}")
        println(s"Total Errors: ${summary.totalErrors}")
        println(s"Error Rate: ${summary.errorRate}%")
        println(s"Avg Response Time: ${summary.avgResponseTimeMs}ms")
        println(s"P95 Response Time: ${summary.p95ResponseTimeMs}ms")
        println(s"P99 Response Time: ${summary.p99ResponseTimeMs}ms")
        println(s"Peak Concurrent: ${summary.peakConcurrentRequests}")
        println(s"Peak Sessions: ${summary.peakActiveSessions}")
      case Failure(e) =>
        println(s"Error getting summary: ${e.getMessage}")
    }
  }

  private def showAnalyticsCache(): Unit = {
    println("--- Analytics Cache Status ---")
    val stats = analyticsQueryService.getCacheStats

    println(s"Cache Size: ${stats("size")}")
    println(s"Cache Hits: ${stats("hits")}")
    println(s"Cache Misses: ${stats("misses")}")
    println(s"Hit Ratio: ${stats("hitRatio")}")
    println(s"Oldest Entry Age (ms): ${stats("oldestEntryAgeMs")}")
    println(s"Max Cache Size: ${stats("maxSize")}")
  }
}