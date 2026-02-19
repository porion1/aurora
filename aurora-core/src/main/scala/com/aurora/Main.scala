package com.aurora

import com.aurora.config.ApplicationConfig
import com.aurora.infrastructure.{MongoDB, TenantDatabaseManager}
import com.aurora.tenant.{TenantContext, TenantService, TenantConfigService, TenantConfigContext}
import com.aurora.tenant.TenantService.TenantRequirements
import com.aurora.api.{TenantRoutes, TenantRoutesWithContext, TenantConfigRoutes}
// NEW: Resource limits imports
import com.aurora.tenant.{ResourceType, ResourceLimit, LimitType, TenantResourceLimits, TenantResourceService, TenantResourceUsageManager}
import com.aurora.api.{TenantResourceMiddleware, TenantResourceRoutes}

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

  // NEW: Resource limits service
  private lazy val resourceService: TenantResourceService = {
    val service = new TenantResourceService()
    import scala.concurrent.ExecutionContext.Implicits.global
    service.startCleanupScheduler
    service
  }

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

    // NEW: Initialize resource limits service
    logInfo("Resource limits service initialized")
    TenantResourceUsageManager.cleanup("system")

    // ------------------------------
    // Start HTTP server with combined routes
    // ------------------------------
    implicit val system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "AuroraSystem")
    implicit val ec: ExecutionContextExecutor = system.executionContext
    val host = ApplicationConfig.Http.host
    val port = ApplicationConfig.Http.port

    // NEW: Combine existing routes with resource limits routes
    // Combine existing tenant routes with new config and resource routes
    val resourceRoutes = new TenantResourceRoutes(resourceService)
    val resourceMiddleware = new TenantResourceMiddleware(resourceService)

    // Wrap all routes with resource limit middleware
    val allRoutes: Route = resourceMiddleware.withResourceLimits(
      TenantRoutes.routes ~ TenantConfigRoutes.routes ~ resourceRoutes.routes
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
      serverBindingFuture.foreach(_.unbind())
      serverBindingFuture.foreach(_ => logInfo("HTTP server stopped"))
      TenantDatabaseManager.close()
      MongoDB.client.close()
      TenantConfigContext.cleanup()
      // NEW: Cleanup resource service
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
          // Existing commands
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

          // NEW: Resource limits commands
          case "limits"           => showResourceLimits()
          case "limits-set"       => setResourceLimit()
          case "limits-usage"     => showResourceUsage()
          case "limits-status"    => showResourceLimitStatus()

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
  // CLI Helper Methods (Existing)
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
    println(s"MongoDB: ${if (mongoHealth) "[OK]" else "[FAILED]"}")
    println(s"Database Pool: ${if (dbHealth) "[OK]" else "[FAILED]"}")
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
  // NEW Resource Limits CLI Commands
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
}