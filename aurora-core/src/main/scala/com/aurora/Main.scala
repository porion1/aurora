package com.aurora

import com.aurora.config.ApplicationConfig
import com.aurora.infrastructure.{MongoDB, TenantDatabaseManager}
import com.aurora.tenant.{TenantContext, TenantService}
import com.aurora.tenant.TenantService.TenantRequirements

import scala.io.StdIn
import scala.util.{Failure, Success, Try}
import java.time.format.DateTimeFormatter
import java.io.BufferedReader
import java.io.InputStreamReader
import org.slf4j.LoggerFactory
import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger

/**
 * Main entry point for Aurora Tenant Service
 */
object Main {

  // Simple logging
  private def logInfo(msg: String): Unit = println(s"[INFO] [Main] $msg")
  private def logWarn(msg: String): Unit = println(s"[WARN] [Main] $msg")
  private def logError(msg: String): Unit = println(s"[ERROR] [Main] $msg")

  // =====================================================
  // Application Lifecycle
  // =====================================================

  def main(args: Array[String]): Unit = {
    // ===== SILENCE MONGO LOGS =====
    val rootLogger = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[Logger]
    rootLogger.setLevel(Level.WARN)

    val mongoLogger = LoggerFactory.getLogger("org.mongodb.driver").asInstanceOf[Logger]
    mongoLogger.setLevel(Level.WARN)
    // ==============================

    // Print banner
    println(s"""
               |+--------------------------------------------------+
               ||                                                  ||
               ||   Aurora Tenant Service v${ApplicationConfig.App.version}                    ||
               ||   Environment: ${ApplicationConfig.App.environment}                             ||
               ||                                                  ||
               |+--------------------------------------------------+
               |""".stripMargin)

    // Validate configuration
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

    // Initialize indexes for system tenant
    TenantService.initialize() match {
      case Success(_) => logInfo("Database indexes initialized")
      case Failure(e) => logWarn(s"Index initialization warning: ${e.getMessage}")
    }

    // Start CLI
    logInfo("Starting CLI mode...")
    startCli()

    // Add shutdown hook
    sys.addShutdownHook {
      logInfo("Shutting down Aurora Tenant Service...")
      TenantDatabaseManager.close()
      MongoDB.client.close()
      logInfo("Shutdown complete")
    }
  }

  // =====================================================
  // CLI Mode
  // =====================================================

  private def startCli(): Unit = {
    println("\n=== Starting CLI mode. Type 'help' for commands ===\n")

    // Default tenant context
    var currentTenantId = "tenant-context-1"
    var dedicatedDB = false
    var running = true

    // Set initial tenant context
    TenantContext.setContext(currentTenantId)

    val reader = new BufferedReader(new InputStreamReader(System.in))

    while (running) {
      try {
        // Show context
        val contextIndicator = if (dedicatedDB) "[D]" else "[S]"
        print(s"$contextIndicator [Tenant: $currentTenantId | Mode: ${if (dedicatedDB) "DEDICATED" else "SHARED"}] > ")

        val input = reader.readLine()
        if (input == null) {
          println("\nInput stream closed. Exiting...")
          running = false
        } else {
          input.trim.toLowerCase match {
            case "help" | "h" => showHelp()
            case "list" | "ls" => listTenants()
            case "create" | "c" => createTenantInteractive()
            case "get" | "g" => getTenantById()
            case "update" | "u" => updateTenantName()
            case "deactivate" | "d" => deactivateTenant()
            case "activate" => activateTenant()
            case "delete" => deleteTenant()
            case "migrate" => migrateTenantInteractive()
            case "toggle" | "t" =>
              dedicatedDB = !dedicatedDB
              TenantContext.setContext(currentTenantId)
              println(s"[OK] Dedicated DB mode: $dedicatedDB")
            case "context" | "ctx" =>
              print("Enter tenant context ID: ")
              currentTenantId = reader.readLine().trim
              TenantContext.setContext(currentTenantId)
              println(s"[OK] Tenant context set to: $currentTenantId")
            case "stats" => showDetailedStats()
            case "health" => healthCheck()
            case "exit" | "quit" | "q" =>
              println("Goodbye!")
              running = false
            case "" => // Ignore empty input
            case cmd => println(s"[ERROR] Unknown command: $cmd. Type 'help' for available commands.")
          }
        }
        println() // Empty line for readability
      } catch {
        case e: Exception =>
          println(s"[ERROR] Error reading input: ${e.getMessage}")
          running = false
      }
    }
  }

  private def showHelp(): Unit = {
    println("""
AVAILABLE COMMANDS:
------------------
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
exit, quit, q  - Exit the application
    """.stripMargin)
  }

  // =====================================================
  // CLI Command Implementations
  // =====================================================

  private def listTenants(): Unit = {
    println("--- Fetching active tenants...")

    TenantService.getActiveTenants(limit = 20) match {
      case Success(tenants) if tenants.nonEmpty =>
        println(f"\n${"ID"}%-36s | ${"Name"}%-30s | ${"Created At"}%-20s")
        println("-" * 90)
        tenants.foreach { t =>
          val createdAt = DateTimeFormatter.ISO_INSTANT.format(t.createdAt).take(19)
          println(f"${t.id}%-36s | ${t.name}%-30s | $createdAt%-20s")
        }
        println(f"\n[OK] Found ${tenants.size} active tenant(s)")
      case Success(_) =>
        println("[INFO] No active tenants found.")
      case Failure(e) =>
        println(s"[ERROR] Error fetching tenants: ${e.getMessage}")
    }
  }

  private def createTenantInteractive(): Unit = {
    println("--- Create New Tenant ---")
    println("-" * 40)

    print("Enter tenant name: ")
    val name = scala.io.StdIn.readLine().trim

    if (name.isEmpty) {
      println("[ERROR] Tenant name cannot be empty")
      return
    }

    println("\nSelect auto-detection options:")
    print("Use auto-detection for database type? (y/n) [n]: ")
    val useAutoDetect = scala.io.StdIn.readLine().trim.toLowerCase == "y"

    val result = if (useAutoDetect) {
      println("\nEnter expected usage (for auto-detection):")
      print("Expected number of users: ")
      val users = Try(scala.io.StdIn.readLine().trim.toInt).getOrElse(100)
      print("Enterprise features needed? (y/n) [n]: ")
      val enterprise = scala.io.StdIn.readLine().trim.toLowerCase == "y"
      print("Compliance features needed? (y/n) [n]: ")
      val compliance = scala.io.StdIn.readLine().trim.toLowerCase == "y"
      print("Custom domain needed? (y/n) [n]: ")
      val customDomain = scala.io.StdIn.readLine().trim.toLowerCase == "y"

      val requirements = TenantRequirements(
        enterprise = enterprise,
        compliance = compliance,
        customDomain = customDomain,
        expectedUsers = users
      )

      TenantService.createTenantWithRequirements(name, requirements)
    } else {
      TenantService.createTenant(name)
    }

    result match {
      case Success(tenant) =>
        println(s"\n[OK] Tenant created successfully!")
        println(s"   ID: ${tenant.id}")
        println(s"   Name: ${tenant.name}")
        println(s"   Created: ${DateTimeFormatter.ISO_INSTANT.format(tenant.createdAt)}")
      case Failure(e) =>
        println(s"\n[ERROR] Failed to create tenant: ${e.getMessage}")
    }
  }

  private def getTenantById(): Unit = {
    print("Enter tenant record ID: ")
    val id = scala.io.StdIn.readLine().trim

    TenantService.getTenantById(id) match {
      case Success(Some(tenant)) =>
        println("\n--- Tenant Details ---")
        println("-" * 40)
        println(f"ID:           ${tenant.id}")
        println(f"Tenant ID:    ${tenant.tenantId}")
        println(f"Name:         ${tenant.name}")
        println(f"Active:       ${tenant.isActive}")
        println(f"Created:      ${DateTimeFormatter.ISO_INSTANT.format(tenant.createdAt)}")
        println(f"Updated:      ${DateTimeFormatter.ISO_INSTANT.format(tenant.updatedAt)}")
      case Success(None) =>
        println(s"[ERROR] Tenant with ID '$id' not found or inactive")
      case Failure(e) =>
        println(s"[ERROR] Error fetching tenant: ${e.getMessage}")
    }
  }

  private def updateTenantName(): Unit = {
    print("Enter tenant record ID: ")
    val id = scala.io.StdIn.readLine().trim
    print("Enter new name: ")
    val newName = scala.io.StdIn.readLine().trim

    TenantService.updateTenantName(id, newName) match {
      case Success(updatedTenant) =>
        println(s"[OK] Tenant name updated successfully to: ${updatedTenant.name}")
      case Failure(e) =>
        println(s"[ERROR] Failed to update tenant: ${e.getMessage}")
    }
  }

  private def deactivateTenant(): Unit = {
    print("Enter tenant record ID to deactivate: ")
    val id = scala.io.StdIn.readLine().trim

    TenantService.deactivateTenant(id) match {
      case Success(tenant) =>
        println(s"[OK] Tenant deactivated successfully")
      case Failure(e) =>
        println(s"[ERROR] Failed to deactivate tenant: ${e.getMessage}")
    }
  }

  private def activateTenant(): Unit = {
    print("Enter tenant record ID to activate: ")
    val id = scala.io.StdIn.readLine().trim

    TenantService.activateTenant(id) match {
      case Success(tenant) =>
        println(s"[OK] Tenant activated successfully")
      case Failure(e) =>
        println(s"[ERROR] Failed to activate tenant: ${e.getMessage}")
    }
  }

  private def deleteTenant(): Unit = {
    print("[WARN] Enter tenant record ID to PERMANENTLY DELETE: ")
    val id = scala.io.StdIn.readLine().trim
    print("Type 'DELETE' to confirm: ")
    val confirmation = scala.io.StdIn.readLine().trim
    if (confirmation == "DELETE") {
      println("[ERROR] Hard delete not implemented in this version")
    } else {
      println("[INFO] Deletion cancelled")
    }
  }

  private def migrateTenantInteractive(): Unit = {
    TenantService.migrateCurrentTenantToDedicated() match {
      case Success(true) =>
        println(s"[OK] Tenant successfully migrated to dedicated database")
      case Success(false) =>
        println(s"[INFO] No migration needed")
      case Failure(e) =>
        println(s"[ERROR] Migration failed: ${e.getMessage}")
    }
  }

  private def showDetailedStats(): Unit = {
    println("--- Fetching tenant statistics...")
    println("[INFO] Statistics feature coming soon")
  }

  private def healthCheck(): Unit = {
    println("--- Running health checks...")
    val mongoHealth = MongoDB.ping()
    val dbManagerHealth = TenantDatabaseManager.pingDatabase(
      TenantDatabaseManager.getDatabase("system", dedicated = false)
    )
    println(s"\nMongoDB:        ${if (mongoHealth) "[OK]" else "[FAILED]"}")
    println(s"Database Pool:  ${if (dbManagerHealth) "[OK]" else "[FAILED]"}")
    val allHealthy = mongoHealth && dbManagerHealth
    if (allHealthy) {
      println("\n[OK] All systems operational")
    } else {
      println("\n[ERROR] Some systems are unhealthy")
    }
  }
}