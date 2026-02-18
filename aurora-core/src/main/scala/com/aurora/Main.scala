package com.aurora

import com.aurora.config.ApplicationConfig
import com.aurora.infrastructure.{MongoDB, TenantDatabaseManager}
import com.aurora.tenant.{TenantContext, TenantService}
import com.aurora.tenant.TenantService.TenantRequirements
import com.aurora.api.{TenantRoutes, TenantRoutesWithContext}

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

    // Initialize indexes
    TenantService.initialize() match {
      case Success(_) => logInfo("Database indexes initialized")
      case Failure(e) => logWarn(s"Index initialization warning: ${e.getMessage}")
    }

    // ------------------------------
    // Start HTTP server
    // ------------------------------
    implicit val system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "AuroraSystem")
    implicit val ec: ExecutionContextExecutor = system.executionContext
    val host = ApplicationConfig.Http.host
    val port = ApplicationConfig.Http.port

    val serverBindingFuture: Future[Http.ServerBinding] =
      startHttpServer(host, port, TenantRoutes.routes)

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
}
