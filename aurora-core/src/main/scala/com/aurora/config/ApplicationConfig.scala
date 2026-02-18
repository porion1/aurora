package com.aurora.config

import com.typesafe.config.{Config, ConfigFactory}
import scala.jdk.CollectionConverters.*
import scala.util.{Try, Success, Failure}

/**
 * Application configuration with minimal settings
 * Provides structured access to App info, HTTP, MongoDB, and Tenant settings.
 */
object ApplicationConfig {

  private val config: Config = ConfigFactory.load()

  // =====================================================
  // Application Info (with defaults)
  // =====================================================
  object App {
    val name: String = Try(config.getString("app.name")).getOrElse("aurora-tenant-service")
    val version: String = Try(config.getString("app.version")).getOrElse("1.0.0")
    val environment: String = Try(config.getString("app.environment")).getOrElse("development")
    val instanceId: String = Try(config.getString("app.instance-id")).getOrElse("instance-1")

    val isDevelopment: Boolean = environment == "development"
    val isProduction: Boolean = environment == "production"

    // Add HTTP delegation for Main.scala
    val httpHost: String = Http.host
    val httpPort: Int = Http.port
  }

  // =====================================================
  // HTTP Server Configuration
  // =====================================================
  object Http {
    val host: String = Try(config.getString("http.host")).getOrElse("0.0.0.0")
    val port: Int = Try(config.getInt("http.port")).getOrElse(8099)
  }

  // =====================================================
  // MongoDB Configuration
  // =====================================================
  object Mongo {
    val host: String = Try(config.getString("mongodb.host")).getOrElse("localhost")
    val port: Int = Try(config.getInt("mongodb.port")).getOrElse(27017)
    val database: String = Try(config.getString("mongodb.database")).getOrElse("aurora_mvp")

    // MongoDB connection string
    def connectionString: String = s"mongodb://$host:$port"

    // Connection pool settings
    object Pool {
      val maxSize: Int = Try(config.getInt("mongodb.pool.max-size")).getOrElse(100)
      val minSize: Int = Try(config.getInt("mongodb.pool.min-size")).getOrElse(10)
      val maxWaitMs: Int = Try(config.getInt("mongodb.pool.max-wait-ms")).getOrElse(5000)
      val maxIdleMs: Int = Try(config.getInt("mongodb.pool.max-idle-ms")).getOrElse(600000)
      val maxLifeMs: Int = Try(config.getInt("mongodb.pool.max-life-ms")).getOrElse(1800000)
    }

    // Tenant-specific Mongo settings
    object Tenant {
      val sharedDbName: String = Try(config.getString("mongodb.tenant.shared-db-name")).getOrElse("aurora_shared")
      val dedicatedDbPrefix: String = Try(config.getString("mongodb.tenant.dedicated-db-prefix")).getOrElse("aurora_")
      val maxTenantsPerShared: Int = Try(config.getInt("mongodb.tenant.max-tenants-per-shared")).getOrElse(1000)
      val autoCreateDedicated: Boolean = Try(config.getBoolean("mongodb.tenant.auto-create-dedicated")).getOrElse(true)
    }
  }

  // =====================================================
  // Tenant Auto-Detection Configuration
  // =====================================================
  object Tenant {
    object AutoDetection {
      val userCountThreshold: Int = Try(config.getInt("tenant.auto-detection.user-count-threshold")).getOrElse(500)
    }
  }

  // =====================================================
  // Configuration Validation
  // =====================================================
  def validate(): Either[String, Unit] = {
    try {
      // Required paths
      require(config.hasPath("mongodb.host"), "mongodb.host is required")
      require(config.hasPath("mongodb.port"), "mongodb.port is required")
      require(config.hasPath("http.host"), "http.host is required")
      require(config.hasPath("http.port"), "http.port is required")

      // Validate port ranges
      require(Http.port > 0 && Http.port < 65536, s"Invalid HTTP port: ${Http.port}")
      require(Mongo.port > 0 && Mongo.port < 65536, s"Invalid MongoDB port: ${Mongo.port}")

      Right(())
    } catch {
      case e: Exception => Left(s"Configuration validation failed: ${e.getMessage}")
    }
  }
}
