package com.aurora.config

import com.typesafe.config.{Config, ConfigFactory}
import scala.jdk.CollectionConverters.*
import scala.util.{Try, Success, Failure}

/**
 * Application configuration with minimal settings
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
  }

  // =====================================================
  // HTTP Server Configuration
  // =====================================================
  object Http {
    val host: String = config.getString("http.host")
    val port: Int = config.getInt("http.port")
  }

  // =====================================================
  // MongoDB Configuration
  // =====================================================
  object Mongo {
    val host: String = config.getString("mongodb.host")
    val port: Int = config.getInt("mongodb.port")
    val database: String = config.getString("mongodb.database")

    // Connection string
    def connectionString: String = s"mongodb://$host:$port"

    // Connection pool settings (with defaults)
    object Pool {
      val maxSize: Int = Try(config.getInt("mongodb.pool.max-size")).getOrElse(100)
      val minSize: Int = Try(config.getInt("mongodb.pool.min-size")).getOrElse(10)
      val maxWaitMs: Int = Try(config.getInt("mongodb.pool.max-wait-ms")).getOrElse(5000)
      val maxIdleMs: Int = Try(config.getInt("mongodb.pool.max-idle-ms")).getOrElse(600000)
      val maxLifeMs: Int = Try(config.getInt("mongodb.pool.max-life-ms")).getOrElse(1800000)
    }

    // Tenant settings (with defaults)
    object Tenant {
      val sharedDbName: String = Try(config.getString("mongodb.tenant.shared-db-name")).getOrElse("aurora_shared")
      val dedicatedDbPrefix: String = Try(config.getString("mongodb.tenant.dedicated-db-prefix")).getOrElse("aurora_")
      val maxTenantsPerShared: Int = Try(config.getInt("mongodb.tenant.max-tenants-per-shared")).getOrElse(1000)
      val autoCreateDedicated: Boolean = Try(config.getBoolean("mongodb.tenant.auto-create-dedicated")).getOrElse(true)
    }
  }

  // =====================================================
  // Tenant Configuration (with defaults)
  // =====================================================
  object Tenant {

    object AutoDetection {
      val userCountThreshold: Int = Try(config.getInt("tenant.auto-detection.user-count-threshold")).getOrElse(500)
    }
  }

  // =====================================================
  // Validation
  // =====================================================

  def validate(): Either[String, Unit] = {
    try {
      // Validate required configs
      require(config.hasPath("mongodb.host"), "mongodb.host is required")
      require(config.hasPath("mongodb.port"), "mongodb.port is required")
      require(config.hasPath("http.port"), "http.port is required")

      // Validate ports
      require(Http.port > 0 && Http.port < 65536, s"Invalid HTTP port: ${Http.port}")
      require(Mongo.port > 0 && Mongo.port < 65536, s"Invalid MongoDB port: ${Mongo.port}")

      Right(())
    } catch {
      case e: Exception => Left(s"Configuration validation failed: ${e.getMessage}")
    }
  }
}