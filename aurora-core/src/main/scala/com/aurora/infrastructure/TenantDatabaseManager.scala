package com.aurora.infrastructure

import com.aurora.config.ApplicationConfig
import com.mongodb.client.{MongoClient, MongoClients, MongoDatabase}
import com.mongodb.{ConnectionString, MongoClientSettings}
import scala.collection.concurrent.TrieMap
import scala.jdk.CollectionConverters.*
import scala.util.Try
import java.util.concurrent.TimeUnit
import com.mongodb.event.{CommandStartedEvent, CommandListener}
import java.util.logging.Level
import java.util.logging.Logger

/**
 * TenantDatabaseManager handles multi-tenant database selection with connection pooling.
 */
object TenantDatabaseManager {

  // Simple logging
  private def logInfo(msg: String): Unit = println(s"[INFO] [TenantDatabaseManager] $msg")
  private def logWarn(msg: String): Unit = println(s"[WARN] [TenantDatabaseManager] $msg")
  private def logError(msg: String): Unit = println(s"[ERROR] [TenantDatabaseManager] $msg")

  // Get MongoDB connection string from config
  private def getConnectionString: String = {
    val host = ApplicationConfig.Mongo.host
    val port = ApplicationConfig.Mongo.port
    s"mongodb://$host:$port"
  }

  // Enhanced connection settings
  private val settings = MongoClientSettings.builder()
    .applyConnectionString(new ConnectionString(getConnectionString))
    .applyToConnectionPoolSettings(builder =>
      builder
        .maxSize(ApplicationConfig.Mongo.Pool.maxSize)
        .minSize(ApplicationConfig.Mongo.Pool.minSize)
        .maxWaitTime(ApplicationConfig.Mongo.Pool.maxWaitMs, TimeUnit.MILLISECONDS)
        .build()
    )
    .build()

  private val client: MongoClient = MongoClients.create(settings)

  // Cache for per-tenant DB connections
  private val tenantDbCache = TrieMap[String, MongoDatabase]()

  // Track tenant types (shared vs dedicated)
  private val tenantTypeCache = TrieMap[String, Boolean]()

  // Shared DB for small tenants
  private val sharedDatabase: MongoDatabase =
    client.getDatabase(ApplicationConfig.Mongo.Tenant.sharedDbName)

  // =====================================================
  // Public Methods
  // =====================================================

  /**
   * Get database for a tenant
   */
  def getDatabase(tenantId: String, dedicated: Boolean = false): MongoDatabase = {
    if (!dedicated) {
      sharedDatabase
    } else {
      tenantDbCache.getOrElseUpdate(tenantId, {
        val dbName = s"${ApplicationConfig.Mongo.Tenant.dedicatedDbPrefix}$tenantId"
        logInfo(s"Creating dedicated DB connection for tenant '$tenantId': $dbName")
        client.getDatabase(dbName)
      })
    }
  }

  /**
   * Get tenant type from cache
   */
  def getTenantType(tenantId: String): Option[Boolean] = {
    tenantTypeCache.get(tenantId)
  }

  /**
   * Determine if tenant needs dedicated database based on requirements
   */
  def determineTenantType(tenantId: String, userCount: Int, requirements: TenantRequirements): Boolean = {
    val needsDedicated =
      userCount > ApplicationConfig.Tenant.AutoDetection.userCountThreshold ||
        requirements.enterprise ||
        requirements.compliance ||
        requirements.customDomain ||
        requirements.expectedUsers > 1000

    tenantTypeCache.put(tenantId, needsDedicated)
    logInfo(s"Tenant '$tenantId' auto-detected as: ${if(needsDedicated) "DEDICATED" else "SHARED"}")
    needsDedicated
  }

  /**
   * Migrate tenant data from shared to dedicated database
   */
  def migrateToDedicated(tenantId: String): Try[Boolean] = Try {
    logInfo(s"Starting migration for tenant '$tenantId' from shared to dedicated DB")

    val sourceDb = getDatabase(tenantId, dedicated = false)
    val targetDb = getDatabase(tenantId, dedicated = true)

    // Get all collections from source database
    val collections = sourceDb.listCollectionNames().asScala.toList

    if (collections.isEmpty) {
      logInfo(s"No collections found for tenant '$tenantId'")
      tenantTypeCache.put(tenantId, true)
      // FIXED: Removed the 'return' statement
      true
    } else {
      collections.foreach { collectionName =>
        logInfo(s"Migrating collection: $collectionName")

        val sourceCollection = sourceDb.getCollection(collectionName)
        val targetCollection = targetDb.getCollection(collectionName)

        // Copy all documents
        val documents = sourceCollection.find().iterator().asScala.toList
        if (documents.nonEmpty) {
          targetCollection.insertMany(documents.asJava)
          logInfo(s"Migrated ${documents.size} documents from $collectionName")
        }

        // Copy indexes
        val indexes = sourceCollection.listIndexes().iterator().asScala.toList
        indexes.foreach { indexDoc =>
          // Skip the default _id index
          if (indexDoc.getString("name") != "_id_") {
            val keys = indexDoc.get("key").asInstanceOf[org.bson.Document]
            val options = new com.mongodb.client.model.IndexOptions()

            if (indexDoc.containsKey("unique") && indexDoc.getBoolean("unique")) {
              options.unique(true)
            }
            if (indexDoc.containsKey("name")) {
              options.name(indexDoc.getString("name"))
            }

            targetCollection.createIndex(keys, options)
            logInfo(s"Created index: ${indexDoc.getString("name")}")
          }
        }
      }

      // Update tenant type cache
      tenantTypeCache.put(tenantId, true)
      logInfo(s"Migration completed successfully for tenant '$tenantId'")
      true
    }
  }

  /**
   * Ping database to check connectivity
   */
  def pingDatabase(db: MongoDatabase): Boolean = {
    try {
      val start = System.currentTimeMillis()
      val result = db.runCommand(new org.bson.Document("ping", 1))
      val latency = System.currentTimeMillis() - start

      if (result.getDouble("ok") == 1.0) {
        logInfo(s"Database ping successful: ${db.getName} (latency: ${latency}ms)")
        true
      } else {
        logError(s"Database ping failed: ${db.getName}")
        false
      }
    } catch {
      case e: Exception =>
        logError(s"Database ping failed: ${e.getMessage}")
        false
    }
  }

  /**
   * Get database statistics
   */
  def getDatabaseStats(db: MongoDatabase): Try[DatabaseStats] = Try {
    val stats = db.runCommand(new org.bson.Document("dbStats", 1))
    DatabaseStats(
      databaseName = db.getName,
      collections = stats.getInteger("collections", 0),
      indexes = stats.getInteger("indexes", 0),
      documents = stats.getLong("objects", 0L),
      dataSizeMB = stats.getDouble("dataSize", 0.0) / (1024 * 1024),
      storageSizeMB = stats.getDouble("storageSize", 0.0) / (1024 * 1024),
      indexSizeMB = stats.getDouble("totalIndexSize", 0.0) / (1024 * 1024)
    )
  }

  /**
   * Get connection pool statistics
   */
  def getPoolStats(): PoolStats = {
    PoolStats(
      cachedConnections = tenantDbCache.size,
      sharedDbInUse = true,
      dedicatedDbsCount = tenantDbCache.count { case (_, db) =>
        db.getName != ApplicationConfig.Mongo.Tenant.sharedDbName
      }
    )
  }

  /**
   * Remove tenant from cache (for cleanup)
   */
  def evictTenant(tenantId: String): Unit = {
    tenantDbCache.remove(tenantId)
    tenantTypeCache.remove(tenantId)
    logInfo(s"Evicted tenant '$tenantId' from cache")
  }

  /**
   * Check if tenant exists in cache
   */
  def isTenantCached(tenantId: String): Boolean = {
    tenantDbCache.contains(tenantId)
  }

  /**
   * Close all database connections
   */
  def close(): Unit = {
    logInfo("Closing all database connections...")
    try {
      client.close()
      tenantDbCache.clear()
      tenantTypeCache.clear()
      logInfo("Database connections closed successfully")
    } catch {
      case e: Exception =>
        logError(s"Error closing database connections: ${e.getMessage}")
    }
  }

  /**
   * Get MongoDB client (for advanced use cases)
   */
  def getClient: MongoClient = client
}

// =====================================================
// Case Classes
// =====================================================

/**
 * Tenant requirements for auto-detection
 */
case class TenantRequirements(
                               enterprise: Boolean = false,
                               compliance: Boolean = false,
                               customDomain: Boolean = false,
                               sla: Double = 99.9,
                               expectedUsers: Int = 0
                             )

/**
 * Database statistics
 */
case class DatabaseStats(
                          databaseName: String,
                          collections: Int,
                          indexes: Int,
                          documents: Long,
                          dataSizeMB: Double,
                          storageSizeMB: Double,
                          indexSizeMB: Double
                        )

/**
 * Connection pool statistics
 */
case class PoolStats(
                      cachedConnections: Int,
                      sharedDbInUse: Boolean,
                      dedicatedDbsCount: Int
                    )