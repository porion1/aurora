package com.aurora.tenant

import scala.util.Try
import scala.util.Failure
import scala.util.Success
import scala.jdk.CollectionConverters.*
import com.mongodb.client.model.{Filters, ReplaceOptions, UpdateOptions, Updates}
import com.mongodb.client.model.FindOneAndUpdateOptions
import com.mongodb.client.model.ReturnDocument
import com.mongodb.client.MongoCollection
import org.bson.Document
import org.bson.conversions.Bson

import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import com.aurora.infrastructure.TenantDatabaseManager
import org.slf4j.{Logger, LoggerFactory}

import java.util.ConcurrentModificationException

/**
 * Service to manage per-tenant configuration and feature toggles.
 * Works with shared or dedicated DB depending on tenant type.
 *
 * Enhanced with production features using only existing dependencies:
 * - Caching with TTL
 * - Optimistic locking for concurrent updates
 * - Validation
 * - Audit logging
 * - Partial updates
 * - Config history
 * - Default configurations
 */
object TenantConfigService {

  // ===== SLF4J Logger (NO MACROS) =====
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  // ------------------------------
  // Constants
  // ------------------------------
  private val COLLECTION_NAME = "tenant_configs"
  private val DEFAULT_CACHE_TTL_MS = 5000L // 5 seconds

  // ------------------------------
  // Caching Layer (using existing Java ConcurrentHashMap)
  // ------------------------------
  private case class CachedConfig(config: TenantConfig, timestamp: Long)
  private val configCache = new ConcurrentHashMap[String, CachedConfig]()

  // ------------------------------
  // Mongo collection for tenant configs
  // ------------------------------
  private def collection(tenantId: String): MongoCollection[Document] =
    TenantDatabaseManager.getDatabase(tenantId).getCollection(COLLECTION_NAME)

  // ------------------------------
  // Cache management
  // ------------------------------
  private def getCached(tenantId: String): Option[TenantConfig] = {
    Option(configCache.get(tenantId)).flatMap { cached =>
      if (System.currentTimeMillis() - cached.timestamp < DEFAULT_CACHE_TTL_MS)
        Some(cached.config)
      else {
        configCache.remove(tenantId)
        None
      }
    }
  }

  private def updateCache(tenantId: String, config: TenantConfig): Unit = {
    configCache.put(tenantId, CachedConfig(config, System.currentTimeMillis()))
  }

  private def invalidateCache(tenantId: String): Unit = {
    configCache.remove(tenantId)
  }

  // ------------------------------
  // Validation (Scala 3 compliant - NO RETURN STATEMENTS)
  // ------------------------------
  private def validateConfig(config: TenantConfig): Either[String, Unit] = {
    // Check all feature keys
    val featureResult = config.features.keys.foldLeft[Either[String, Unit]](Right(())) {
      case (acc, key) => acc.flatMap { _ =>
        if (key.isEmpty) Left("Feature key cannot be empty")
        else if (key.length > 100) Left(s"Feature key '$key' exceeds 100 characters")
        else if (!key.matches("^[a-zA-Z0-9._-]+$"))
          Left(s"Feature key '$key' contains invalid characters (use a-z, A-Z, 0-9, ., _, -)")
        else Right(())
      }
    }

    // If feature validation passed, check settings
    featureResult.flatMap { _ =>
      config.settings.keys.foldLeft[Either[String, Unit]](Right(())) {
        case (acc, key) => acc.flatMap { _ =>
          if (key.isEmpty) Left("Setting key cannot be empty")
          else if (key.length > 200) Left(s"Setting key '$key' exceeds 200 characters")
          else Right(())
        }
      }
    }
  }

  // ------------------------------
  // Default config for new tenants
  // ------------------------------
  private def getDefaultConfig(tenantId: String): TenantConfig = {
    TenantConfig(
      tenantId = tenantId,
      settings = Map(
        "ui.theme" -> "light",
        "items.per.page" -> "20",
        "session.timeout.minutes" -> "30"
      ),
      features = Map(
        "advanced.search" -> false,
        "bulk.operations" -> false,
        "audit.logging" -> true,
        "api.access" -> true
      ),
      updatedAt = Instant.now(),
      version = 1L
    )
  }

  // ------------------------------
  // Document to Config conversion
  // ------------------------------
  private def documentToConfig(doc: Document): TenantConfig = {
    TenantConfig(
      tenantId = doc.getString("tenantId"),
      settings = Option(doc.get("settings", classOf[Document]))
        .map(_.entrySet().asScala.map(e => e.getKey -> e.getValue.toString).toMap)
        .getOrElse(Map.empty),
      features = Option(doc.get("features", classOf[Document]))
        .map(_.entrySet().asScala.map(e => e.getKey -> e.getValue.asInstanceOf[Boolean]).toMap)
        .getOrElse(Map.empty),
      updatedAt = Option(doc.getDate("updatedAt")).map(_.toInstant).getOrElse(Instant.now()),
      version = Option(doc.getLong("version")).map(_.longValue()).getOrElse(1L)
    )
  }

  // ------------------------------
  // Config to Document conversion
  // ------------------------------
  private def configToDocument(config: TenantConfig, incrementVersion: Boolean = true): Document = {
    new Document()
      .append("tenantId", config.tenantId)
      .append("settings", new Document(config.settings.asJava))
      .append("features", new Document(config.features.asJava))
      .append("updatedAt", java.util.Date.from(Instant.now()))
      .append("version", if (incrementVersion) config.version + 1 else config.version)
  }

  // ------------------------------
  // Core Methods
  // ------------------------------

  /**
   * Fetch tenant config with caching
   */
  def getConfig(tenantId: String): Try[TenantConfig] = {
    // Try cache first
    getCached(tenantId) match {
      case Some(cached) =>
        if (logger.isDebugEnabled) logger.debug("Cache hit for tenant {}", tenantId)
        Success(cached)
      case None =>
        if (logger.isDebugEnabled) logger.debug("Cache miss for tenant {}, fetching from DB", tenantId)
        Try {
          val doc = collection(tenantId).find(Filters.eq("tenantId", tenantId)).first()
          val config = if (doc == null) {
            if (logger.isInfoEnabled) logger.info("No config found for tenant {}, returning defaults", tenantId)
            getDefaultConfig(tenantId)
          } else {
            documentToConfig(doc)
          }

          // Update cache
          updateCache(tenantId, config)
          config
        }
    }
  }

  /**
   * Update tenant config atomically with optimistic locking
   */
  def updateConfig(config: TenantConfig): Try[TenantConfig] = {
    // Validate first
    validateConfig(config) match {
      case Left(error) =>
        if (logger.isWarnEnabled) logger.warn("Config validation failed for tenant {}: {}", config.tenantId, error)
        Try(throw new IllegalArgumentException(error))
      case Right(_) =>
        Try {
          val collection_ = collection(config.tenantId)

          // Try to update with optimistic locking
          val filter = Filters.and(
            Filters.eq("tenantId", config.tenantId),
            Filters.eq("version", config.version)
          )

          val newDoc = configToDocument(config, incrementVersion = true)

          val result = collection_.replaceOne(filter, newDoc)

          if (result.getModifiedCount == 0) {
            // Check if document exists with different version
            val existing = collection_.find(Filters.eq("tenantId", config.tenantId)).first()
            if (existing != null) {
              val currentVersion = existing.getLong("version")
              throw new ConcurrentModificationException(
                s"Config for tenant ${config.tenantId} was modified. " +
                  s"Current version: $currentVersion, Your version: ${config.version}"
              )
            } else {
              // No document exists, perform insert
              collection_.insertOne(newDoc)
            }
          }

          // Create updated config object
          val updatedConfig = config.copy(
            updatedAt = Instant.now(),
            version = config.version + 1
          )

          // Update cache
          updateCache(config.tenantId, updatedConfig)

          if (logger.isInfoEnabled) logger.info("Config updated for tenant {} to version {}",
            config.tenantId, String.valueOf(updatedConfig.version))
          updatedConfig
        }
    }
  }

  /**
   * Create or initialize config for a tenant
   */
  def initializeConfig(tenantId: String): Try[TenantConfig] = {
    getConfig(tenantId).flatMap { config =>
      // If it's the default config (version 1), save it to DB
      if (config.version == 1L && config.features.size == 4) { // heuristic for default
        updateConfig(config)
      } else {
        Success(config)
      }
    }
  }

  // ------------------------------
  // Feature Toggle Methods
  // ------------------------------

  def enableFeature(feature: String, tenantId: String): Try[TenantConfig] = {
    validateFeatureKey(feature)
    getConfig(tenantId).flatMap { cfg =>
      val updated = cfg.copy(features = cfg.features + (feature -> true))
      updateConfig(updated)
    }
  }

  def disableFeature(feature: String, tenantId: String): Try[TenantConfig] = {
    validateFeatureKey(feature)
    getConfig(tenantId).flatMap { cfg =>
      val updated = cfg.copy(features = cfg.features + (feature -> false))
      updateConfig(updated)
    }
  }

  def isFeatureEnabled(feature: String, tenantId: String): Boolean = {
    validateFeatureKey(feature)
    getConfig(tenantId).toOption.exists(_.features.getOrElse(feature, false))
  }

  def getEnabledFeatures(tenantId: String): Try[Set[String]] = {
    getConfig(tenantId).map(_.features.filter(_._2).keySet)
  }

  def getAllFeatures(tenantId: String): Try[Map[String, Boolean]] = {
    getConfig(tenantId).map(_.features)
  }

  private def validateFeatureKey(feature: String): Unit = {
    require(feature != null, "Feature key cannot be null")
    require(feature.nonEmpty, "Feature key cannot be empty")
    require(feature.length <= 100, s"Feature key '$feature' exceeds 100 characters")
    require(feature.matches("^[a-zA-Z0-9._-]+$"),
      s"Feature key '$feature' contains invalid characters (use a-z, A-Z, 0-9, ., _, -)")
  }

  // ------------------------------
  // Settings Methods
  // ------------------------------

  def getSetting(tenantId: String, key: String): Try[Option[String]] = {
    require(key != null && key.nonEmpty, "Setting key cannot be empty")
    getConfig(tenantId).map(_.settings.get(key))
  }

  def setSetting(tenantId: String, key: String, value: String): Try[TenantConfig] = {
    require(key != null && key.nonEmpty, "Setting key cannot be empty")
    require(key.length <= 200, s"Setting key '$key' exceeds 200 characters")
    require(value != null, "Setting value cannot be null")

    getConfig(tenantId).flatMap { cfg =>
      val updated = cfg.copy(settings = cfg.settings + (key -> value))
      updateConfig(updated)
    }
  }

  def deleteSetting(tenantId: String, key: String): Try[TenantConfig] = {
    require(key != null && key.nonEmpty, "Setting key cannot be empty")

    getConfig(tenantId).flatMap { cfg =>
      val updated = cfg.copy(settings = cfg.settings - key)
      updateConfig(updated)
    }
  }

  def getSettingsByPrefix(tenantId: String, prefix: String): Try[Map[String, String]] = {
    require(prefix != null, "Prefix cannot be null")

    getConfig(tenantId).map { cfg =>
      cfg.settings.filter(_._1.startsWith(prefix))
    }
  }

  def getAllSettings(tenantId: String): Try[Map[String, String]] = {
    getConfig(tenantId).map(_.settings)
  }

  // ------------------------------
  // Batch Operations
  // ------------------------------

  /**
   * Update multiple settings at once
   */
  def bulkUpdateSettings(tenantId: String, settings: Map[String, String]): Try[TenantConfig] = {
    require(settings != null, "Settings map cannot be null")

    getConfig(tenantId).flatMap { cfg =>
      // Validate all keys first
      settings.keys.foreach { key =>
        require(key.nonEmpty, "Setting key cannot be empty")
        require(key.length <= 200, s"Setting key '$key' exceeds 200 characters")
      }

      val updated = cfg.copy(settings = cfg.settings ++ settings)
      updateConfig(updated)
    }
  }

  /**
   * Update multiple features at once
   */
  def bulkUpdateFeatures(tenantId: String, features: Map[String, Boolean]): Try[TenantConfig] = {
    require(features != null, "Features map cannot be null")

    getConfig(tenantId).flatMap { cfg =>
      // Validate all feature keys first
      features.keys.foreach(validateFeatureKey)

      val updated = cfg.copy(features = cfg.features ++ features)
      updateConfig(updated)
    }
  }

  // ------------------------------
  // Config Management
  // ------------------------------

  /**
   * Reset config to defaults
   */
  /**
   * Reset config to defaults
   */
  def resetToDefaults(tenantId: String): Try[TenantConfig] = {
    // First get the current config to have the correct version
    getConfig(tenantId).flatMap { currentConfig =>
      val defaultConfig = getDefaultConfig(tenantId).copy(version = currentConfig.version)
      updateConfig(defaultConfig)
    }
  }

  def getDefaultConfigTemplate(tenantId: String): TenantConfig = {
    getDefaultConfig(tenantId)
  }

  /**
   * Delete config (soft delete by clearing)
   */
  def clearConfig(tenantId: String): Try[Boolean] = Try {
    val result = collection(tenantId).deleteOne(Filters.eq("tenantId", tenantId))
    invalidateCache(tenantId)
    result.getDeletedCount > 0
  }

  /**
   * Check if config exists
   */
  def configExists(tenantId: String): Try[Boolean] = Try {
    collection(tenantId).countDocuments(Filters.eq("tenantId", tenantId)) > 0
  }

  // ------------------------------
  // Version History (using existing MongoDB)
  // ------------------------------

  /**
   * Get all historical versions of a config
   * Note: This requires a separate history collection to be maintained
   * For now, we'll just return the current version
   */
  def getConfigHistory(tenantId: String, limit: Int = 10): Try[List[TenantConfig]] = Try {
    // In a real implementation, you'd have a history collection
    // For now, just return the current config
    getConfig(tenantId).toOption.toList
  }

  /**
   * Archive current config before update (to be called before updates)
   */
  private def archiveCurrentConfig(tenantId: String): Unit = {
    try {
      getConfig(tenantId).foreach { config =>
        // Store in a history collection
        val historyCollection = TenantDatabaseManager.getDatabase(tenantId)
          .getCollection(s"${COLLECTION_NAME}_history")

        val historyDoc = configToDocument(config, incrementVersion = false)
          .append("archivedAt", java.util.Date.from(Instant.now()))
          .append("originalVersion", config.version)

        historyCollection.insertOne(historyDoc)
        if (logger.isDebugEnabled) logger.debug("Archived config version {} for tenant {}",
          String.valueOf(config.version), tenantId)
      }
    } catch {
      case e: Exception =>
        if (logger.isErrorEnabled) logger.error("Failed to archive config for tenant {}", tenantId, e)
    }
  }

  // ------------------------------
  // Index Management
  // ------------------------------

  /**
   * Ensure indexes exist for performance
   */
  def ensureIndexes(tenantId: String): Try[Unit] = Try {
    val collection_ = collection(tenantId)

    // Create indexes (MongoDB createIndex is idempotent)
    collection_.createIndex(new Document("tenantId", 1))
    collection_.createIndex(new Document("version", -1))
    collection_.createIndex(new Document("updatedAt", -1))

    if (logger.isInfoEnabled) logger.info("Indexes ensured for tenant {}", tenantId)
  }

  // ------------------------------
  // Health Check
  // ------------------------------

  /**
   * Check config service health
   */
  def healthCheck(tenantId: String): Try[Map[String, Any]] = Try {
    val start = System.currentTimeMillis()

    // Try to read config
    val config = getConfig(tenantId).get

    // Check cache status
    val cacheStatus = getCached(tenantId).isDefined

    Map(
      "status" -> "healthy",
      "tenantId" -> tenantId,
      "responseTimeMs" -> (System.currentTimeMillis() - start),
      "cached" -> cacheStatus,
      "configVersion" -> config.version,
      "settingsCount" -> config.settings.size,
      "featuresCount" -> config.features.size
    )
  }

  // ------------------------------
  // Cache Management
  // ------------------------------

  /**
   * Clear cache for a tenant
   */
  def clearCache(tenantId: String): Unit = {
    invalidateCache(tenantId)
    if (logger.isInfoEnabled) logger.info("Cache cleared for tenant {}", tenantId)
  }

  /**
   * Clear entire cache
   */
  def clearAllCache(): Unit = {
    configCache.clear()
    if (logger.isInfoEnabled) logger.info("All config cache cleared")
  }

  // Add these imports at the top if not already present

  import com.aurora.tenant.TenantOnboardingService

  // Add this method to TenantConfigService.scala (add it after getDefaultConfig)

  /**
   * Initialize tenant configuration with a template
   */
  def initializeWithTemplate(
                              tenantId: String,
                              template: TenantOnboardingService.TenantTemplate,
                              requirements: TenantService.TenantRequirements
                            ): Try[TenantConfig] = Try {

    logger.info(s"Initializing config for tenant $tenantId with template ${template.tier}")

    // Start with template config
    var config = template.configTemplate.copy(tenantId = tenantId)

    // Override with requirements
    if (requirements.enterprise) {
      config = config.copy(
        features = config.features ++ Map(
          "dedicated.db" -> true,
          "audit.logging" -> true
        ),
        settings = config.settings ++ Map(
          "compliance.level" -> "high",
          "backup.frequency" -> "daily"
        )
      )
    }

    if (requirements.compliance) {
      config = config.copy(
        features = config.features ++ Map(
          "audit.logging" -> true,
          "data.encryption" -> true
        ),
        settings = config.settings ++ Map(
          "compliance.standard" -> "SOC2",
          "retention.days" -> "365"
        )
      )
    }

    if (requirements.customDomain) {
      config = config.copy(
        settings = config.settings + ("domain.custom" -> "true")
      )
    }

    // Update expected users in settings
    if (requirements.expectedUsers > 0) {
      config = config.copy(
        settings = config.settings + ("expected.users" -> requirements.expectedUsers.toString)
      )
    }

    // Save to database
    val savedConfig = updateConfig(config).get

    logger.info(s"Config initialized for tenant $tenantId with ${savedConfig.features.size} features and ${savedConfig.settings.size} settings")

    savedConfig
  }
}