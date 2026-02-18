package com.aurora.tenant

import java.time.Instant

/**
 * Represents per-tenant configuration and feature toggles.
 *
 * Production-ready enhancements:
 * - Audit fields (created/updated by)
 * - Config categorization and tagging
 * - Expiration/TTL support
 * - Inheritance/parent config support
 * - Metadata for operations
 * - Schema versioning for validation
 * - Config status (active/deprecated/experimental)
 * - Environment targeting
 * - Priority/order for rollouts
 *
 * All fields have defaults to maintain backward compatibility
 * with existing stored documents.
 */
case class TenantConfig(
                         // ===== Core Identifiers =====
                         /** Unique tenant identifier */
                         tenantId: String,

                         // ===== Configuration Data =====
                         /** Key-value settings (strings, numbers, JSON strings) */
                         settings: Map[String, String] = Map.empty,

                         /** Feature toggles (boolean flags) */
                         features: Map[String, Boolean] = Map.empty,

                         // ===== Version Control =====
                         /** Optimistic locking version */
                         version: Long = 1L,

                         // ===== Timestamps =====
                         /** Last update timestamp */
                         updatedAt: Instant = Instant.now(),

                         /** Creation timestamp */
                         createdAt: Instant = Instant.now(),

                         /** When this config becomes effective (for scheduled rollouts) */
                         effectiveFrom: Option[Instant] = None,

                         /** When this config expires (TTL support) */
                         expiresAt: Option[Instant] = None,

                         // ===== Audit Fields =====
                         /** Who created this config (username/service) */
                         createdBy: Option[String] = None,

                         /** Who last updated this config */
                         updatedBy: Option[String] = None,

                         /** Reason for last change (audit trail) */
                         changeReason: Option[String] = None,

                         // ===== Categorization =====
                         /** Config category/group (e.g., "ui", "api", "billing") */
                         category: Option[String] = None,

                         /** Tags for filtering and organization */
                         tags: Set[String] = Set.empty,

                         /** Environment this config applies to (dev/staging/production) */
                         environment: Option[String] = None,

                         // ===== Inheritance =====
                         /** Parent tenant ID for config inheritance */
                         inheritsFrom: Option[String] = None,

                         /** Inheritance strategy: "merge" or "override" */
                         inheritanceStrategy: String = "merge", // merge or override

                         // ===== Status & Lifecycle =====
                         /** Config status: active, deprecated, experimental, disabled */
                         status: String = "active",

                         /** Priority for phased rollouts (higher = applied first) */
                         priority: Int = 0,

                         /** Percentage of requests that should use this config (canary) */
                         rolloutPercentage: Option[Int] = None,

                         // ===== Documentation =====
                         /** Human-readable description */
                         description: Option[String] = None,

                         /** Schema version for validation */
                         schemaVersion: Option[String] = None,

                         /** Example values or usage notes */
                         examples: Map[String, String] = Map.empty,

                         // ===== Metadata =====
                         /** Custom metadata (for extensibility) */
                         metadata: Map[String, String] = Map.empty,

                         /** Config hash for quick equality checks */
                         hash: Option[String] = None
                       ) {

  // ===== Helper Methods =====

  /**
   * Check if config is active and not expired
   */
  def isActive: Boolean =
    status == "active" && !isExpired

  /**
   * Check if config has expired
   */
  def isExpired: Boolean =
    expiresAt.exists(_.isBefore(Instant.now()))

  /**
   * Check if config is effective now (considering effectiveFrom)
   */
  def isEffective: Boolean =
    effectiveForDate(Instant.now())

  /**
   * Check if config is effective for a given date
   */
  def effectiveForDate(date: Instant): Boolean =
    effectiveFrom.forall(_.isBefore(date)) && !isExpired

  /**
   * Get all feature flags that are enabled
   */
  def enabledFeatures: Map[String, Boolean] =
    features.filter(_._2)

  /**
   * Get all feature flags that are disabled
   */
  def disabledFeatures: Map[String, Boolean] =
    features.filterNot(_._2)

  /**
   * Get setting as Option[String]
   */
  def getSetting(key: String): Option[String] =
    settings.get(key)

  /**
   * Get setting with default value
   */
  def getSettingOrElse(key: String, default: String): String =
    settings.getOrElse(key, default)

  /**
   * Get setting as Int
   */
  def getSettingAsInt(key: String): Option[Int] =
    settings.get(key).flatMap { value =>
      try Some(value.toInt) catch { case _: NumberFormatException => None }
    }

  /**
   * Get setting as Boolean
   */
  def getSettingAsBoolean(key: String): Option[Boolean] =
    settings.get(key).map(_.toLowerCase).collect {
      case "true" | "yes" | "1" => true
      case "false" | "no" | "0" => false
    }

  /**
   * Get setting as Double
   */
  def getSettingAsDouble(key: String): Option[Double] =
    settings.get(key).flatMap { value =>
      try Some(value.toDouble) catch { case _: NumberFormatException => None }
    }

  /**
   * Check if feature is enabled
   */
  def isFeatureEnabled(feature: String): Boolean =
    features.getOrElse(feature, false)

  /**
   * Check if any of the given features are enabled
   */
  def isAnyFeatureEnabled(features: String*): Boolean =
    features.exists(isFeatureEnabled)

  /**
   * Check if all given features are enabled
   */
  def areAllFeaturesEnabled(features: String*): Boolean =
    features.forall(isFeatureEnabled)

  /**
   * Merge with another config (for inheritance)
   */
  def merge(parent: TenantConfig, strategy: String = inheritanceStrategy): TenantConfig = {
    strategy match {
      case "override" =>
        // Parent values are overridden by child
        this.copy(
          settings = parent.settings ++ this.settings,
          features = parent.features ++ this.features,
          tags = parent.tags ++ this.tags,
          metadata = parent.metadata ++ this.metadata
        )
      case "merge" =>
        // Merge with child taking precedence
        this.copy(
          settings = parent.settings ++ this.settings,
          features = parent.features ++ this.features,
          tags = parent.tags ++ this.tags,
          metadata = parent.metadata ++ this.metadata
        )
      case _ => this
    }
  }

  /**
   * Create a new version with updated fields
   */
  def withUpdate(
                  newSettings: Map[String, String] = Map.empty,
                  newFeatures: Map[String, Boolean] = Map.empty,
                  updatedBy: Option[String] = None,
                  reason: Option[String] = None
                ): TenantConfig = {
    this.copy(
      settings = this.settings ++ newSettings,
      features = this.features ++ newFeatures,
      version = this.version + 1,
      updatedAt = Instant.now(),
      updatedBy = updatedBy.orElse(this.updatedBy),
      changeReason = reason.orElse(this.changeReason),
      hash = None // Invalidate hash
    )
  }

  /**
   * Generate a simple hash of the config content
   */
  def computeHash: String = {
    val content = s"$tenantId|$settings|$features|$version"
    Integer.toHexString(content.hashCode)
  }

  /**
   * Validate required settings exist
   */
  def requireSettings(keys: String*): Either[String, Unit] = {
    val missing = keys.filterNot(settings.contains)
    if (missing.isEmpty) Right(())
    else Left(s"Missing required settings: ${missing.mkString(", ")}")
  }

  /**
   * Validate settings against a schema (simplified)
   */
  def validateAgainstSchema(schema: Map[String, String => Boolean]): Either[String, Unit] = {
    schema.collectFirst {
      case (key, validator) if settings.contains(key) && !validator(settings(key)) =>
        Left(s"Setting '$key' failed validation")
    }.getOrElse(Right(()))
  }
}

// ===== Companion Object with Factory Methods =====
object TenantConfig {

  /**
   * Create an empty config for a tenant
   */
  def empty(tenantId: String): TenantConfig =
    TenantConfig(tenantId = tenantId)

  /**
   * Create a config with default settings
   */
  def withDefaults(tenantId: String): TenantConfig =
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
      )
    )

  /**
   * Create a config for development environment
   */
  def development(tenantId: String): TenantConfig =
    TenantConfig(
      tenantId = tenantId,
      environment = Some("development"),
      features = Map(
        "debug.mode" -> true,
        "verbose.logging" -> true
      )
    )

  /**
   * Create a config for production environment
   */
  def production(tenantId: String): TenantConfig =
    TenantConfig(
      tenantId = tenantId,
      environment = Some("production"),
      features = Map(
        "debug.mode" -> false,
        "verbose.logging" -> false
      )
    )

  /**
   * Create a config that expires after a duration
   */
  def withExpiry(
                  tenantId: String,
                  expiresInSeconds: Long
                ): TenantConfig =
    TenantConfig(
      tenantId = tenantId,
      expiresAt = Some(Instant.now().plusSeconds(expiresInSeconds))
    )

  /**
   * Create a config that inherits from a parent
   */
  def inherited(
                 tenantId: String,
                 parentId: String,
                 strategy: String = "merge"
               ): TenantConfig =
    TenantConfig(
      tenantId = tenantId,
      inheritsFrom = Some(parentId),
      inheritanceStrategy = strategy
    )

  /**
   * Create a config for canary testing
   */
  def canary(
              tenantId: String,
              percentage: Int
            ): TenantConfig =
    TenantConfig(
      tenantId = tenantId,
      rolloutPercentage = Some(percentage),
      status = "experimental"
    )
}

// ===== Type aliases for better readability =====
object TenantConfigTypes {
  type SettingKey = String
  type SettingValue = String
  type FeatureKey = String
  type FeatureValue = Boolean
  type ConfigCategory = String
  type ConfigTag = String
}