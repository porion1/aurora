package com.aurora.tenant

import java.time.Instant
import java.util.UUID
import scala.util.Try
import scala.util.boundary, boundary.break

/**
 * Represents a tenant in the system with comprehensive metadata
 * for multi-tenant isolation and management.
 */
case class Tenant(
                   id: String,
                   tenantId: String,
                   name: String,
                   createdAt: Instant,
                   updatedAt: Instant,
                   isActive: Boolean,
                   tier: TenantTier = TenantTier.BASIC,
                   status: TenantStatus = TenantStatus.ACTIVE,
                   settings: TenantSettings = TenantSettings(),
                   features: Set[String] = Set.empty,
                   resourceLimits: ResourceLimits = ResourceLimits(),
                   metadata: Map[String, String] = Map.empty,
                   databaseConfig: DatabaseConfig = DatabaseConfig(),
                   contactInfo: Option[ContactInfo] = None,
                   tags: Set[String] = Set.empty,
                   parentTenantId: Option[String] = None
                 )

/**
 * Tenant tier determines features and resource allocation
 */
sealed trait TenantTier {
  def level: Int
  def name: String
  def maxUsers: Int
  def dedicatedDbEligible: Boolean
  def maxStorageGb: Int
  def description: String
  def monthlyPriceUsd: Double
}

object TenantTier {

  case object BASIC extends TenantTier {
    val level = 1
    val name = "BASIC"
    val maxUsers = 100
    val dedicatedDbEligible = false
    val maxStorageGb = 10
    val description = "Basic tier for small teams and startups"
    val monthlyPriceUsd = 0.0

    override def toString = "BASIC"
  }

  case object PROFESSIONAL extends TenantTier {
    val level = 2
    val name = "PROFESSIONAL"
    val maxUsers = 1000
    val dedicatedDbEligible = true
    val maxStorageGb = 100
    val description = "Professional tier for growing businesses"
    val monthlyPriceUsd = 499.0

    override def toString = "PROFESSIONAL"
  }

  case object ENTERPRISE extends TenantTier {
    val level = 3
    val name = "ENTERPRISE"
    val maxUsers = 10000
    val dedicatedDbEligible = true
    val maxStorageGb = 1000
    val description = "Enterprise tier with dedicated resources and premium support"
    val monthlyPriceUsd = 1999.0

    override def toString = "ENTERPRISE"
  }

  val values: List[TenantTier] = List(BASIC, PROFESSIONAL, ENTERPRISE)

  def fromString(s: String): Option[TenantTier] = s.toUpperCase match {
    case "BASIC" => Some(BASIC)
    case "PROFESSIONAL" => Some(PROFESSIONAL)
    case "ENTERPRISE" => Some(ENTERPRISE)
    case _ => None
  }

  def fromLevel(level: Int): Option[TenantTier] = level match {
    case 1 => Some(BASIC)
    case 2 => Some(PROFESSIONAL)
    case 3 => Some(ENTERPRISE)
    case _ => None
  }
}

/**
 * Tenant status lifecycle
 */
sealed trait TenantStatus {
  def name: String
  def canTransitionTo(newStatus: TenantStatus): Boolean
  def isOperational: Boolean
  def isDeletable: Boolean
  def requiresApproval: Boolean
}

object TenantStatus {

  case object PENDING extends TenantStatus {
    val name = "PENDING"
    def canTransitionTo(newStatus: TenantStatus): Boolean =
      newStatus == ACTIVE || newStatus == SUSPENDED || newStatus == REJECTED
    def isOperational: Boolean = false
    def isDeletable: Boolean = true
    def requiresApproval: Boolean = true

    override def toString = "PENDING"
  }

  case object ACTIVE extends TenantStatus {
    val name = "ACTIVE"
    def canTransitionTo(newStatus: TenantStatus): Boolean =
      newStatus == SUSPENDED || newStatus == DISABLED || newStatus == DELETED
    def isOperational: Boolean = true
    def isDeletable: Boolean = false
    def requiresApproval: Boolean = false

    override def toString = "ACTIVE"
  }

  case object SUSPENDED extends TenantStatus {
    val name = "SUSPENDED"
    def canTransitionTo(newStatus: TenantStatus): Boolean =
      newStatus == ACTIVE || newStatus == DISABLED
    def isOperational: Boolean = false
    def isDeletable: Boolean = false
    def requiresApproval: Boolean = true

    override def toString = "SUSPENDED"
  }

  case object DISABLED extends TenantStatus {
    val name = "DISABLED"
    def canTransitionTo(newStatus: TenantStatus): Boolean =
      newStatus == DELETED || newStatus == ARCHIVED
    def isOperational: Boolean = false
    def isDeletable: Boolean = true
    def requiresApproval: Boolean = true

    override def toString = "DISABLED"
  }

  case object REJECTED extends TenantStatus {
    val name = "REJECTED"
    def canTransitionTo(newStatus: TenantStatus): Boolean =
      newStatus == PENDING || newStatus == DELETED
    def isOperational: Boolean = false
    def isDeletable: Boolean = true
    def requiresApproval: Boolean = false

    override def toString = "REJECTED"
  }

  case object DELETED extends TenantStatus {
    val name = "DELETED"
    def canTransitionTo(newStatus: TenantStatus): Boolean = false
    def isOperational: Boolean = false
    def isDeletable: Boolean = false
    def requiresApproval: Boolean = false

    override def toString = "DELETED"
  }

  case object ARCHIVED extends TenantStatus {
    val name = "ARCHIVED"
    def canTransitionTo(newStatus: TenantStatus): Boolean = false
    def isOperational: Boolean = false
    def isDeletable: Boolean = false
    def requiresApproval: Boolean = false

    override def toString = "ARCHIVED"
  }

  val values: List[TenantStatus] = List(
    PENDING, ACTIVE, SUSPENDED, DISABLED, REJECTED, DELETED, ARCHIVED
  )

  def fromString(s: String): Option[TenantStatus] = s.toUpperCase match {
    case "PENDING" => Some(PENDING)
    case "ACTIVE" => Some(ACTIVE)
    case "SUSPENDED" => Some(SUSPENDED)
    case "DISABLED" => Some(DISABLED)
    case "REJECTED" => Some(REJECTED)
    case "DELETED" => Some(DELETED)
    case "ARCHIVED" => Some(ARCHIVED)
    case _ => None
  }
}

/**
 * Tenant settings and configuration
 */
case class TenantSettings(
                           timezone: String = "UTC",
                           locale: String = "en-US",
                           dateFormat: String = "yyyy-MM-dd",
                           timeFormat: String = "HH:mm:ss",
                           currency: String = "USD",
                           theme: Option[String] = None,
                           logoUrl: Option[String] = None,
                           faviconUrl: Option[String] = None,
                           primaryColor: Option[String] = None,
                           secondaryColor: Option[String] = None,
                           customDomain: Option[String] = None,
                           allowedIpRanges: List[String] = List.empty,
                           blockedIpRanges: List[String] = List.empty,
                           sessionTimeoutMinutes: Int = 30,
                           maxConcurrentSessions: Int = 5,
                           mfaRequired: Boolean = false,
                           mfaEnforced: Boolean = false,
                           ssoEnabled: Boolean = false,
                           ssoProvider: Option[String] = None,
                           ssoConfig: Map[String, String] = Map.empty,
                           webhookUrls: Map[String, String] = Map.empty,
                           webhookSecrets: Map[String, String] = Map.empty,
                           rateLimitingEnabled: Boolean = true,
                           auditLoggingEnabled: Boolean = true
                         ) {
  def isCustomDomainConfigured: Boolean = customDomain.isDefined
  def isSsoConfigured: Boolean = ssoEnabled && ssoProvider.isDefined
}

/**
 * Resource limits and quotas
 */
case class ResourceLimits(
                           maxUsers: Int = 100,
                           maxProjects: Int = 10,
                           maxStorageGb: Int = 10,
                           maxBandwidthGbPerMonth: Int = 100,
                           maxApiCallsPerDay: Int = 10000,
                           maxApiCallsPerMonth: Int = 300000,
                           maxConcurrentRequests: Int = 50,
                           maxDatabases: Int = 1,
                           maxCollections: Int = 10,
                           maxDocuments: Long = 1000000L,
                           maxIndexes: Int = 20,
                           maxFileSizeMb: Int = 100,
                           maxTeamMembers: Int = 10,
                           maxIntegrations: Int = 5,
                           features: Map[String, Boolean] = Map.empty,
                           customQuotas: Map[String, Long] = Map.empty
                         ) {
  def canAddUser(currentUsers: Int): Boolean = currentUsers < maxUsers
  def canAddProject(currentProjects: Int): Boolean = currentProjects < maxProjects
  def hasStorageAvailable(currentStorageGb: Double): Boolean = currentStorageGb < maxStorageGb
  def hasBandwidthAvailable(currentBandwidthGb: Double): Boolean = currentBandwidthGb < maxBandwidthGbPerMonth
  def hasApiCallsAvailable(todayCalls: Int): Boolean = todayCalls < maxApiCallsPerDay
}

/**
 * Database configuration for tenant
 */
case class DatabaseConfig(
                           databaseType: DatabaseType = DatabaseType.SHARED,
                           databaseName: Option[String] = None,
                           connectionString: Option[String] = None,
                           sharded: Boolean = false,
                           replicas: Int = 1,
                           backupEnabled: Boolean = true,
                           backupSchedule: Option[String] = None,
                           backupRetentionDays: Int = 30,
                           encryptionAtRest: Boolean = false,
                           encryptionInTransit: Boolean = true,
                           readPreference: String = "primary",
                           writeConcern: String = "majority",
                           maxPoolSize: Int = 100,
                           minPoolSize: Int = 10,
                           maxIdleTimeMs: Int = 600000,
                           region: Option[String] = None,
                           clusterId: Option[String] = None
                         ) {
  def isDedicated: Boolean = databaseType == DatabaseType.DEDICATED
  def isShared: Boolean = databaseType == DatabaseType.SHARED
  def isIsolated: Boolean = databaseType == DatabaseType.ISOLATED
}

sealed trait DatabaseType {
  def name: String
  def description: String
  def isScalable: Boolean
  def costMultiplier: Double
}

object DatabaseType {

  case object SHARED extends DatabaseType {
    val name = "SHARED"
    val description = "Shared database with other small tenants"
    val isScalable = false
    val costMultiplier = 1.0
    override def toString = "SHARED"
  }

  case object DEDICATED extends DatabaseType {
    val name = "DEDICATED"
    val description = "Dedicated database instance"
    val isScalable = true
    val costMultiplier = 3.0
    override def toString = "DEDICATED"
  }

  case object ISOLATED extends DatabaseType {
    val name = "ISOLATED"
    val description = "Isolated cluster with dedicated resources"
    val isScalable = true
    val costMultiplier = 5.0
    override def toString = "ISOLATED"
  }

  val values: List[DatabaseType] = List(SHARED, DEDICATED, ISOLATED)

  def fromString(s: String): Option[DatabaseType] = s.toUpperCase match {
    case "SHARED" => Some(SHARED)
    case "DEDICATED" => Some(DEDICATED)
    case "ISOLATED" => Some(ISOLATED)
    case _ => None
  }
}

/**
 * Contact information
 */
case class ContactInfo(
                        email: String,
                        phone: Option[String] = None,
                        address: Option[String] = None,
                        address2: Option[String] = None,
                        city: Option[String] = None,
                        state: Option[String] = None,
                        country: Option[String] = None,
                        postalCode: Option[String] = None,
                        website: Option[String] = None,
                        primaryContactName: Option[String] = None,
                        primaryContactEmail: Option[String] = None,
                        primaryContactPhone: Option[String] = None,
                        technicalContactName: Option[String] = None,
                        technicalContactEmail: Option[String] = None,
                        technicalContactPhone: Option[String] = None,
                        billingContactName: Option[String] = None,
                        billingContactEmail: Option[String] = None,
                        billingContactPhone: Option[String] = None,
                        emergencyContactName: Option[String] = None,
                        emergencyContactEmail: Option[String] = None,
                        emergencyContactPhone: Option[String] = None
                      )

/**
 * Tenant creation request
 */
case class CreateTenantRequest(
                                tenantId: String,
                                name: String,
                                tier: TenantTier = TenantTier.BASIC,
                                settings: TenantSettings = TenantSettings(),
                                features: Set[String] = Set.empty,
                                contactInfo: Option[ContactInfo] = None,
                                tags: Set[String] = Set.empty,
                                metadata: Map[String, String] = Map.empty,
                                requestedDatabaseType: Option[DatabaseType] = None,
                                referrer: Option[String] = None,
                                campaign: Option[String] = None
                              ) {

  def validate: Either[String, CreateTenantRequest] = boundary {
    if (tenantId == null || tenantId.trim.isEmpty)
      break(Left("tenantId cannot be empty"))
    else if (name == null || name.trim.isEmpty)
      break(Left("name cannot be empty"))
    else if (name.length > 100)
      break(Left("name cannot exceed 100 characters"))
    else if (!tenantId.matches("^[a-zA-Z0-9_-]+$"))
      break(Left("tenantId contains invalid characters. Allowed: letters, numbers, underscore, hyphen"))
    else if (tenantId.length < 3)
      break(Left("tenantId must be at least 3 characters"))
    else if (tenantId.length > 50)
      break(Left("tenantId cannot exceed 50 characters"))
    else
      Right(this)
  }

  def needsApproval: Boolean = tier == TenantTier.ENTERPRISE || requestedDatabaseType.contains(DatabaseType.ISOLATED)
}

/**
 * Tenant update request
 */
case class UpdateTenantRequest(
                                name: Option[String] = None,
                                settings: Option[TenantSettings] = None,
                                features: Option[Set[String]] = None,
                                contactInfo: Option[ContactInfo] = None,
                                tags: Option[Set[String]] = None,
                                metadata: Option[Map[String, String]] = None,
                                resourceLimits: Option[ResourceLimits] = None,
                                status: Option[TenantStatus] = None
                              ) {

  def isEmpty: Boolean =
    name.isEmpty && settings.isEmpty && features.isEmpty &&
      contactInfo.isEmpty && tags.isEmpty && metadata.isEmpty &&
      resourceLimits.isEmpty && status.isEmpty

  def validate: Either[String, UpdateTenantRequest] = boundary {
    name.foreach { n =>
      if (n.trim.isEmpty) break(Left("name cannot be empty"))
      if (n.length > 100) break(Left("name cannot exceed 100 characters"))
    }
    Right(this)
  }
}

/**
 * Tenant response for API (PII-safe)
 */
case class TenantResponse(
                           id: String,
                           tenantId: String,
                           name: String,
                           createdAt: Instant,
                           updatedAt: Instant,
                           isActive: Boolean,
                           tier: String,
                           status: String,
                           features: Set[String],
                           databaseType: String,
                           contactEmail: Option[String],
                           tags: Set[String],
                           resourceLimits: ResourceLimitsSummary,
                           settings: TenantSettingsSummary
                         )

case class ResourceLimitsSummary(
                                  maxUsers: Int,
                                  maxProjects: Int,
                                  maxStorageGb: Int,
                                  maxApiCallsPerDay: Int
                                )

case class TenantSettingsSummary(
                                  timezone: String,
                                  locale: String,
                                  customDomain: Option[String],
                                  mfaRequired: Boolean,
                                  ssoEnabled: Boolean
                                )

object TenantResponse {
  def fromTenant(tenant: Tenant): TenantResponse = TenantResponse(
    id = tenant.id,
    tenantId = tenant.tenantId,
    name = tenant.name,
    createdAt = tenant.createdAt,
    updatedAt = tenant.updatedAt,
    isActive = tenant.isActive,
    tier = tenant.tier.toString,
    status = tenant.status.toString,
    features = tenant.features,
    databaseType = tenant.databaseConfig.databaseType.toString,
    contactEmail = tenant.contactInfo.map(_.email),
    tags = tenant.tags,
    resourceLimits = ResourceLimitsSummary(
      maxUsers = tenant.resourceLimits.maxUsers,
      maxProjects = tenant.resourceLimits.maxProjects,
      maxStorageGb = tenant.resourceLimits.maxStorageGb,
      maxApiCallsPerDay = tenant.resourceLimits.maxApiCallsPerDay
    ),
    settings = TenantSettingsSummary(
      timezone = tenant.settings.timezone,
      locale = tenant.settings.locale,
      customDomain = tenant.settings.customDomain,
      mfaRequired = tenant.settings.mfaRequired,
      ssoEnabled = tenant.settings.ssoEnabled
    )
  )
}

/**
 * Tenant events for audit logging
 */
sealed trait TenantEvent {
  def tenantId: String
  def timestamp: Instant
  def userId: String
  def eventType: String
  def description: String
}

object TenantEvent {

  case class TenantCreated(
                            tenantId: String,
                            timestamp: Instant,
                            userId: String,
                            name: String,
                            tier: TenantTier,
                            createdBy: String
                          ) extends TenantEvent {
    val eventType = "TENANT_CREATED"
    val description = s"Tenant '$name' created with tier ${tier.name}"
  }

  case class TenantUpdated(
                            tenantId: String,
                            timestamp: Instant,
                            userId: String,
                            changes: Map[String, (Any, Any)]
                          ) extends TenantEvent {
    val eventType = "TENANT_UPDATED"
    val description = s"Tenant updated: ${changes.keys.mkString(", ")}"
  }

  case class TenantStatusChanged(
                                  tenantId: String,
                                  timestamp: Instant,
                                  userId: String,
                                  oldStatus: TenantStatus,
                                  newStatus: TenantStatus,
                                  reason: Option[String]
                                ) extends TenantEvent {
    val eventType = "TENANT_STATUS_CHANGED"
    val description = s"Status changed from ${oldStatus.name} to ${newStatus.name}" +
      reason.map(r => s": $r").getOrElse("")
  }

  case class TenantTierChanged(
                                tenantId: String,
                                timestamp: Instant,
                                userId: String,
                                oldTier: TenantTier,
                                newTier: TenantTier,
                                reason: Option[String]
                              ) extends TenantEvent {
    val eventType = "TENANT_TIER_CHANGED"
    val description = s"Tier changed from ${oldTier.name} to ${newTier.name}"
  }

  case class TenantDatabaseChanged(
                                    tenantId: String,
                                    timestamp: Instant,
                                    userId: String,
                                    oldDbType: DatabaseType,
                                    newDbType: DatabaseType,
                                    oldDbName: Option[String],
                                    newDbName: Option[String]
                                  ) extends TenantEvent {
    val eventType = "TENANT_DATABASE_CHANGED"
    val description = s"Database changed from ${oldDbType.name} to ${newDbType.name}"
  }

  case class TenantDeleted(
                            tenantId: String,
                            timestamp: Instant,
                            userId: String,
                            reason: Option[String]
                          ) extends TenantEvent {
    val eventType = "TENANT_DELETED"
    val description = s"Tenant deleted" + reason.map(r => s": $r").getOrElse("")
  }

  case class TenantFeatureToggled(
                                   tenantId: String,
                                   timestamp: Instant,
                                   userId: String,
                                   feature: String,
                                   enabled: Boolean
                                 ) extends TenantEvent {
    val eventType = "TENANT_FEATURE_TOGGLED"
    val description = s"Feature '$feature' ${if (enabled) "enabled" else "disabled"}"
  }
}

/**
 * Companion object with factory methods and utilities
 */
object Tenant {

  def create(request: CreateTenantRequest, createdBy: String = "system"): Tenant = {
    val now = Instant.now()
    val tier = request.tier

    val dbType = request.requestedDatabaseType match {
      case Some(requested) if tier.dedicatedDbEligible => requested
      case Some(_) => DatabaseType.SHARED
      case None =>
        if (tier == TenantTier.ENTERPRISE) DatabaseType.DEDICATED
        else DatabaseType.SHARED
    }

    val baseMetadata = Map("createdBy" -> createdBy) ++
      request.referrer.map("referrer" -> _).toMap ++
      request.campaign.map("campaign" -> _).toMap

    Tenant(
      id = UUID.randomUUID().toString,
      tenantId = request.tenantId,
      name = request.name,
      createdAt = now,
      updatedAt = now,
      isActive = false,
      tier = tier,
      status = if (request.needsApproval) TenantStatus.PENDING else TenantStatus.ACTIVE,
      settings = request.settings,
      features = request.features,
      resourceLimits = ResourceLimits(
        maxUsers = tier.maxUsers,
        maxStorageGb = tier.maxStorageGb,
        maxProjects = if (tier == TenantTier.ENTERPRISE) 100 else 10,
        maxApiCallsPerDay = if (tier == TenantTier.ENTERPRISE) 100000 else 10000
      ),
      metadata = baseMetadata,
      databaseConfig = DatabaseConfig(
        databaseType = dbType,
        sharded = tier == TenantTier.ENTERPRISE,
        replicas = if (tier == TenantTier.ENTERPRISE) 3 else 1,
        backupEnabled = true,
        encryptionAtRest = tier == TenantTier.ENTERPRISE,
        backupRetentionDays = if (tier == TenantTier.ENTERPRISE) 90 else 30
      ),
      contactInfo = request.contactInfo,
      tags = request.tags + tier.name.toLowerCase,
      parentTenantId = None
    )
  }

  def activate(tenant: Tenant, activatedBy: String = "system"): Either[String, Tenant] = {
    if (tenant.status != TenantStatus.PENDING && tenant.status != TenantStatus.SUSPENDED) {
      Left(s"Cannot activate tenant in ${tenant.status.name} state")
    } else {
      val newMetadata = tenant.metadata ++ Map(
        "activatedBy" -> activatedBy,
        "activatedAt" -> Instant.now().toString
      )

      Right(
        tenant.copy(
          status = TenantStatus.ACTIVE,
          isActive = true,
          updatedAt = Instant.now(),
          metadata = newMetadata
        )
      )
    }
  }

  def suspend(tenant: Tenant, reason: String, suspendedBy: String = "system"): Either[String, Tenant] = {
    if (tenant.status != TenantStatus.ACTIVE) {
      Left(s"Cannot suspend tenant in ${tenant.status.name} state")
    } else {
      val newMetadata = tenant.metadata ++ Map(
        "suspendedBy" -> suspendedBy,
        "suspendedAt" -> Instant.now().toString,
        "suspensionReason" -> reason
      )

      Right(
        tenant.copy(
          status = TenantStatus.SUSPENDED,
          isActive = false,
          updatedAt = Instant.now(),
          metadata = newMetadata
        )
      )
    }
  }

  def disable(tenant: Tenant, reason: String, disabledBy: String = "system"): Either[String, Tenant] = {
    if (!Set(TenantStatus.ACTIVE, TenantStatus.SUSPENDED).contains(tenant.status)) {
      Left(s"Cannot disable tenant in ${tenant.status.name} state")
    } else {
      val newMetadata = tenant.metadata ++ Map(
        "disabledBy" -> disabledBy,
        "disabledAt" -> Instant.now().toString,
        "disableReason" -> reason
      )

      Right(
        tenant.copy(
          status = TenantStatus.DISABLED,
          isActive = false,
          updatedAt = Instant.now(),
          metadata = newMetadata
        )
      )
    }
  }

  def reject(tenant: Tenant, reason: String, rejectedBy: String = "system"): Either[String, Tenant] = {
    if (tenant.status != TenantStatus.PENDING) {
      Left(s"Cannot reject tenant in ${tenant.status.name} state")
    } else {
      val newMetadata = tenant.metadata ++ Map(
        "rejectedBy" -> rejectedBy,
        "rejectedAt" -> Instant.now().toString,
        "rejectionReason" -> reason
      )

      Right(
        tenant.copy(
          status = TenantStatus.REJECTED,
          isActive = false,
          updatedAt = Instant.now(),
          metadata = newMetadata
        )
      )
    }
  }

  def upgradeTier(tenant: Tenant, newTier: TenantTier, upgradedBy: String = "system"): Either[String, Tenant] = {
    if (newTier.level <= tenant.tier.level) {
      Left(s"Can only upgrade to higher tier. Current: ${tenant.tier.name}, Requested: ${newTier.name}")
    } else {
      val newMetadata = tenant.metadata ++ Map(
        "upgradedBy" -> upgradedBy,
        "upgradedAt" -> Instant.now().toString,
        "previousTier" -> tenant.tier.name
      )

      Right(
        tenant.copy(
          tier = newTier,
          resourceLimits = ResourceLimits(
            maxUsers = newTier.maxUsers,
            maxStorageGb = newTier.maxStorageGb,
            maxProjects = tenant.resourceLimits.maxProjects,
            maxApiCallsPerDay = if (newTier == TenantTier.ENTERPRISE) 100000 else 10000
          ),
          updatedAt = Instant.now(),
          metadata = newMetadata
        )
      )
    }
  }

  def assignToDedicatedDatabase(tenant: Tenant, databaseName: String, assignedBy: String = "system"): Tenant = {
    val newMetadata = tenant.metadata ++ Map(
      "dbAssignedBy" -> assignedBy,
      "dbAssignedAt" -> Instant.now().toString
    )

    tenant.copy(
      databaseConfig = tenant.databaseConfig.copy(
        databaseType = DatabaseType.DEDICATED,
        databaseName = Some(databaseName)
      ),
      updatedAt = Instant.now(),
      metadata = newMetadata
    )
  }

  def updateSettings(tenant: Tenant, newSettings: TenantSettings): Tenant = {
    tenant.copy(
      settings = newSettings,
      updatedAt = Instant.now()
    )
  }

  def addFeature(tenant: Tenant, feature: String, enabledBy: String = "system"): Tenant = {
    val newMetadata = tenant.metadata ++ Map(
      s"feature.$feature.enabledBy" -> enabledBy,
      s"feature.$feature.enabledAt" -> Instant.now().toString
    )

    tenant.copy(
      features = tenant.features + feature,
      updatedAt = Instant.now(),
      metadata = newMetadata
    )
  }

  def removeFeature(tenant: Tenant, feature: String): Tenant = {
    tenant.copy(
      features = tenant.features - feature,
      updatedAt = Instant.now()
    )
  }

  def addTag(tenant: Tenant, tag: String): Tenant = {
    tenant.copy(
      tags = tenant.tags + tag,
      updatedAt = Instant.now()
    )
  }

  def removeTag(tenant: Tenant, tag: String): Tenant = {
    tenant.copy(
      tags = tenant.tags - tag,
      updatedAt = Instant.now()
    )
  }

  def canPerformAction(tenant: Tenant, action: String): Boolean = {
    action match {
      case "login" => tenant.status == TenantStatus.ACTIVE
      case "api_call" => tenant.status == TenantStatus.ACTIVE
      case "create_project" => tenant.status == TenantStatus.ACTIVE
      case "view_data" => tenant.status == TenantStatus.ACTIVE || tenant.status == TenantStatus.SUSPENDED
      case "modify_settings" => tenant.status == TenantStatus.ACTIVE
      case "delete" => tenant.status.isDeletable
      case _ => false
    }
  }
}