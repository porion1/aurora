package com.aurora.tenant

import com.aurora.infrastructure.{TenantDatabaseManager}
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.{Filters, IndexOptions, Indexes, Updates, FindOneAndUpdateOptions}
import com.mongodb.client.model.ReturnDocument
import org.bson.Document
import org.bson.conversions.Bson

import scala.jdk.CollectionConverters.*
import scala.util.{Try, Success, Failure}
import java.time.Instant
import java.util.UUID

/**
 * TenantService manages tenants with tenant context
 */
object TenantService {

  // Simple logging
  private def logInfo(msg: String): Unit = println(s"[INFO] [TenantService] $msg")
  private def logWarn(msg: String): Unit = println(s"[WARN] [TenantService] $msg")
  private def logError(msg: String): Unit = println(s"[ERROR] [TenantService] $msg")

  private val COLLECTION_NAME = "tenants"

  // =====================================================
  // Simplified Tenant case class for this service
  // =====================================================
  case class Tenant(
                     id: String,
                     tenantId: String,
                     name: String,
                     createdAt: Instant,
                     updatedAt: Instant,
                     isActive: Boolean
                   )

  object Tenant {
    def create(tenantId: String, name: String): Tenant = {
      val now = Instant.now()
      Tenant(
        id = UUID.randomUUID().toString,
        tenantId = tenantId,
        name = name,
        createdAt = now,
        updatedAt = now,
        isActive = true
      )
    }
  }

  // =====================================================
  // Simple TenantRequirements case class
  // =====================================================
  case class TenantRequirements(
                                 enterprise: Boolean = false,
                                 compliance: Boolean = false,
                                 customDomain: Boolean = false,
                                 expectedUsers: Int = 0
                               )

  // =====================================================
  // COLLECTION ACCESS
  // =====================================================

  private def collectionFromContext(): MongoCollection[Document] = {
    val tenantId = TenantContext.getCurrentTenantId
    val dedicated = TenantContext.isDedicated
    TenantDatabaseManager.getDatabase(tenantId, dedicated).getCollection(COLLECTION_NAME)
  }

  private def collection(tenantId: String, dedicated: Boolean): MongoCollection[Document] = {
    TenantDatabaseManager.getDatabase(tenantId, dedicated).getCollection(COLLECTION_NAME)
  }

  // =====================================================
  // INDEX INITIALIZATION
  // =====================================================

  def initialize(tenantId: String = "system", dedicated: Boolean = false): Try[Unit] = Try {
    logInfo(s"Initializing indexes for tenant '$tenantId' (dedicated=$dedicated)")

    val indexes = List(
      (Indexes.ascending("id"), new IndexOptions().unique(true).name("idx_id_unique")),
      (Indexes.ascending("tenantId"), new IndexOptions().name("idx_tenantId")),
      (Indexes.ascending("name"), new IndexOptions().name("idx_name")),
      (Indexes.ascending("isActive", "tenantId"), new IndexOptions().name("idx_active_tenant")),
      (Indexes.ascending("createdAt"), new IndexOptions().name("idx_createdAt"))
    )

    indexes.foreach { case (keys, options) =>
      ensureIndex(keys, options, tenantId, dedicated)
    }

    logInfo(s"Index initialization completed for tenant '$tenantId'")
  }

  private def ensureIndex(
                           keys: Bson,
                           options: IndexOptions,
                           tenantId: String,
                           dedicated: Boolean
                         ): Unit = {
    Try(collection(tenantId, dedicated).createIndex(keys, options)) match {
      case Success(indexName) =>
        logInfo(s"Index created/verified: $indexName")
      case Failure(ex) if ex.getMessage.contains("already exists") =>
        logInfo(s"Index already exists: ${options.getName}")
      case Failure(ex) =>
        logWarn(s"Index creation warning: ${ex.getMessage}")
    }
  }

  // =====================================================
  // CREATE TENANT
  // =====================================================

  def createTenant(name: String): Try[Tenant] = Try {
    val tenantId = TenantContext.getCurrentTenantId
    val dedicated = TenantContext.isDedicated

    validateTenantName(name)

    val tenant = Tenant.create(tenantId, name)
    collection(tenantId, dedicated).insertOne(toDocument(tenant))
    logInfo(s"Tenant created: ${tenant.id} (dedicated=$dedicated)")
    tenant
  }

  def createTenantWithRequirements(name: String, requirements: TenantRequirements): Try[Tenant] = Try {
    val tenantId = TenantContext.getCurrentTenantId

    validateTenantName(name)

    // Auto-detect if dedicated needed
    val dedicated = TenantDatabaseManager.determineTenantType(
      tenantId,
      requirements.expectedUsers,
      com.aurora.infrastructure.TenantRequirements(
        enterprise = requirements.enterprise,
        compliance = requirements.compliance,
        customDomain = requirements.customDomain,
        expectedUsers = requirements.expectedUsers
      )
    )

    val tenant = Tenant.create(tenantId, name)
    collection(tenantId, dedicated).insertOne(toDocument(tenant))
    logInfo(s"Tenant created with auto-detect: ${tenant.id} (dedicated=$dedicated)")
    tenant
  }

  // =====================================================
  // READ OPERATIONS
  // =====================================================

  def getActiveTenants(limit: Int = 100, offset: Int = 0): Try[List[Tenant]] = Try {
    val tenantId = TenantContext.getCurrentTenantId
    val dedicated = TenantContext.isDedicated

    collection(tenantId, dedicated)
      .find(activeTenantFilter(tenantId))
      .skip(offset)
      .limit(limit)
      .sort(new Document("createdAt", -1))
      .iterator()
      .asScala
      .map(toTenant)
      .toList
  }

  def getTenantById(id: String): Try[Option[Tenant]] = Try {
    val tenantId = TenantContext.getCurrentTenantId
    val dedicated = TenantContext.isDedicated

    Option(collection(tenantId, dedicated)
      .find(activeTenantByIdFilter(tenantId, id))
      .first()
    ).map(toTenant)
  }

  def getTenantByName(name: String): Try[Option[Tenant]] = Try {
    val tenantId = TenantContext.getCurrentTenantId
    val dedicated = TenantContext.isDedicated

    Option(collection(tenantId, dedicated)
      .find(Filters.and(
        Filters.eq("tenantId", tenantId),
        Filters.eq("name", name),
        Filters.eq("isActive", true)
      ))
      .first()
    ).map(toTenant)
  }

  // =====================================================
  // UPDATE OPERATIONS
  // =====================================================

  def updateTenantName(id: String, newName: String): Try[Tenant] = Try {
    val tenantId = TenantContext.getCurrentTenantId
    val dedicated = TenantContext.isDedicated

    validateTenantName(newName)

    // Check if name already exists
    getTenantByName(newName).get.foreach { existing =>
      if (existing.id != id) {
        throw new IllegalStateException(s"Tenant with name '$newName' already exists")
      }
    }

    val filter = activeTenantByIdFilter(tenantId, id)
    val update = Updates.combine(
      Updates.set("name", newName),
      Updates.set("updatedAt", Instant.now().toString)
    )

    val options = new FindOneAndUpdateOptions()
      .returnDocument(ReturnDocument.AFTER)

    val updated = Option(collection(tenantId, dedicated)
      .findOneAndUpdate(filter, update, options))
      .map(toTenant)

    updated.getOrElse {
      throw new IllegalStateException(s"Tenant with id $id not found or inactive")
    }
  }

  def updateTenantStatus(id: String, isActive: Boolean): Try[Tenant] = Try {
    val tenantId = TenantContext.getCurrentTenantId
    val dedicated = TenantContext.isDedicated

    val filter = Filters.and(
      Filters.eq("tenantId", tenantId),
      Filters.eq("id", id)
    )

    val update = Updates.combine(
      Updates.set("isActive", isActive),
      Updates.set("updatedAt", Instant.now().toString)
    )

    val options = new FindOneAndUpdateOptions()
      .returnDocument(ReturnDocument.AFTER)

    val updated = Option(collection(tenantId, dedicated)
      .findOneAndUpdate(filter, update, options))
      .map(toTenant)

    updated.getOrElse {
      throw new IllegalStateException(s"Tenant with id $id not found")
    }
  }

  // =====================================================
  // DEACTIVATE/ACTIVATE TENANT
  // =====================================================

  def deactivateTenant(id: String, reason: Option[String] = None): Try[Tenant] = {
    updateTenantStatus(id, isActive = false)
  }

  def activateTenant(id: String): Try[Tenant] = {
    updateTenantStatus(id, isActive = true)
  }

  // =====================================================
  // MIGRATION
  // =====================================================

  def migrateCurrentTenantToDedicated(): Try[Boolean] = {
    val tenantId = TenantContext.getCurrentTenantId
    logInfo(s"Starting tenant migration for: $tenantId")
    TenantDatabaseManager.migrateToDedicated(tenantId)
  }

  // =====================================================
  // VALIDATION
  // =====================================================

  private def validateTenantName(name: String): Unit = {
    if (name == null || name.trim.isEmpty) {
      throw new IllegalArgumentException("Tenant name cannot be empty")
    }
    if (name.length > 100) {
      throw new IllegalArgumentException("Tenant name cannot exceed 100 characters")
    }
  }

  // =====================================================
  // FILTER HELPERS
  // =====================================================

  private def activeTenantFilter(tenantId: String): Bson =
    Filters.and(
      Filters.eq("tenantId", tenantId),
      Filters.eq("isActive", true)
    )

  private def activeTenantByIdFilter(tenantId: String, id: String): Bson =
    Filters.and(
      Filters.eq("tenantId", tenantId),
      Filters.eq("id", id),
      Filters.eq("isActive", true)
    )

  // =====================================================
  // MAPPERS
  // =====================================================

  private def toDocument(t: Tenant): Document = {
    new Document()
      .append("id", t.id)
      .append("tenantId", t.tenantId)
      .append("name", t.name)
      .append("createdAt", t.createdAt.toString)
      .append("updatedAt", t.updatedAt.toString)
      .append("isActive", t.isActive)
  }

  private def toTenant(doc: Document): Tenant = {
    Tenant(
      id = doc.getString("id"),
      tenantId = doc.getString("tenantId"),
      name = doc.getString("name"),
      createdAt = Instant.parse(doc.getString("createdAt")),
      updatedAt = Instant.parse(doc.getString("updatedAt")),
      isActive = doc.getBoolean("isActive")
    )
  }
}