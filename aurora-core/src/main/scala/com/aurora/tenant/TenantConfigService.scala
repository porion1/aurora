package com.aurora.tenant

import scala.util.Try
import scala.jdk.CollectionConverters.*
import com.mongodb.client.model.ReplaceOptions
import com.mongodb.client.MongoCollection
import org.bson.Document
import java.time.Instant
import com.aurora.infrastructure.TenantDatabaseManager

/**
 * Service to manage per-tenant configuration and feature toggles.
 * Works with shared or dedicated DB depending on tenant type.
 */
object TenantConfigService {

  // ------------------------------
  // Mongo collection for tenant configs
  // ------------------------------
  private def collection(tenantId: String): MongoCollection[Document] =
    TenantDatabaseManager.getDatabase(tenantId).getCollection("tenant_configs")

  // ------------------------------
  // Fetch tenant config
  // ------------------------------
  def getConfig(tenantId: String): Try[TenantConfig] = Try {
    val doc = collection(tenantId).find(new Document("tenantId", tenantId)).first()
    if (doc == null) TenantConfig(tenantId)
    else TenantConfig(
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
  // Update tenant config atomically
  // ------------------------------
  def updateConfig(config: TenantConfig): Try[Boolean] = Try {
    val doc = new Document()
      .append("tenantId", config.tenantId)
      .append("settings", new Document(config.settings.asJava))
      .append("features", new Document(config.features.asJava))
      .append("updatedAt", java.util.Date.from(Instant.now()))
      .append("version", config.version + 1)

    val result = collection(config.tenantId)
      .replaceOne(
        new Document("tenantId", config.tenantId),
        doc,
        new ReplaceOptions().upsert(true)
      )

    result.getModifiedCount + (if (result.getUpsertedId != null) 1 else 0) > 0
  }

  // ------------------------------
  // Feature toggles
  // ------------------------------
  def enableFeature(feature: String, tenantId: String): Try[Boolean] = Try {
    val cfg = getConfig(tenantId).get
    val updated = cfg.copy(features = cfg.features + (feature -> true))
    updateConfig(updated).get
  }

  def disableFeature(feature: String, tenantId: String): Try[Boolean] = Try {
    val cfg = getConfig(tenantId).get
    val updated = cfg.copy(features = cfg.features + (feature -> false))
    updateConfig(updated).get
  }

  def isFeatureEnabled(feature: String, tenantId: String): Boolean =
    getConfig(tenantId).toOption.exists(_.features.getOrElse(feature, false))
}
