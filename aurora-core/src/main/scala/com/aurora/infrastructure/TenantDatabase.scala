package com.aurora.infrastructure

import com.aurora.config.ApplicationConfig
import com.mongodb.client.{MongoClient, MongoClients, MongoDatabase}
import org.bson.Document

import scala.collection.concurrent.TrieMap

/**
 * TenantDatabase handles multi-tenant database selection.
 *
 * Strategy:
 * - Small tenants → shared database
 * - Enterprise tenants → dedicated database per tenant
 */
object TenantDatabase {

  // ============================================
  // Mongo Client (Singleton)
  // ============================================
  private val client: MongoClient =
    MongoClients.create(
      s"mongodb://${ApplicationConfig.Mongo.host}:${ApplicationConfig.Mongo.port}"
    )

  // ============================================
  // Shared Database
  // ============================================
  private val sharedDb: MongoDatabase =
    client.getDatabase(ApplicationConfig.Mongo.database)

  // ============================================
  // Dedicated DB Cache
  // ============================================
  private val tenantDbCache: TrieMap[String, MongoDatabase] = TrieMap.empty

  // ============================================
  // Get Database for Tenant
  // ============================================
  def getDatabase(tenantId: String, dedicated: Boolean = false): MongoDatabase = {
    if (!dedicated) {
      sharedDb
    } else {
      tenantDbCache.getOrElseUpdate(
        tenantId,
        client.getDatabase(s"${ApplicationConfig.Mongo.database}_$tenantId")
      )
    }
  }

  // ============================================
  // Health Check
  // ============================================
  def ping(tenantId: String, dedicated: Boolean = false): Boolean = {
    try {
      val db = getDatabase(tenantId, dedicated)
      val result = db.runCommand(new Document("ping", 1))
      println(s"MongoDB ping for tenant '$tenantId' (dedicated=$dedicated): $result")
      true
    } catch {
      case e: Exception =>
        println(s"MongoDB ping failed for tenant '$tenantId': ${e.getMessage}")
        false
    }
  }
}
