package com.aurora.infrastructure

import com.aurora.config.ApplicationConfig
import com.mongodb.client.{MongoClient, MongoClients, MongoDatabase}
import org.bson.Document

object MongoDB {

  // ============================================
  // Configuration
  // ============================================

  private val host: String = ApplicationConfig.Mongo.host
  private val port: Int    = ApplicationConfig.Mongo.port
  private val dbName: String = ApplicationConfig.Mongo.database

  private val connectionString: String =
    s"mongodb://$host:$port"

  // ============================================
  // Mongo Client (Singleton)
  // ============================================

  val client: MongoClient =
    MongoClients.create(connectionString)

  // ============================================
  // Default Database (Backward Compatible)
  // ============================================

  val database: MongoDatabase =
    client.getDatabase(dbName)

  // ============================================
  // Health Check
  // ============================================

  def ping(): Boolean = {
    try {
      val result = database.runCommand(new Document("ping", 1))
      println(s"MongoDB ping result: $result")
      true
    } catch {
      case e: Exception =>
        println(s"MongoDB ping failed: ${e.getMessage}")
        false
    }
  }
}
