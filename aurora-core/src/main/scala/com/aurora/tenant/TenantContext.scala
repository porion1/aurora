package com.aurora.tenant

import com.aurora.infrastructure.TenantDatabaseManager

/**
 * TenantContext as a singleton object - manages tenant context per thread
 */
object TenantContext {

  // Simple logging
  private def debug(msg: String): Unit = println(s"[DEBUG] [TenantContext] $msg")

  // ThreadLocal to store tenant context per request/thread
  private val currentTenant = new ThreadLocal[TenantInfo]()

  case class TenantInfo(
                         tenantId: String,
                         name: Option[String] = None,
                         dedicated: Boolean = false,
                         features: Set[String] = Set.empty
                       )

  // =========================
  // Set tenant context
  // =========================
  def setContext(tenantId: String, name: Option[String] = None): Unit = {
    val dedicated = TenantDatabaseManager.getTenantType(tenantId).getOrElse(false)
    val info = TenantInfo(tenantId, name, dedicated)
    currentTenant.set(info)
    debug(s"Tenant context set: $tenantId (dedicated=$dedicated)")
  }

  // =========================
  // Clear tenant context
  // =========================
  def clearContext(): Unit = {
    currentTenant.remove()
    debug("Tenant context cleared")
  }

  // =========================
  // Get current tenant ID
  // =========================
  def getCurrentTenantId: String = {
    Option(currentTenant.get()) match {
      case Some(info) => info.tenantId
      case None => throw new IllegalStateException("No tenant context set. Call setContext() first.")
    }
  }

  // =========================
  // Check if tenant uses dedicated DB
  // =========================
  def isDedicated: Boolean = {
    Option(currentTenant.get()) match {
      case Some(info) => info.dedicated
      case None => false
    }
  }

  // =========================
  // Get full tenant info
  // =========================
  def getCurrentTenantInfo: Option[TenantInfo] = {
    Option(currentTenant.get())
  }

  // =========================
  // Execute with tenant context
  // =========================
  def withTenant[T](tenantId: String, name: Option[String] = None)(block: => T): T = {
    val previous = Option(currentTenant.get())
    try {
      setContext(tenantId, name)
      block
    } finally {
      previous match {
        case Some(info) => setContext(info.tenantId, info.name)
        case None => clearContext()
      }
    }
  }
}