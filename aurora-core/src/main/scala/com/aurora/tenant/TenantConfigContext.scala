package com.aurora.tenant

/**
 * Thread-local context for per-tenant configuration.
 * Ensures all operations are scoped to the current tenant.
 */
object TenantConfigContext {

  // Thread-local storage for current tenant ID
  private val threadLocalTenantId = new ThreadLocal[String]()

  /** Set the current tenant context */
  def setTenant(tenantId: String): Unit =
    threadLocalTenantId.set(tenantId)

  /** Get the current tenant ID, if any */
  def getTenant: Option[String] =
    Option(threadLocalTenantId.get())

  /** Clear the tenant context from the current thread */
  def clear(): Unit =
    threadLocalTenantId.remove()

  /**
   * Execute a block of code within a specific tenant context.
   * Automatically restores previous tenant after execution.
   */
  def withTenant[T](tenantId: String)(block: => T): T = {
    val previous = getTenant
    try {
      setTenant(tenantId)
      block
    } finally {
      previous match {
        case Some(id) => setTenant(id)
        case None     => clear()
      }
    }
  }
}
