package com.aurora.tenant

import java.time.Instant

case class TenantConfig(
                         tenantId: String,
                         settings: Map[String, String] = Map.empty,
                         features: Map[String, Boolean] = Map.empty,
                         updatedAt: Instant = Instant.now(),
                         version: Long = 1L
                       )
