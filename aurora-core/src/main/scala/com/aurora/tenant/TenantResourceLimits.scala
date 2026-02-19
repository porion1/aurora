package com.aurora.tenant

import java.time.Instant
import scala.concurrent.duration.FiniteDuration

/**
 * Represents a resource limit for a tenant
 */
enum LimitType:
  case Soft  // Can burst, but gets warnings
  case Hard  // Strict enforcement, requests rejected

enum ResourceType:
  case CPU
  case Memory
  case APIRequests
  case ConcurrentRequests

  def unit: String = this match
    case CPU => "cores"
    case Memory => "MB"
    case APIRequests => "requests"
    case ConcurrentRequests => "connections"

/**
 * Configuration for a single resource limit
 */
case class ResourceLimit(
                          value: Double,
                          limitType: LimitType,
                          windowSeconds: Option[Int] = None, // For rate-based limits (e.g., per minute)
                          description: Option[String] = None
                        ):
  require(value > 0, "Limit value must be positive")
  require(windowSeconds.forall(_ > 0), "Window must be positive if defined")

/**
 * Complete resource limits configuration for a tenant
 */
case class TenantResourceLimits(
                                 tenantId: String,
                                 limits: Map[ResourceType, ResourceLimit] = Map.empty,
                                 updatedAt: Instant = Instant.now(),
                                 updatedBy: Option[String] = None,
                                 version: Long = 1L
                               ):
  def getLimit(resource: ResourceType): Option[ResourceLimit] = limits.get(resource)

  def isHardLimit(resource: ResourceType): Boolean =
    limits.get(resource).exists(_.limitType == LimitType.Hard)

  def isSoftLimit(resource: ResourceType): Boolean =
    limits.get(resource).exists(_.limitType == LimitType.Soft)

  def withLimit(resource: ResourceType, limit: ResourceLimit): TenantResourceLimits =
    copy(
      limits = limits + (resource -> limit),
      updatedAt = Instant.now(),
      version = version + 1
    )

  def withoutLimit(resource: ResourceType): TenantResourceLimits =
    copy(
      limits = limits - resource,
      updatedAt = Instant.now(),
      version = version + 1
    )

/**
 * Default limits for different tenant tiers
 */
object DefaultLimits:
  val freeTier = TenantResourceLimits(
    tenantId = "default",
    limits = Map(
      ResourceType.CPU -> ResourceLimit(0.5, LimitType.Hard, description = Some("Max 0.5 cores")),
      ResourceType.Memory -> ResourceLimit(512, LimitType.Hard, description = Some("Max 512MB RAM")),
      ResourceType.APIRequests -> ResourceLimit(100, LimitType.Hard, Some(60), description = Some("100 requests per minute")),
      ResourceType.ConcurrentRequests -> ResourceLimit(10, LimitType.Soft, description = Some("Max 10 concurrent requests"))
    )
  )

  val proTier = TenantResourceLimits(
    tenantId = "default",
    limits = Map(
      ResourceType.CPU -> ResourceLimit(2.0, LimitType.Hard, description = Some("Max 2 cores")),
      ResourceType.Memory -> ResourceLimit(2048, LimitType.Hard, description = Some("Max 2GB RAM")),
      ResourceType.APIRequests -> ResourceLimit(1000, LimitType.Hard, Some(60), description = Some("1000 requests per minute")),
      ResourceType.ConcurrentRequests -> ResourceLimit(50, LimitType.Soft, description = Some("Max 50 concurrent requests"))
    )
  )

  val enterpriseTier = TenantResourceLimits(
    tenantId = "default",
    limits = Map(
      ResourceType.CPU -> ResourceLimit(8.0, LimitType.Soft, description = Some("Up to 8 cores, burstable")),
      ResourceType.Memory -> ResourceLimit(8192, LimitType.Soft, description = Some("Up to 8GB RAM, burstable")),
      ResourceType.APIRequests -> ResourceLimit(10000, LimitType.Hard, Some(60), description = Some("10000 requests per minute")),
      ResourceType.ConcurrentRequests -> ResourceLimit(200, LimitType.Soft, description = Some("Max 200 concurrent requests"))
    )
  )