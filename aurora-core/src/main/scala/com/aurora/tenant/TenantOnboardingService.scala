package com.aurora.tenant

import com.aurora.infrastructure.TenantDatabaseManager
import scala.util.{Try, Success, Failure}
import java.time.Instant
import scala.concurrent.{Future, ExecutionContext}
import io.circe.syntax.*
import io.circe.generic.auto.*
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.net.URI
import com.aurora.tenant.DefaultLimits

/**
 * Orchestrates automated tenant onboarding including:
 * - Tenant registration
 * - Database provisioning
 * - Configuration setup
 * - Resource limits initialization
 * - Webhook notifications
 */
object TenantOnboardingService {

  private def logInfo(msg: String): Unit = println(s"[INFO] [TenantOnboardingService] $msg")
  private def logError(msg: String): Unit = println(s"[ERROR] [TenantOnboardingService] $msg")

  // Tenant templates for different tiers
  case class TenantTemplate(
                             tier: String,                    // "free", "pro", "enterprise"
                             configTemplate: TenantConfig,
                             limitsTemplate: TenantResourceLimits,
                             features: Map[String, Boolean],
                             settings: Map[String, String]
                           )

  // Pre-defined templates
  val templates: Map[String, TenantTemplate] = Map(
    "free" -> TenantTemplate(
      tier = "free",
      configTemplate = TenantConfig(
        tenantId = "", // Will be filled
        settings = Map(
          "ui.theme" -> "light",
          "items.per.page" -> "20",
          "session.timeout.minutes" -> "30",
          "storage.limit.mb" -> "100"
        ),
        features = Map(
          "advanced.search" -> false,
          "bulk.operations" -> false,
          "audit.logging" -> true,
          "api.access" -> true
        ),
        updatedAt = Instant.now(),
        version = 1L
      ),
      limitsTemplate = DefaultLimits.freeTier,
      features = Map(
        "advanced.search" -> false,
        "bulk.operations" -> false
      ),
      settings = Map(
        "storage.limit.mb" -> "100",
        "rate.limit.per.minute" -> "60"
      )
    ),
    "pro" -> TenantTemplate(
      tier = "pro",
      configTemplate = TenantConfig(
        tenantId = "", // Will be filled
        settings = Map(
          "ui.theme" -> "dark",
          "items.per.page" -> "100",
          "session.timeout.minutes" -> "120",
          "storage.limit.mb" -> "1024"
        ),
        features = Map(
          "advanced.search" -> true,
          "bulk.operations" -> true,
          "audit.logging" -> true,
          "api.access" -> true
        ),
        updatedAt = Instant.now(),
        version = 1L
      ),
      limitsTemplate = DefaultLimits.proTier,
      features = Map(
        "advanced.search" -> true,
        "bulk.operations" -> true
      ),
      settings = Map(
        "storage.limit.mb" -> "1024",
        "rate.limit.per.minute" -> "300"
      )
    ),
    "enterprise" -> TenantTemplate(
      tier = "enterprise",
      configTemplate = TenantConfig(
        tenantId = "", // Will be filled
        settings = Map(
          "ui.theme" -> "dark",
          "items.per.page" -> "500",
          "session.timeout.minutes" -> "480",
          "storage.limit.mb" -> "10240",
          "dedicated.db" -> "true"
        ),
        features = Map(
          "advanced.search" -> true,
          "bulk.operations" -> true,
          "audit.logging" -> true,
          "api.access" -> true,
          "dedicated.db" -> true
        ),
        updatedAt = Instant.now(),
        version = 1L
      ),
      limitsTemplate = DefaultLimits.enterpriseTier,
      features = Map(
        "advanced.search" -> true,
        "bulk.operations" -> true,
        "dedicated.db" -> true
      ),
      settings = Map(
        "storage.limit.mb" -> "10240",
        "rate.limit.per.minute" -> "1000",
        "concurrent.limit" -> "100"
      )
    )
  )

  /**
   * Onboard a new tenant automatically
   * @param name Tenant name
   * @param tier Tenant tier ("free", "pro", "enterprise")
   * @param requirements Additional requirements
   * @param webhookUrl Optional webhook to notify on completion
   */
  def onboardTenant(
                     name: String,
                     tier: String = "free",
                     requirements: TenantService.TenantRequirements = TenantService.TenantRequirements(),
                     webhookUrl: Option[String] = None
                   )(implicit ec: ExecutionContext): Future[OnboardingResult] = {

    val startTime = System.currentTimeMillis()
    logInfo(s"Starting onboarding for tenant '$name' with tier '$tier'")

    // Step 1: Validate inputs
    val validationResult = validateOnboardingRequest(name, tier, requirements)
    if (validationResult.isFailure) {
      return Future.successful(OnboardingResult.failure(validationResult.failed.get.getMessage))
    }

    // Step 2: Create tenant in system context
    // Switch to system context for tenant creation
    val result = TenantContext.withTenantAndDedicated("system", dedicated = false) {

      // Determine if dedicated DB is needed
      val needsDedicated = tier == "enterprise" ||
        requirements.enterprise ||
        requirements.expectedUsers > 1000

      // Create the tenant record
      val tenantResult = TenantService.createTenantWithRequirements(name, requirements)

      tenantResult match {
        case Success(tenant) =>
          logInfo(s"Tenant record created: ${tenant.id}")

          // Step 3: Provision database
          val dbResult = if (needsDedicated) {
            TenantDatabaseManager.provisionDatabase(tenant.tenantId, dedicated = true)
          } else {
            Success(true) // Shared DB already exists
          }

          dbResult match {
            case Success(_) =>
              logInfo(s"Database provisioned for tenant ${tenant.id} (dedicated=$needsDedicated)")

              // Step 4: Initialize configuration with template
              val template = templates.getOrElse(tier, templates("free"))
              val configResult = TenantConfigService.initializeWithTemplate(
                tenant.tenantId,
                template,
                requirements
              )

              configResult match {
                case Success(config) =>
                  logInfo(s"Configuration initialized for tenant ${tenant.id}")

                  // Step 5: Initialize resource limits with template
                  import scala.concurrent.ExecutionContext.Implicits.global
                  val resourceService = new TenantResourceService()
                  val limitsFuture = resourceService.initializeWithTemplate(
                    tenant.tenantId,
                    template,
                    requirements
                  )

                  limitsFuture.map { limits =>
                    logInfo(s"Resource limits initialized for tenant ${tenant.id}")

                    // Step 6: Send webhook notification if configured
                    webhookUrl.foreach { url =>
                      sendWebhookNotification(url, tenant, template, startTime)
                    }

                    val duration = System.currentTimeMillis() - startTime
                    OnboardingResult.success(
                      tenant = tenant,
                      tier = tier,
                      dedicated = needsDedicated,
                      config = config,
                      limits = limits,
                      durationMs = duration
                    )
                  }.recover {
                    case e: Exception =>
                      logError(s"Failed to initialize limits: ${e.getMessage}")
                      OnboardingResult.failure(
                        s"Limits initialization failed: ${e.getMessage}",
                        Some(tenant)
                      )
                  }

                case Failure(e) =>
                  logError(s"Failed to initialize config: ${e.getMessage}")
                  Future.successful(OnboardingResult.failure(
                    s"Config initialization failed: ${e.getMessage}",
                    Some(tenant)
                  ))
              }

            case Failure(e) =>
              logError(s"Failed to provision database: ${e.getMessage}")
              Future.successful(OnboardingResult.failure(
                s"Database provisioning failed: ${e.getMessage}",
                Some(tenant)
              ))
          }

        case Failure(e) =>
          logError(s"Failed to create tenant: ${e.getMessage}")
          Future.successful(OnboardingResult.failure(s"Tenant creation failed: ${e.getMessage}"))
      }
    }

    result.asInstanceOf[Future[OnboardingResult]] // Help the compiler with type
  }

  /**
   * Onboard multiple tenants in batch
   */
  def onboardTenantsBatch(
                           requests: List[(String, String, TenantService.TenantRequirements)]
                         )(implicit ec: ExecutionContext): Future[List[OnboardingResult]] = {
    Future.sequence(requests.map { case (name, tier, req) =>
      onboardTenant(name, tier, req)
    })
  }

  private def validateOnboardingRequest(
                                         name: String,
                                         tier: String,
                                         requirements: TenantService.TenantRequirements
                                       ): Try[Unit] = Try {
    require(name != null && name.nonEmpty, "Tenant name cannot be empty")
    require(templates.contains(tier), s"Invalid tier: $tier. Must be one of: ${templates.keys.mkString(", ")}")
    require(requirements.expectedUsers >= 0, "Expected users must be non-negative")
  }

  private def sendWebhookNotification(
                                       url: String,
                                       tenant: TenantService.Tenant,
                                       template: TenantTemplate,
                                       startTime: Long
                                     ): Unit = {

    // Define explicit case classes for the payload
    case class TenantInfo(
                           id: String,
                           tenantId: String,
                           name: String,
                           tier: String
                         )

    case class WebhookPayload(
                               event: String,
                               timestamp: String,
                               tenant: TenantInfo,
                               durationMs: Long
                             )

    // Create the payload using case classes
    val payload = WebhookPayload(
      event = "tenant.onboarded",
      timestamp = Instant.now().toString,
      tenant = TenantInfo(
        id = tenant.id,
        tenantId = tenant.tenantId,
        name = tenant.name,
        tier = template.tier
      ),
      durationMs = System.currentTimeMillis() - startTime
    )

    // Convert to JSON using Circe
    import io.circe.syntax.*
    import io.circe.generic.auto.*

    val jsonPayload = payload.asJson.noSpaces

    try {
      val client = HttpClient.newHttpClient()
      val request = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(jsonPayload))
        .build()

      client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
        .thenAccept(response => {
          if (response.statusCode() >= 200 && response.statusCode() < 300) {
            logInfo(s"Webhook sent successfully to $url")
          } else {
            logError(s"Webhook failed with status ${response.statusCode()}: ${response.body()}")
          }
        })
    } catch {
      case e: Exception =>
        logError(s"Failed to send webhook: ${e.getMessage}")
    }
  }
}

case class OnboardingResult(
                             success: Boolean,
                             tenant: Option[TenantService.Tenant],
                             tier: String,
                             dedicated: Boolean,
                             config: Option[TenantConfig] = None,
                             limits: Option[TenantResourceLimits] = None,
                             error: Option[String] = None,
                             durationMs: Long
                           )

object OnboardingResult {
  def success(
               tenant: TenantService.Tenant,
               tier: String,
               dedicated: Boolean,
               config: TenantConfig,
               limits: TenantResourceLimits,
               durationMs: Long
             ): OnboardingResult = OnboardingResult(
    success = true,
    tenant = Some(tenant),
    tier = tier,
    dedicated = dedicated,
    config = Some(config),
    limits = Some(limits),
    error = None,
    durationMs = durationMs
  )

  def failure(error: String, tenant: Option[TenantService.Tenant] = None): OnboardingResult = OnboardingResult(
    success = false,
    tenant = tenant,
    tier = "",
    dedicated = false,
    error = Some(error),
    durationMs = 0L
  )
}