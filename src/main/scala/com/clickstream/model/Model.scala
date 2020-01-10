package com.clickstream.model

import java.sql.Timestamp

/**
 * For revenue calculation dataset
 */
case class ProjectionWithRevenue(purchaseId: String,
                                 purchaseTime: Timestamp,
                                 billingCost: Double,
                                 isConfirmed: Boolean,
                                 sessionId: String,
                                 campaignId: String,
                                 channelId: String,
                                 revenue: Double)

/**
 * User event types
 */
object EventTypes {
  val appOpen = "app_open"
  val searchProduct = "search_product"
  val viewProductDetails = "view_product_details"
  val purchase = "purchase"
  val appClose = "app_close"
}
