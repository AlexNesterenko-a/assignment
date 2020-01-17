package com.ecommerce.model

object ECommerceColumns {

  // input columns
  val category = "category"
  val product = "product"
  val userId = "userId"
  val eventTime = "eventTime"
  val eventType = "eventType"

  // enriched columns
  val initial = "initial"
  val sessionId = "sessionId"
  val sessionStartTime = "sessionStartTime"
  val sessionEndTime = "sessionEndTime"

  // statistics
  val duration = "duration"
  val totalDuration = "totalDuration"
  val userDurationCategory = "userCategory"
}
