package com.ecommerce.model.schema

import com.ecommerce.model.ECommerceColumns._
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

object ECommerceSchema {

  // source schema
  val inputSchema = StructType(Array(
    StructField(category, StringType, true),
    StructField(product, StringType, true),
    StructField(userId, StringType, true),
    StructField(eventTime, TimestampType, true),
    StructField(eventType, StringType, true)))

  // schemas for aggregation
  val bufferSchema = new StructType()
    .add("previousTime", TimestampType)
    .add(sessionId, StringType)

  val udafInputSchema = new StructType().add(eventTime, TimestampType)
}
