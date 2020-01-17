package com.ecommerce.enrichment

import com.ecommerce.model.DurationConstraints
import com.ecommerce.model.ECommerceColumns._
import com.ecommerce.model.schema.ECommerceSchema
import com.ecommerce.utils.UUIDHelper
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

trait ECommerceEnricher {
  def enrich(spark: SparkSession, df: DataFrame): DataFrame
}

class SessionEnricher extends ECommerceEnricher {

  override def enrich(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._

    val windowSpec = Window.partitionBy(userId, category).orderBy(eventTime)
    val windowBySessionSpec = Window.partitionBy(userId, category, sessionId)

    val generateIdUdf = udf(() => UUIDHelper.randomUUID)

    val dfEnriched =
      df
        .withColumn("prevEventTime", lag(eventTime, 1).over(windowSpec))
        .withColumn("timeDiff", unix_timestamp($"eventTime") - unix_timestamp($"prevEventTime"))
        .withColumn(initial, when($"timeDiff".isNull || $"timeDiff" > DurationConstraints.fiveMinutes, generateIdUdf()))
        .withColumn(sessionId, last("initial", true).over(windowSpec))
        .withColumn(sessionStartTime, min("eventTime").over(windowBySessionSpec))
        .withColumn(sessionEndTime, max(eventTime).over(windowBySessionSpec))
        .select($"userId", $"category", $"product", $"eventTime", $"eventType", $"sessionId", $"sessionStartTime", $"sessionEndTime")
        .orderBy("eventTime")

    dfEnriched
  }

}

class SessionEnricherAgg extends ECommerceEnricher {

  override def enrich(spark: SparkSession, df: DataFrame): DataFrame = {

    spark.udf.register("session_retrieve_func", new SessionRetrieveAggFunction())

    val tempViewName = "ecommerce"

    df.createOrReplaceTempView(tempViewName)

    val dfEnriched = spark.sql(
      s"""
         |with enriched as (
         |  select
         |    ec.*,
         |    session_retrieve_func(ec.eventTime) over (partition by ec.userId, ec.category order by ec.eventTime) sessionId
         |    from $tempViewName ec
         |)
         |select
         |  enr.*,
         |  min(enr.eventTime) over (partition by enr.sessionId) as sessionStartTime,
         |  max(enr.eventTime) over (partition by enr.sessionId) as sessionEndTime
         |  from enriched enr
         |  order by enr.eventTime
         |""".stripMargin)

    dfEnriched
  }

}

/**
 * Custom aggregation function
 * used for distinguishing individual session through events
 */
class SessionRetrieveAggFunction extends UserDefinedAggregateFunction {

  private val eventTimeIdx = 0
  private val sessionIdIdx = 1

  override def initialize(buffer: MutableAggregationBuffer): Unit =
    buffer.update(sessionIdIdx, UUIDHelper.randomUUID)

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val curEventTime = input.getTimestamp(eventTimeIdx)

    if (!buffer.isNullAt(eventTimeIdx)) {
      val prevEventTime = buffer.getTimestamp(eventTimeIdx)

      // times 1000 to get seconds from millis
      if ((curEventTime.getTime - prevEventTime.getTime) > DurationConstraints.fiveMinutes * 1000) {
        buffer.update(sessionIdIdx, UUIDHelper.randomUUID)
      }
    }

    buffer.update(eventTimeIdx, curEventTime)
  }

  override def evaluate(buffer: Row): Any = buffer.getString(sessionIdIdx)

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Nothing = ???

  override def deterministic: Boolean = true

  override def inputSchema: StructType = ECommerceSchema.udafInputSchema

  override def bufferSchema: StructType = ECommerceSchema.bufferSchema

  override def dataType: DataType = StringType
}
