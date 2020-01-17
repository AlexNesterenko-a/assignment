package com.ecommerce.statistics

import com.ecommerce.model.ECommerceColumns._
import com.ecommerce.utils.UUIDHelper
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class ECommerceStatisticsProvider(spark: SparkSession) {

  import spark.implicits._

  /**
   * For each category find median session duration
   */
  def findMedianSessionDuration(sessionedDf: DataFrame): DataFrame = {
    val tableName = "sessioned"
    sessionedDf.createOrReplaceTempView(tableName)
    spark.sql(
      s"""
        |with sessionDuration as (
        |   select
        |     ss.category,
        |     ss.sessionId,
        |     (unix_timestamp(max(ss.sessionEndTime)) - unix_timestamp(min(ss.sessionStartTime))) duration
        |   from $tableName ss
        |   group by ss.sessionId, ss.category
        |)
        |select
        |   sd.category,
        |   percentile(sd.duration, 0.5) medDuration
        |from sessionDuration sd
        |group by sd.category
        |""".stripMargin)
  }

  /**
   * For each category find # of unique users divided into time spending groups
   */
  def userGroupsBySessionDuration(sessionedDf: DataFrame): DataFrame = {
    import com.ecommerce.model.DurationConstraints._

    sessionedDf
      .groupBy(userId, category, sessionId)
      .agg((unix_timestamp(max($"sessionEndTime")) - unix_timestamp(min($"sessionStartTime"))).as(duration))
      .select(category, userId, duration, sessionId)
      .distinct()
      .groupBy(userId, category, sessionId).agg(sum(duration).as(duration))
      .withColumn(userDurationCategory,
        when($"duration" < oneMinute, "less than 1 min")
        .when($"duration".between(oneMinute, fiveMinutes), "1 to 5 mins")
        .otherwise("more than 5 mins")
      )
      .groupBy(category, userDurationCategory)
      .agg(count("*").as('cnt))
  }

  /**
   * Top n products ranked by time spent on product
   */
  def topProductsRankingInEachCat(ecommerceDf: DataFrame, nProducts: Int = 10): DataFrame = {

    val windowSpec = Window.partitionBy(userId).orderBy(eventTime)
    val windowByDurationSpec = Window.partitionBy(category).orderBy($"totalDuration".desc_nulls_last)

    val generateIdUdf = udf(() => UUIDHelper.randomUUID)

    ecommerceDf
      .withColumn("prevProduct", lag(product, 1).over(windowSpec))
      .withColumn(initial, when($"prevProduct".isNull || $"prevProduct" =!= $"product", generateIdUdf()))
      .withColumn(sessionId, last(initial, true).over(windowSpec))
      .groupBy(userId, category, product, sessionId)
      .agg((unix_timestamp(max(eventTime)) - unix_timestamp(min(eventTime))).as(duration))
      .groupBy(userId, category, product)
      .agg(sum(duration).as(totalDuration))
      .withColumn("rank", rank().over(windowByDurationSpec))
      .select(category, product, totalDuration, "rank")
      .where($"rank" <= nProducts)
  }

}
