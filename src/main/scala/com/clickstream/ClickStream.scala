package com.clickstream

import java.sql.Timestamp

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object ClickStream {

  // spark session object
  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Clickstream")
      .master("local")
      .getOrCreate()

  // For implicit conversions like converting RDDs to DataFrames, call methods on dataframe columns and etc
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    // read input data
    val clickStreamDf = read("src/main/resources/clickstream/clickstream.csv")
    val purchaseDf = read("src/main/resources/clickstream/purchases.csv")

    // for access from plain sql
    purchaseDf.createOrReplaceTempView("purchases")

    val clickStreamDfFlattened = flattenClickStreamInput(clickStreamDf)

    // for access from plain sql
    clickStreamDfFlattened.createOrReplaceTempView("clickStreamFlattened")

    // build projection using sql
    purchaseAttributesProjectionSql()

    // build projection using dataframe API
    // cache result here, as it will be used by next operations
    val resultProjection = purchaseAttributesProjectionDf(clickStreamDfFlattened, purchaseDf).cache()

    // calculate top campaigns/channels via sql and dataframe/dataset API
    topCampaigns(resultProjection)
    topCampaignsDataset(resultProjection)
    topChannelEngagement(resultProjection)
    topChannelEngagementDataframe(resultProjection)

    spark.close()
  }

  // get dataframe by path to csv file
  // escape quotes to read json data normally
  def read(path: String): DataFrame = {
    spark.read.options(Map("header" -> "true", "inferSchema" -> "true")).option("escape", "\"").csv(path)
  }

  def flattenClickStreamInput(clickStreamDf: DataFrame): DataFrame = {
    // describe schema for nested json (attributes)
    val appOpenJsonSchema = StructType(
      Seq(
        StructField("campaign_id", StringType, true),
        StructField("channel_id", StringType, true)
      )
    )
    val purchaseJsonSchema = StructType(
      Seq(
        StructField("purchase_id", StringType, true)
      )
    )

    // make flat structure by extracting nested fields
    val clickStreamDfFlattened = clickStreamDf
      .withColumn("purchase_struct", from_json($"attributes", purchaseJsonSchema))
      .withColumn("open_app", from_json($"attributes", appOpenJsonSchema))
      .withColumn("campaignId", $"open_app.campaign_id")
      .withColumn("channelId", $"open_app.channel_id")
      .withColumn("purchase_id", $"purchase_struct.purchase_id")
      .drop("open_app", "attributes", "purchase_struct")

    clickStreamDfFlattened
  }

  // DataFrame API realization
  def purchaseAttributesProjectionDf(clickStreamDfFlattened: DataFrame, purchaseDf: DataFrame): DataFrame = {
    val windowSpec = Window.partitionBy("userId").orderBy("eventTime")

    val clickStreamDfSessioned = clickStreamDfFlattened
      .withColumn("purchaseToJoin", lead($"purchase_id", 3).over(windowSpec))
      .where($"eventType" === EventTypes.appOpen)

    val joinedWithPurchases = clickStreamDfSessioned.join(purchaseDf, $"purchaseToJoin" === $"purchaseId", "inner").cache()

    val resultProjection = joinedWithPurchases.select($"purchaseId", $"purchaseTime", $"billingCost", $"isConfirmed", $"eventId".as('sessionId), $"campaignId", $"channelId")

    resultProjection.show()
    resultProjection
  }

  // Plain SQL realization
  def purchaseAttributesProjectionSql(): DataFrame = {
    spark.sql(s"select csf.*, " +
      s"lead(csf.purchase_id, 3) over (partition by csf.userId order by csf.eventTime) as purchaseToJoin " +
      s"from clickStreamFlattened csf").where($"eventType" === EventTypes.appOpen)
      .createOrReplaceTempView("clickStreamSessioned")

    val joinedWithPurchases = spark
      .sql("select p.purchaseId, p.purchaseTime, p.billingCost, p.isConfirmed, css.eventId as sessionId, css.campaignId, css.channelId " +
      "from clickStreamSessioned css " +
      "inner join purchases p " +
      "on css.purchaseToJoin = p.purchaseId")

    joinedWithPurchases.show()
    joinedWithPurchases
  }

  // top 10 campaigns by billing cost sql realization
  def topCampaigns(projectionDf: DataFrame): DataFrame = {
    projectionDf.createOrReplaceTempView("projection")
    val resultDf = spark.sql("select p.campaignId from projection p order by p.billingCost limit 10")

    resultDf.show()
    resultDf
  }

  // top 10 campaigns by billing cost dataset realization
  def topCampaignsDataset(projectionDf: DataFrame): Dataset[String] = {
    val projectionDs = projectionDf.as[Projection]
    val resultDs = projectionDs.orderBy($"billingCost".desc).map(_.campaignId).limit(10)

    resultDs.show()
    resultDs
  }

  // top 3 channel engagement sql realization
  def topChannelEngagement(projectionDf: DataFrame): DataFrame = {
    projectionDf.createOrReplaceTempView("projection")
    val resultDf = spark.sql("select p.channelId from projection p group by p.channelId order by count(p.sessionId) limit 3")

    resultDf.show()
    resultDf
  }

  // top 3 channel engagement dataframe realization
  def topChannelEngagementDataframe(projectionDf: DataFrame): DataFrame = {
    val resultDf = projectionDf
      .groupBy($"channelId").agg(count($"sessionId").as('sessionCount))
      .orderBy($"sessionCount".desc)
      .select("channelId").limit(3)

    resultDf.show()
    resultDf
  }

}

// User event types
object EventTypes {
  val appOpen = "app_open"
  val searchProduct = "search_product"
  val viewProductDetails = "view_product_details"
  val purchase = "purchase"
  val appClose = "app_close"
}

// Model for projection
case class Projection(purchaseId: String,
                      purchaseTime: Timestamp,
                      billingCost: Double,
                      isConfirmed: Boolean,
                      sessionId: String,
                      campaignId: String,
                      channelId: String)
