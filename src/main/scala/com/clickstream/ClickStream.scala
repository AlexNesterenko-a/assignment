package com.clickstream

import java.sql.Timestamp

import org.apache.spark.sql.expressions.{Aggregator, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession}
import ConfigHelper.getConfigOpt

import scala.collection.mutable

object ClickStream {

  // spark session object
  val spark: SparkSession =
    SparkSession
      .builder()
      .appName(getConfigOpt[String]("app.spark.name").getOrElse("Clickstream"))
      .master(getConfigOpt[String]("app.spark.host").getOrElse("local"))
      .config("spark.driver.memory", getConfigOpt[String]("driver-memory").getOrElse("1g"))
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

    purchaseAttributesProjectionAggregator(clickStreamDfFlattened, purchaseDf)

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

  /**
   * Make flat structure by extracting nested json data
   */
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

    val clickStreamDfFlattened = clickStreamDf
      .withColumn("purchase_struct", from_json($"attributes", purchaseJsonSchema))
      .withColumn("open_app", from_json($"attributes", appOpenJsonSchema))
      .withColumn("campaignId", $"open_app.campaign_id")
      .withColumn("channelId", $"open_app.channel_id")
      .withColumn("purchaseId", $"purchase_struct.purchase_id")
      .drop("open_app", "attributes", "purchase_struct")

    clickStreamDfFlattened
  }

  /**
   * Building projection by using window functions
   */
  def purchaseAttributesProjectionDf(clickStreamDfFlattened: DataFrame, purchaseDf: DataFrame): DataFrame = {
    val windowSpec = Window.partitionBy("userId").orderBy("eventTime")
    val windowBySessionSpec = Window.partitionBy("userId", "sessionId")

    val clickStreamDfSessioned = clickStreamDfFlattened
      .withColumn("prevEventTime", lag($"eventTime", 1).over(windowSpec))
      .withColumn("diff", unix_timestamp($"eventTime") - unix_timestamp($"prevEventTime"))
      .withColumn("initial", when($"diff".isNull || lag($"eventType", 1).over(windowSpec) === EventTypes.appClose, monotonically_increasing_id()))
      .withColumn("sessionId", last($"initial", true).over(windowSpec))
      .withColumn("curPurchase", last("purchaseId", true).over(windowBySessionSpec))
      .select($"userId", $"curPurchase".as('purchaseId), $"sessionId", $"campaignId", $"channelId")
      .where($"purchaseId".isNotNull && $"campaignId".isNotNull && $"channelId".isNotNull)

    val joinedWithPurchases = clickStreamDfSessioned.join(purchaseDf, Seq("purchaseId"), "inner").cache()

    val resultProjection = joinedWithPurchases.select($"purchaseId", $"purchaseTime", $"billingCost", $"isConfirmed", $"sessionId", $"campaignId", $"channelId")

    resultProjection.show()
    resultProjection
  }

  /**
   * For aggregation: sessionId, purchaseId, campaignId, channelId
   */
  private type AggType = (Int, String, String, String)

  /**
   * Groups all needed data
   * Obtains session id, purchases and etc info in 1 column
   */
  private class CustomAggregator extends Aggregator[Row, (String, AggType), String] with Serializable {

    private lazy val buffer = new mutable.HashMap[String, AggType]()
    private var purchaseId = ""
    private var campaignId = ""
    private var channelId = ""

    override def zero: (String, AggType) = ("", (-1, "", "", ""))

    override def reduce(b: (String, AggType), a: Row): (String, AggType) = {
      val key = a.getString(a.fieldIndex("purchaseId"))
      val eventId = a.getString(a.fieldIndex("eventId"))
      val sessionId = (a.getString(a.fieldIndex("userId")) + eventId.takeRight(1)).hashCode
      val curCampaignId = a.getString(a.fieldIndex("campaignId"))
      val curChannelId = a.getString(a.fieldIndex("channelId"))

      if (key != null) purchaseId = key
      if (curCampaignId != null) campaignId = curCampaignId
      if (curChannelId != null) channelId = curChannelId

      buffer.put(key, (sessionId, purchaseId, campaignId, channelId))
      (key, buffer(key))
    }


    override def merge(buf1: (String, AggType), buf2: (String, AggType)): (String, AggType) = {
      buf2
    }

    /**
     * Form json string as aggregation output
     */
    override def finish(reduction: (String, AggType)): String = {
      val value = reduction._2
      s"""
         |{"sessionId": ${value._1}, "purchaseId": "${value._2}", "campaignId": "${value._3}", "channelId": "${value._4}"}
         """.stripMargin
    }

    override def outputEncoder: Encoder[String] = Encoders.STRING

    override def bufferEncoder: Encoder[(String, AggType)] =
      Encoders.tuple(Encoders.STRING, Encoders.tuple(Encoders.scalaInt, Encoders.STRING, Encoders.STRING, Encoders.STRING))
  }

  /**
   * Building projection by using custom aggregator
   */
  def purchaseAttributesProjectionAggregator(clickStreamDf: DataFrame, purchaseDf: DataFrame): DataFrame = {
    val aggDf = clickStreamDf
      .groupBy("purchaseId").agg(new CustomAggregator().toColumn.as('agg))
      .select(
        $"purchaseId",
        get_json_object($"agg", "$.sessionId").as('sessionId),
        get_json_object($"agg", "$.campaignId").as('campaignId),
        get_json_object($"agg", "$.channelId").as('channelId)
      ).cache().filter($"purchaseId".isNotNull)

    val resultProjection = aggDf.join(purchaseDf, Seq("purchaseId"), "inner")
      .select($"purchaseId", $"purchaseTime", $"billingCost", $"isConfirmed", $"sessionId", $"campaignId", $"channelId")

    resultProjection.show()
    resultProjection
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
