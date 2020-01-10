package com.clickstream

import com.clickstream.ConfigHelper.getConfigOpt
import com.clickstream.model.{EventTypes, ProjectionWithRevenue}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.{Aggregator, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable

object ClickStream extends ClickStreamInterface {

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Clickstream")
      .master(getConfigOpt[String]("app.spark.host").getOrElse("local"))
      .config("spark.driver.memory", getConfigOpt[String]("driver-memory").getOrElse("1g"))
      .getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    // read input data
    val clickStreamDf = read("src/main/resources/com/clickstream/clickstream.csv")
    val purchaseDf = read("src/main/resources/com/clickstream/purchases.csv")

    val clickStreamDfFlattened = flattenClickStreamInput(clickStreamDf)

    purchaseAttributesProjectionAggregator(clickStreamDfFlattened, purchaseDf)

    // build projection using dataframe API
    // cache result here, as it will be used by next operations
    val resultProjection = purchaseAttributesProjectionDf(clickStreamDfFlattened, purchaseDf).cache()

    // calculate top campaigns/channels via sql and dataframe/dataset API
    topCampaigns(resultProjection)
    topCampaignsDataset(resultProjection)
    topChannelEngagementSql(clickStreamDfFlattened)
    topChannelEngagementDataframe(clickStreamDfFlattened)

    spark.close()
  }

  /**
   * get dataframe by path to csv file
   * escape quotes to read json data normally
   */
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

    resultProjection
  }

  def topCampaigns(projectionDf: DataFrame): DataFrame = {
    projectionDf.createOrReplaceTempView("projection")
    val resultDf = spark.sql(
      s"""
         |with projectionWithRevenue as
         |  (select
         |    p.*, sum(p.billingCost) over (partition by p.campaignId) as revenue
         |   from projection p
         |  )
         |select pr.campaignId from projectionWithRevenue pr order by pr.revenue desc limit 10
         |""".stripMargin
    )

    resultDf
  }

  def topCampaignsDataset(projectionDf: DataFrame): Dataset[String] = {
    val ds = projectionDf
      .withColumn("revenue", sum($"billingCost").over(Window.partitionBy($"campaignId")).as('revenue))
      .orderBy($"revenue".desc)
      .as[ProjectionWithRevenue]
      .limit(10)

    val resultDs = ds.map(_.campaignId).limit(10)

    resultDs
  }

  /**
   * top 3 channel engagement sql realization
   */
  def topChannelEngagementSql(clickStreamFlattenDf: DataFrame): DataFrame = {
    clickStreamFlattenDf.createOrReplaceTempView("csFlat")
    val resultDf = spark.sql(
      s"""
         |with channelCount as
         |  (
         |    select csf.*, count(csf.channelId) over (partition by csf.campaignId) as cnt
         |    from csFlat csf
         |  ),
         |nonZeroCount as (select distinct campaignId, channelId, cnt from channelCount where cnt > 0),
         |res as (select nonZeroCount.*, row_number() over (partition by nonZeroCount.campaignId order by nonZeroCount.cnt desc) rn from nonZeroCount)
         |select res.campaignId, res.channelId from res where res.rn <= 3
         |""".stripMargin
    )

    resultDf
  }

  /**
   * top 3 channel engagement dataframe realization
   */
  def topChannelEngagementDataframe(clickStreamFlattenDf: DataFrame): DataFrame = {
    val resultDf = clickStreamFlattenDf
      .withColumn("cnt", count($"channelId").over(Window.partitionBy($"campaignId")))
      .select("campaignId", "channelId", "cnt")
      .where($"cnt" > 0)
      .distinct()
      .withColumn("rn", row_number().over(Window.partitionBy($"campaignId").orderBy($"cnt".desc)))
      .where($"rn" <= 3)
      .select("campaignId", "channelId")

    resultDf
  }

}
