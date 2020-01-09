package clickstream

import com.clickstream.ClickStream
import org.junit.Test

class ClickStreamSuite {
  private val clickStreamDf = ClickStream.read("src/main/resources/clickstream/clickstream.csv")
  private val purchaseDf = ClickStream.read("src/main/resources/clickstream/purchases.csv")

  // for access from plain sql
  purchaseDf.createOrReplaceTempView("purchases")

  private val clickStreamDfFlattened = ClickStream.flattenClickStreamInput(clickStreamDf)

  // for access from plain sql
  clickStreamDfFlattened.createOrReplaceTempView("clickStreamFlattened")

  // clickstream projection results
  private val projectionDf1 = ClickStream.purchaseAttributesProjectionSql()
  // cache here as we will reuse this dataset below
  private val projectionDf2 = ClickStream.purchaseAttributesProjectionDf(clickStreamDfFlattened, purchaseDf).cache()

  // top campaigns results
  private val topCampaignsDf = ClickStream.topCampaigns(projectionDf2)
  private val topCampaignsDs = ClickStream.topCampaignsDataset(projectionDf2)

  // top channels results
  private val topChannelsDf1 = ClickStream.topChannelEngagement(projectionDf2)
  private val topChannelsDf2 = ClickStream.topChannelEngagementDataframe(projectionDf2)

  @Test def `check dataframe and sql approaches produce same result`: Unit = {
    import org.apache.spark.sql.functions._
    // print results
    projectionDf1.show()
    projectionDf2.show()
    val billingCostSum1 = projectionDf1.agg(sum("billingCost").cast("long")).first.getLong(0)
    val billingCostSum2 = projectionDf2.agg(sum("billingCost").cast("long")).first.getLong(0)
    // compare results by billing costs sum
    assert(billingCostSum1 == billingCostSum2)
  }

  @Test def `check top campaigns calculated correctly with spark sql and dataset api`: Unit = {
    topCampaignsDf.show()
    topCampaignsDs.show()
    val topCampaignsFromDatasetRes = topCampaignsDs.collect()
    val topCampaignsFromDataframeRes = topCampaignsDf.collect().map(_.getString(0))
    assert(topCampaignsFromDatasetRes sameElements topCampaignsFromDataframeRes)
  }

  @Test def `check top channels calculated correctly with spark sql and dataframe api`: Unit = {
    topChannelsDf1.show()
    topChannelsDf2.show()
    val topChannelsDfRes1 = topChannelsDf1.collect().map(_.getString(0))
    val topChannelsDfRes2 = topChannelsDf2.collect().map(_.getString(0))
    assert(topChannelsDfRes1 sameElements topChannelsDfRes2)
  }
}
