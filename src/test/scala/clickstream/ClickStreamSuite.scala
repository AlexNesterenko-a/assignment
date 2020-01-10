package clickstream

import com.clickstream.ClickStream
import org.junit.Test

class ClickStreamSuite {

  import ClickStream.spark.implicits._

  private val clickStreamDf = ClickStream.read("src/main/resources/clickstream/clickstream.csv")
  private val purchaseDf = ClickStream.read("src/main/resources/clickstream/purchases.csv")

  private val clickStreamDfFlattened = ClickStream.flattenClickStreamInput(clickStreamDf)

  // clickstream projection results
  private val projectionDf1 = ClickStream.purchaseAttributesProjectionAggregator(clickStreamDfFlattened, purchaseDf)
  // cache here as we will reuse this dataset below
  private val projectionDf2 = ClickStream.purchaseAttributesProjectionDf(clickStreamDfFlattened, purchaseDf).cache()

  // top campaigns results
  private val topCampaignsDf = ClickStream.topCampaigns(projectionDf2)
  private val topCampaignsDs = ClickStream.topCampaignsDataset(projectionDf2)

  // top channels results
  private val topChannelsDf1 = ClickStream.topChannelEngagement(projectionDf2)
  private val topChannelsDf2 = ClickStream.topChannelEngagementDataframe(projectionDf2)

  @Test def `check window functions and custom aggregator approaches produce same result`: Unit = {
    import org.apache.spark.sql.functions._
    // print results
    projectionDf1.show()
    projectionDf2.show()
    val billingCostByChannelId1 = projectionDf1.groupBy($"channelId").agg(sum($"billingCost"))
    val billingCostByChannelId2 = projectionDf2.groupBy($"channelId").agg(sum($"billingCost"))
    // compare results by aggregation of billing costs by each channel
    assert(billingCostByChannelId1.except(billingCostByChannelId2).count == 0)
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
    assert(topChannelsDf1.except(topChannelsDf2).count == 0)
  }
}
