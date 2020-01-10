package com.clickstream

import org.apache.spark.sql.{DataFrame, Dataset}

trait ClickStreamInterface {
  def flattenClickStreamInput(clickStreamDf: DataFrame): DataFrame
  def purchaseAttributesProjectionAggregator(clickStreamDf: DataFrame, purchaseDf: DataFrame): DataFrame
  def purchaseAttributesProjectionDf(clickStreamDfFlattened: DataFrame, purchaseDf: DataFrame): DataFrame
  def topCampaigns(projectionDf: DataFrame): DataFrame
  def topCampaignsDataset(projectionDf: DataFrame): Dataset[String]
  def topChannelEngagementSql(clickStreamFlattenDf: DataFrame): DataFrame
  def topChannelEngagementDataframe(clickStreamFlattenDf: DataFrame): DataFrame
}
