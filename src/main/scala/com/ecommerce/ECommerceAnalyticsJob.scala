package com.ecommerce

import com.ecommerce.utils.ConfigHelper.getConfigOpt
import com.ecommerce.enrichment.{SessionEnricher, SessionEnricherAgg}
import com.ecommerce.model.schema.ECommerceSchema
import com.ecommerce.statistics.ECommerceStatisticsProvider
import org.apache.spark.sql.{DataFrame, SparkSession}

object ECommerceAnalyticsJob extends SessionProvider {

  private val spark = getSession[SparkSession].getConnection

  private val ecommerceSessionEnricher = new SessionEnricher()
  private val ecommerceSessionEnricherAgg = new SessionEnricherAgg()

  private val ecommerceStatisticProvider = new ECommerceStatisticsProvider(spark)

  def apply(): Unit = {
    runJob()
  }

  private def runJob(): Unit = {

    val ecommerceDf = spark
      .read
      .option("header", "true")
      .schema(ECommerceSchema.inputSchema)
      .csv(getConfigOpt[String]("app.source.path").getOrElse(""))

    val ecommerceSessionedDf = enrichedWithSessions(ecommerceDf).cache()

    ecommerceSessionedDf.show()

    enrichedWithSessionsAggr(ecommerceDf).show()
    medianSessionDuration(ecommerceSessionedDf).show()
    userGroupsBySessionDuration(ecommerceSessionedDf).show()
    topProductsRankingInEachCat(ecommerceDf).show()

    spark.close()
  }

  def enrichedWithSessions(ecommerceDf: DataFrame): DataFrame = {
    ecommerceSessionEnricher.enrich(spark, ecommerceDf)
  }

  def enrichedWithSessionsAggr(ecommerceDf: DataFrame): DataFrame = {
    ecommerceSessionEnricherAgg.enrich(spark, ecommerceDf)
  }

  def medianSessionDuration(ecommerceSessionedDf: DataFrame): DataFrame = {
    ecommerceStatisticProvider.findMedianSessionDuration(ecommerceSessionedDf)
  }

  def userGroupsBySessionDuration(ecommerceSessionedDf: DataFrame): DataFrame = {
    ecommerceStatisticProvider.userGroupsBySessionDuration(ecommerceSessionedDf)
  }

  def topProductsRankingInEachCat(ecommerceDf: DataFrame): DataFrame = {
    ecommerceStatisticProvider.topProductsRankingInEachCat(ecommerceDf)
  }
}
