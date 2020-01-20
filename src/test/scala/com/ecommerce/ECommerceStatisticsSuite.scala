package com.ecommerce

import com.ecommerce.enrichment.SessionEnricher
import com.ecommerce.model.schema.ECommerceSchema
import com.ecommerce.statistics.ECommerceStatisticsProvider
import org.apache.spark.sql.SparkSession
import org.junit.Test

/**
 * Check statistic is computed correctly by comparing results
 * with manually precalculated on small samples
 */
class ECommerceStatisticsSuite {

  private lazy val spark = SparkSession
    .builder()
    .appName("ECommerceEnrichmentTest")
    .master("local[1]")
    .getOrCreate()

  private val statisticProvider = new ECommerceStatisticsProvider(spark)

  private val ecommerceSessionEnricher = new SessionEnricher()

  @Test def `check median duration calculated correctly`: Unit = {

    val ecommerceSampleDf = spark
      .read
      .option("header", "true")
      .schema(ECommerceSchema.inputSchema)
      .csv("src/test/resources/com/ecommerce/shortened_input_sample_stat.csv")

    val ecommerceSessionedDf = ecommerceSessionEnricher.enrich(spark, ecommerceSampleDf)

    val medSessionDurationDf = statisticProvider.findMedianSessionDuration(ecommerceSessionedDf)

    val actualMap = medSessionDurationDf.collect().map { r =>
      r.getString(0) -> r.getDouble(1)
    }.toMap

    // precalculated
    val expectedMap = Map(
      "books" -> 107.0,
      "notebooks" -> 134.0,
      "mobile phones" -> 0.0
    )

    assert(actualMap.equals(expectedMap))
  }

  @Test def `check user events are distributed by groups correctly`: Unit = {

    val ecommerceSampleDf = spark
      .read
      .option("header", "true")
      .schema(ECommerceSchema.inputSchema)
      .csv("src/test/resources/com/ecommerce/shortened_input_sample_groups.csv")

    val ecommerceSessionedDf = ecommerceSessionEnricher.enrich(spark, ecommerceSampleDf)

    val groupedDf = statisticProvider.userGroupsBySessionDuration(ecommerceSessionedDf)

    val actualResult = groupedDf.collect().map { r =>
      (r.getString(0), r.getString(1), r.getLong(2))
    }.toSet

    val expectedResult = Set(
      ("mobile phones", "less than 1 min", 2L),
      ("books", "1 to 5 mins", 1L),
      ("notebooks", "1 to 5 mins", 1L),
      ("books", "less than 1 min", 2L),
      ("books", "more than 5 mins", 2L)
    )

    assert(actualResult.equals(expectedResult))
  }

  @Test def `check top products ranked`: Unit = {

    val sampleDf = spark
      .read
      .option("header", "true")
      .schema(ECommerceSchema.inputSchema)
      .csv("src/test/resources/com/ecommerce/shortened_input_sample_ranking.csv").cache()

    val sessionedDf = ecommerceSessionEnricher.enrich(spark, sampleDf)

    val groupedDf = statisticProvider.topProductsRankingInEachCat(sessionedDf)

    val actualResult = groupedDf.collect().map { r =>
      (r.getString(0), r.getString(1), r.getLong(2), r.getInt(3))
    }.toSet

    val expectedResult = Set(
      ("books", "Scala for Dummies", 108L, 1),
      ("books", "Java for Dummies", 2L, 2),
      ("notebooks", "MacBook Air", 0L, 1),
      ("notebooks", "MacBook Pro 13", 0L, 1),
      ("mobile phones", "iPhone X", 317L, 1)
    )

    assert(actualResult.equals(expectedResult))
  }
}
