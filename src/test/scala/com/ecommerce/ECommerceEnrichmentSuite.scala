package com.ecommerce

import com.ecommerce.enrichment.{SessionEnricher, SessionEnricherAgg}
import com.ecommerce.model.schema.ECommerceSchema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.junit.Test

/**
 * Check enrichment works correctly by comparing results
 * with manually precalculated on small samples
 */
class ECommerceEnrichmentSuite {

  private val ecommerceSessionEnricher = new SessionEnricher()
  private val ecommerceSessionEnricherAgg = new SessionEnricherAgg()

  private lazy val spark = SparkSession
    .builder()
    .appName("ECommerceEnrichmentTest")
    .master("local[1]")
    .getOrCreate()

  @Test def `check sessions were defined correctly`: Unit = {
    // in sample file we can see there're only 3 sessions
    val expectedNumberOfSessions = 3

    val ecommerceSampleDf = spark
      .read
      .option("header", "true")
      .schema(ECommerceSchema.inputSchema)
      .csv("src/test/resources/com/ecommerce/shortened_input_sample.csv")

    val sessioned = ecommerceSessionEnricher.enrich(spark, ecommerceSampleDf)
    val sessionedUdaf = ecommerceSessionEnricherAgg.enrich(spark, ecommerceSampleDf)

    val sessionsCount = sessioned.agg(countDistinct("sessionId")).collect().head.getLong(0)
    val sessionsCountUdaf = sessionedUdaf.agg(countDistinct("sessionId")).collect().head.getLong(0)

    assert((sessionsCount == sessionsCountUdaf) && sessionsCount == expectedNumberOfSessions)
  }

  @Test def `check sessions are different for same users due to time delay`: Unit = {
    val ecommerceSampleDf = spark
      .read
      .option("header", "true")
      .schema(ECommerceSchema.inputSchema)
      .csv("src/test/resources/com/ecommerce/shortened_input_sample2.csv")

    val sessioned = ecommerceSessionEnricher.enrich(spark, ecommerceSampleDf)
    val sessionedUdaf = ecommerceSessionEnricherAgg.enrich(spark, ecommerceSampleDf)

    val sessionsCount = sessioned.agg(countDistinct("sessionId")).collect().head.getLong(0)
    val sessionsCountUdaf = sessionedUdaf.agg(countDistinct("sessionId")).collect().head.getLong(0)

    assert((sessionsCount == sessionsCountUdaf) && sessionsCount == 2)
  }

  @Test def `check all events are within single session`: Unit = {
    val ecommerceSampleDf = spark
      .read
      .option("header", "true")
      .schema(ECommerceSchema.inputSchema)
      .csv("src/test/resources/com/ecommerce/shortened_input_sample3.csv")

    val sessioned = ecommerceSessionEnricher.enrich(spark, ecommerceSampleDf)

    val eventsBySession = sessioned
      .groupBy("sessionId")
      .agg(count("*").as('cnt))
      .select("cnt")
      .head.getLong(0)

    val totalInputRecords = sessioned.count

    assert(totalInputRecords == eventsBySession)
  }
}
