package com.ecommerce

import com.ecommerce.utils.ConfigHelper.getConfigOpt
import org.apache.spark.sql.SparkSession

trait ConnectionFactory[A] {
  def getConnection: A
}

object ConnectionFactory {
  implicit lazy val sparkSession: ConnectionFactory[SparkSession] = new ConnectionFactory[SparkSession] {
    override def getConnection: SparkSession =
      SparkSession
        .builder()
        .appName("ECommerceAnalyticsJob")
        .master(getConfigOpt[String]("app.spark.host").getOrElse("local"))
        .config("spark.driver.memory", getConfigOpt[String]("app.spark.driver-memory").getOrElse("1g"))
        .config("spark.cores.max", getConfigOpt[Int]("app.spark.max-cores").getOrElse(1))
        .config("spark.executor.cores", getConfigOpt[Int]("app.spark.executor-cores").getOrElse(1))
        .getOrCreate()
  }
}
