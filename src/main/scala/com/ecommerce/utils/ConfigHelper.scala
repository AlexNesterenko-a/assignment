package com.ecommerce.utils

import com.typesafe.config.{Config, ConfigFactory}

object ConfigHelper {
  private lazy val conf = ConfigFactory.load("reference.conf")

  def getConfigOpt[T](path: String): Option[T] = conf.getByPathOpt(path)

  implicit class ConfExts(config: Config) {
    def getByPathOpt[T](path: String): Option[T] = {
      if (config.hasPath(path)) Some(config.getAnyRef(path).asInstanceOf[T]) else None
    }
  }
}
