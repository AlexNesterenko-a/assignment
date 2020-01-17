package com.ecommerce.utils

import java.util.UUID

trait UUIDHelper {
  def randomUUID: String = UUID.randomUUID().toString.replaceAll("-", "")
}

object UUIDHelper extends UUIDHelper
