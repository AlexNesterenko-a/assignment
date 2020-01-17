package com.ecommerce

trait SessionProvider {

  def getSession[A: ConnectionFactory]: ConnectionFactory[A] = implicitly[ConnectionFactory[A]]

}
