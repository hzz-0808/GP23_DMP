package com.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object JedisConnectionPool {

  private val config = new JedisPoolConfig()
  //设置最大连接数
  config.setMaxTotal(20)
  //设置最大获取数
  config.setMaxIdle(10)

  private val pool = new JedisPool(config,"192.168.159.149",6379,10000)
  def getJedisConnection()={

    pool.getResource

  }

}
