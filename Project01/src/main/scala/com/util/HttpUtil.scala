package com.util

import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.util.EntityUtils


object HttpUtil {

  def getHttp(url:String)={
    val httpClient: CloseableHttpClient = HttpClients.createDefault()

    val httpGet = new HttpGet(url)

    val response: CloseableHttpResponse = httpClient.execute(httpGet)

    EntityUtils.toString(response.getEntity,"UTF-8")

  }

}
