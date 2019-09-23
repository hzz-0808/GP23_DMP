package com.util

import ch.hsr.geohash.GeoHash
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

import scala.collection.mutable
import scala.collection.mutable.HashMap

object LabelUtils {
  def adspacetypeLabel(row:Row,map:HashMap[String,Int])= {
    val adspacetype: Int = row.getAs[Int]("adspacetype")
    val adspacetypelable = f"LC$adspacetype%02d"
    map.put(adspacetypelable, 1)
    val adspacetypename: String = row.getAs[String]("adspacetypename")
    map.put("LN"+adspacetypename, 1)

    map

  }

  def appnameLabel(row:Row,broadcast: Broadcast[Map[String, String]],map:HashMap[String,Int])={
    val dict_map: Map[String, String] = broadcast.value
    var userid: String = row.getAs[String]("userid")
    var appname: String = row.getAs[String]("appname")
    if(appname.equals("其他")){
      appname=dict_map.getOrElse(row.getAs[String]("appid"),"未知")
    }
    appname=s"APP$appname"
    map.put(appname,1)

  }

  def adplatformproviderLabel(row:Row,map:HashMap[String,Int])= {

    var userid: String = row.getAs[String]("userid")
    val adplatformproviderid: Int = row.getAs[Int]("adplatformproviderid")
    val adplatformproviderlabel = "CN" + adplatformproviderid
    map.put(adplatformproviderlabel, 1)
    map


  }

  def clientLabel(row:Row,map:HashMap[String,Int])= {
    var userid: String = row.getAs[String]("userid")
    var client: Int = row.getAs[Int]("client")
    val clientlabel: String = client match {
      case 1 => "D00010001"
      case 2 => "D00010002"
      case 3 => "D00010003"
      case _ => "D00010004"
    }
    map.put(clientlabel, 1)
    map
  }

  def networkmannerLabel(row:Row,map:HashMap[String,Int])= {
    var userid: String = row.getAs[String]("userid")
    var networkmannername: String = row.getAs[String]("networkmannername")
    val networkmannerlabel: String = networkmannername match {
      case "Wifi" => "D00020001"
      case "4G" => "D00020002"
      case "3G" => "D00020003"
      case "2G" => "D00020004"
      case _ => "D00020005"
    }
    map.put(networkmannerlabel, 1)
    map

  }

  def ispnameLabel(row:Row,map:HashMap[String,Int])= {
    var userid: String = row.getAs[String]("userid")
    var ispname: String = row.getAs[String]("ispname")
    val ispnamelabel: String = ispname match {
      case "移动" => "D00030001"
      case "联通" => "D00030002"
      case "电信" => "D00030003"
      case _ => "D00030004"
    }
    map.put(ispnamelabel, 1)
    map
  }

  def keywordsLabel(row:Row,map:HashMap[String,Int],broadcast: Broadcast[Map[String,Int]])= {
    var userid: String = row.getAs[String]("userid")
    var keywords: String = row.getAs[String]("keywords")
    val keys: Array[String] = keywords.split("\\|")
      .filter(arr => arr.length >= 3 && arr.length <= 8 && !broadcast.value.contains(arr))
    keys.foreach(key => {
      map.put("K" + key, 1)
    })
    map

  }

  def areaLabel(row:Row,map:HashMap[String,Int])= {

    var userid: String = row.getAs[String]("userid")
    var provincename: String = row.getAs[String]("provincename")

    var cityname: String = row.getAs[String]("cityname")

    map.put("ZP" + provincename, 1)
    map.put("ZC" + cityname, 1)
    map

  }

  def businessDistrictLabel(row:Row,map:HashMap[String,Int])={
    var userid: String = row.getAs[String]("userid")
    val long: Double = String2Type.toDouble(row.getAs[String]("longed"))
    val lat: Double = String2Type.toDouble(row.getAs[String]("lat"))
    if(long>=73 && long<=136 && lat>=3 && lat<=53){
      val business: String = getBusiness(long,lat)
      val businesses: Array[String] = business.split(",")

      businesses.foreach(arr=>{
        map.put("SQ"+arr,1)
      })
    }
    map
  }

  def getBusiness(long:Double,lat:Double)={
    val geohash: String = GeoHash.geoHashStringWithCharacterPrecision(lat,long,6)

    var business: String = redis_RequaryBusiness(geohash)
    if(business==null){
      //获取商圈信息
      business = getBusinessFromAMap(long,lat)
      redis_InsertBusiness(geohash,business)
    }
    business

  }

  def redis_RequaryBusiness(geohash:String)={
    val jedis: Jedis = JedisConnectionPool.getJedisConnection()

    val business: String = jedis.get(geohash)
    jedis.close()
    business
  }

  def redis_InsertBusiness(geohash:String,business:String): Unit ={
    val jedis: Jedis = JedisConnectionPool.getJedisConnection()

    jedis.set(geohash,business)
    jedis.close()
  }

  def getBusinessFromAMap(long:Double,lat:Double):String={
    val url = s"https://restapi.amap.com/v3/geocode/regeo?location=$long,$lat&key=02c277d5b2c279e76667145fc9a9c821"
    val jsonStr: String = HttpUtil.getHttp(url)
    //println(jsonStr)

    val jSONObject1: JSONObject = JSON.parseObject(jsonStr)

    val status: Integer = jSONObject1.getInteger("status")

    if(status==0) return ""

    val jSONObject2: JSONObject = jSONObject1.getJSONObject("regeocode")

    if(jSONObject2==null) return ""

    val jSONObject3: JSONObject = jSONObject2.getJSONObject("addressComponent")

    if(jSONObject3==null) return ""

    val jsonArray: Array[AnyRef] = jSONObject3.getJSONArray("businessAreas").toArray

    if(jsonArray==null || jsonArray.length==0) return ""

    val list = mutable.ListBuffer[String]()
    for(item <- jsonArray){
      if(item.isInstanceOf[JSONObject]){
        list += item.asInstanceOf[JSONObject].getString("name")
      }
    }
    list.mkString(",")

  }


}
