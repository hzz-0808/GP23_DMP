package Exam

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

object Test1 {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("Tes1").master("local[*]").getOrCreate()
    val src: RDD[String] = sparkSession
      .read.textFile("G:\\Spark\\项目\\项目day01\\Spark用户画像分析\\json.txt").rdd

    //1、按照pois，分类businessarea，并统计每个businessarea的总数。
     src.map(JSON.parseObject(_)).filter(_.getInteger("status") == 1).flatMap(jsonobj => {
      var list = List[(String, Int)]()
       val jsonobj1: JSONObject = jsonobj.getJSONObject("regeocode")
       if(jsonobj1!=null) {
         val roadsArray = jsonobj1.getJSONArray("pois").toArray
         if(roadsArray!=null){
         for (item <- roadsArray) {
           val obj1: JSONObject = item.asInstanceOf[JSONObject]
           if (obj1 != null) {
             val businessarea: String = obj1.getString("businessarea")
             if (businessarea != null && businessarea != "") {
               list :+= (businessarea, 1)
             }
           }
         }
         }
       }
      list
    }).filter(_._1!="[]").reduceByKey(_+_).collect().foreach(println)

    //2、按照pois，分类type，为每一个Type类型打上标签，统计各标签的数量
    src.map(JSON.parseObject(_)).filter(_.getInteger("status") == 1).flatMap(jsonobj=>{
      var list = List[(String, Int)]()
      val types: String = getType(jsonobj)
      val arr: Array[String] = types.split(";")
      arr.foreach(list :+= (_,1))
      list
    }).reduceByKey(_+_).collect().foreach(println)

  }

  def getType(jObject: JSONObject):String={
    var list = List[String]()
    val jsonobj1: JSONObject = jObject.getJSONObject("regeocode")
    if(jsonobj1==null) return ""
    val poisArray: Array[AnyRef] = jsonobj1.getJSONArray("pois").toArray()
    if(poisArray==null) return ""

    for(item <- poisArray){
      val obj1: JSONObject = item.asInstanceOf[JSONObject]
      val types: String = obj1.getString("type")
      if(types!=null && types!=""){
        list :+= types
      }
    }

    list.mkString(";")
  }

}
