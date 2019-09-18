package com.location

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.mutable


object Target {
  def main(args: Array[String]): Unit = {
    if(args.length!=2){
      println("目录错误")
      sys.exit()
    }

    val Array(inputDir1,inputDir2)=args

    val sparkSession: SparkSession = SparkSession.builder()
      .appName("ct")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    import sparkSession.implicits._
    import org.apache.spark.sql.functions._
    val df: DataFrame = sparkSession.read.parquet(inputDir1)

    val markdf: DataFrame = df.select(df.col("provincename"), df.col("cityname"),
      df.col("ispname"),
      df.col("networkmannername"),
      when($"devicetype"===1,"手机").otherwise("平板") as "devicetype",
      when($"client"===1,"android")
        .when($"client"===2,"ios").otherwise("wp") as "client",
      df.col("appid"),
      df.col("appname"),
      when($"requestmode" === 1 && $"processnode" >= 1, 1).otherwise(0) as "srcReq",
      when($"requestmode" === 1 && $"processnode" >= 2, 1).otherwise(0) as "validReq",
      when($"requestmode" === 1 && $"processnode" === 3, 1).otherwise(0) as "adReq",
      when($"iseffective" === "1" && $"isbilling" === "1" && $"isbid" === "1", 1).otherwise(0) as "partReq",
      when($"iseffective" === "1" && $"isbilling" === "1" && $"iswin" === "1" && $"adorderid" != 0, 1).otherwise(0) as "sucBidding",
      when($"requestmode" === 2 && $"iseffective" === "1", 1).otherwise(0) as "display",
      when($"requestmode" === 3 && $"iseffective" === "1", 1).otherwise(0) as "hit",
      when($"iseffective" === "1" && $"isbilling" === "1", $"winprice").otherwise(0) as "DSPAdCons",
      when($"iseffective" === "1" && $"isbilling" === "1", $"adpayment").otherwise(0) as "DSPAdCost"
    )
    //省市
//    val ProCitytarget = markdf.groupBy("provincename", "cityname").agg(sum("srcReq") as "srcReqSum",
//      sum("validReq") as "validReqSum",
//      sum("adReq") as "adReqSum",
//      sum("partReq") as "partReqSum",
//      sum("sucBidding") as "sucBiddingSum",
//      sum("sucBidding").cast("Double")/sum("partReq") as "sucBiddingRate",
//      sum("display") as "displaySum",
//      sum("hit") as "hitSum",
//      sum("hit").cast("Double")/sum("display") as "hitRate",
//      sum("DSPAdCons") / 1000 as "DSPAdConsSum",
//      sum("DSPAdCost") / 1000 as "DSPAdCostSum"
//    )

    //运营
//    markdf.groupBy("ispname").agg(sum("srcReq") as "srcReqSum",
//      sum("validReq") as "validReqSum",
//      sum("adReq") as "adReqSum",
//      sum("partReq") as "partReqSum",
//      sum("sucBidding") as "sucBiddingSum",
//      sum("sucBidding").cast("Double")/sum("partReq") as "sucBiddingRate",
//      sum("display") as "displaySum",
//      sum("hit") as "hitSum",
//      sum("hit").cast("Double")/sum("display") as "hitRate",
//      sum("DSPAdCons") / 1000 as "DSPAdConsSum",
//      sum("DSPAdCost") / 1000 as "DSPAdCostSum"
//    ).show()

    //网络
//    markdf.groupBy("networkmannername").agg(sum("srcReq") as "srcReqSum",
//      sum("validReq") as "validReqSum",
//      sum("adReq") as "adReqSum",
//      sum("partReq") as "partReqSum",
//      sum("sucBidding") as "sucBiddingSum",
//      sum("sucBidding").cast("Double")/sum("partReq") as "sucBiddingRate",
//      sum("display") as "displaySum",
//      sum("hit") as "hitSum",
//      sum("hit").cast("Double")/sum("display") as "hitRate",
//      sum("DSPAdCons") / 1000 as "DSPAdConsSum",
//      sum("DSPAdCost") / 1000 as "DSPAdCostSum"
//    ).show()

    //设备类
//    markdf.groupBy("devicetype").agg(sum("srcReq") as "srcReqSum",
//      sum("validReq") as "validReqSum",
//      sum("adReq") as "adReqSum",
//      sum("partReq") as "partReqSum",
//      sum("sucBidding") as "sucBiddingSum",
//      sum("sucBidding").cast("Double")/sum("partReq") as "sucBiddingRate",
//      sum("display") as "displaySum",
//      sum("hit") as "hitSum",
//      sum("hit").cast("Double")/sum("display") as "hitRate",
//      sum("DSPAdCons") / 1000 as "DSPAdConsSum",
//      sum("DSPAdCost") / 1000 as "DSPAdCostSum"
//    ).show()

    //操作系统
//    markdf.groupBy("client").agg(sum("srcReq") as "srcReqSum",
//      sum("validReq") as "validReqSum",
//      sum("adReq") as "adReqSum",
//      sum("partReq") as "partReqSum",
//      sum("sucBidding") as "sucBiddingSum",
//      sum("sucBidding").cast("Double")/sum("partReq") as "sucBiddingRate",
//      sum("display") as "displaySum",
//      sum("hit") as "hitSum",
//      sum("hit").cast("Double")/sum("display") as "hitRate",
//      sum("DSPAdCons") / 1000 as "DSPAdConsSum",
//      sum("DSPAdCost") / 1000 as "DSPAdCostSum"
//    ).show()

    //3.2.3媒体分析
    val dict_src: Dataset[String] = sparkSession.read.textFile(inputDir2)

    val map: Map[String, String] = dict_src.map(line => line.split("\\t", -1)).filter(_.length >= 5).map(x => {
      (x(4), x(1))
    }).collect().toMap

    val broadval: Broadcast[Map[String, String]] = sparkSession.sparkContext.broadcast(map)

    val broad: Map[String, String] = broadval.value

    markdf.groupBy(when(col("appname").equalTo("其他")
      ,broad.getOrElse($"appid".toString(),"未知")).otherwise($"appname") as "appname")
      .agg(sum("srcReq") as "srcReqSum",
      sum("validReq") as "validReqSum",
      sum("adReq") as "adReqSum",
      sum("partReq") as "partReqSum",
      sum("sucBidding") as "sucBiddingSum",
      sum("sucBidding").cast("Double")/sum("partReq") as "sucBiddingRate",
      sum("display") as "displaySum",
      sum("hit") as "hitSum",
      sum("hit").cast("Double")/sum("display") as "hitRate",
      sum("DSPAdCons") / 1000 as "DSPAdConsSum",
      sum("DSPAdCost") / 1000 as "DSPAdCostSum"
    ).show()

  }

}
