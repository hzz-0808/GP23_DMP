package com.location

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.codehaus.jackson.map.SerializerFactory.Config

object ProCityCt {
  def main(args: Array[String]): Unit = {
    if(args.length!=1){
      println("目录错误")
      sys.exit()
    }

    val Array(inputDir)=args

    val sparkSession: SparkSession = SparkSession.builder()
      .appName("ct")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    import sparkSession.implicits._
    import org.apache.spark.sql.functions._
    val df: DataFrame = sparkSession.read.parquet(inputDir)

    val df2: DataFrame = df.select("provincename", "cityname")
      .groupBy("provincename", "cityname")
      .agg(count("*") as "ct")
//    df2.write.partitionBy("provincename","cityname").json("G:/location")
    val properties = new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","123456")
    df2.write.mode("append")
      .jdbc("jdbc:mysql://192.168.159.149:3306/testdb","ProCityCt",properties)

    sparkSession.stop()

  }

}
