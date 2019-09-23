package com.coreTarget

import com.util.RtbUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object CoreTarget {
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

    val rdd: RDD[Row] = df.rdd

    rdd.map(row=>{
      //REQUESTMODE	PROCESSNODE	ISEFFECTIVE	ISBILLING	ISBID	ISWIN	ADORDEERID
      val provincename: String = row.getAs[String]("provincename")
      val cityname: String = row.getAs[String]("cityname")
      val requestmode: Int = row.getAs[Int]("requestmode")
      val processnode: Int = row.getAs[Int]("processnode")
      val iseffective: Int = row.getAs[Int]("iseffective")
      val isbilling: Int = row.getAs[Int]("isbilling")
      val isbid: Int = row.getAs[Int]("isbid")
      val iswin: Int = row.getAs[Int]("iswin")
      val adorderid: Int = row.getAs[Int]("adorderid")
      //WinPrice adpayment
      val winprice: Double = row.getAs[Double]("winprice")
      val adpayment: Double = row.getAs[Double]("adpayment")

      val reqlist: List[Double] = RtbUtil.req(requestmode,processnode)
      val hitlist: List[Double] = RtbUtil.hit(requestmode,iseffective)
      val bclist: List[Double] = RtbUtil.bidAndCost(iseffective,isbilling,isbid,iswin,adorderid,winprice,adpayment)

      val alllist: List[Double] = reqlist++hitlist++bclist

      ((provincename,cityname),alllist)
    }).reduceByKey((list1,list2)=>{
      list1.zip(list2).map(x=>x._1+x._2)
    }).map(x=>{
      (x._1,x._2.mkString(","))
    }).saveAsTextFile(inputDir2)

  }

}
