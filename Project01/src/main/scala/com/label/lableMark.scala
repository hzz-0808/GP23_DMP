package com.label

import com.util.{LabelUtils, UserUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.ivy.util.url.HttpClientHandler.HttpClientHelper
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.HashMap

object lableMark {
  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      println("目录错误")
      sys.exit()
    }

    val HbaseTableName = "Label_Table"


    val Array(inputDir1,inputDir2,inputDir3,day) = args

    val sparkSession: SparkSession = SparkSession.builder()
      .appName("ct")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    //创建hadoop任务
    val configuration: Configuration = sparkSession.sparkContext.hadoopConfiguration

    //创建hbase连接
    configuration.set("hbase.zookeeper.quorum","192.168.159.149:2181,192.168.159.150:2181,192.168.159.151:2181")

    val connection: Connection = ConnectionFactory.createConnection(configuration)

    val admin: Admin = connection.getAdmin

    //判断当前表是否被使用
    if(!admin.tableExists(TableName.valueOf(HbaseTableName))){
      println("当前表可用")
      //创建表对象
      val Htable = new HTableDescriptor(TableName.valueOf(HbaseTableName))
      //创建列簇
      val hColumnDescriptor = new HColumnDescriptor("labels")
      Htable.addFamily(hColumnDescriptor)

      admin.createTable(Htable)

      admin.close()
      connection.close()

    }

    val conf =new  JobConf(configuration)
    //指定输出类型
    conf.setOutputFormat(classOf[TableOutputFormat])

    conf.set(TableOutputFormat.OUTPUT_TABLE,HbaseTableName)

    import sparkSession.implicits._
    val df: DataFrame = sparkSession.read.parquet(inputDir1)
    //df.show()
    val dict_app: Map[String, String] = sparkSession.sparkContext.textFile(inputDir2)
      .map(_.split("\\s", -1))
      .filter(_.length >= 5).map(fields => (fields(4), fields(0))).collect().toMap
    val broadcast: Broadcast[Map[String, String]] = sparkSession.sparkContext.broadcast(dict_app)


    val dict_stopword: Map[String, Int] = sparkSession.sparkContext.textFile(inputDir3).map((_,1)).collect().toMap

    val broadcast1: Broadcast[Map[String, Int]] = sparkSession.sparkContext.broadcast(dict_stopword)


    df.map(row=>{
      val userid: String = UserUtils.getOneUserId(row)
      var map = new HashMap[String,Int]()
      LabelUtils.adspacetypeLabel(row,map)
      LabelUtils.appnameLabel(row,broadcast,map)
      LabelUtils.adplatformproviderLabel(row,map)
      LabelUtils.clientLabel(row,map)
      LabelUtils.networkmannerLabel(row,map)
      LabelUtils.ispnameLabel(row,map)
      LabelUtils.keywordsLabel(row,map,broadcast1)
      LabelUtils.areaLabel(row,map)
      LabelUtils.businessDistrictLabel(row,map)
      (userid,map.toList)
    }).rdd.reduceByKey((list1,list2)=>{
      (list1:::list2).groupBy(_._1).mapValues(list=>{
        list.map(_._2)
      }).mapValues(_.reduce(_+_)).toList
    })
      .map{
      case (userId,userTags) =>{
        val put = new Put(Bytes.toBytes(userId))
        put.addImmutable(Bytes.toBytes("labels"),Bytes.toBytes(day),Bytes.toBytes(userTags.mkString(",")))
        (new ImmutableBytesWritable(),put)
      }
    }.saveAsHadoopDataset(conf)


  }

}
