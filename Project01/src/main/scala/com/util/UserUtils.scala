package com.util

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

import scala.collection.mutable

object UserUtils {
  def getOneUserId(row: Row): String = {
    row match {
      case t if StringUtils.isNotBlank(t.getAs[String]("imei")) => "IM" + t.getAs[String]("imei")
      case t if StringUtils.isNotBlank(t.getAs[String]("mac")) => "MC" + t.getAs[String]("mac")
      case t if StringUtils.isNotBlank(t.getAs[String]("idfa")) => "ID" + t.getAs[String]("idfa")
      case t if StringUtils.isNotBlank(t.getAs[String]("openudid")) => "OD" + t.getAs[String]("openudid")
      case t if StringUtils.isNotBlank(t.getAs[String]("androidid")) => "AD" + t.getAs[String]("androidid")
      case t if StringUtils.isNotBlank(t.getAs[String]("imeimd5")) => "IM" + t.getAs[String]("imeimd5")
      case t if StringUtils.isNotBlank(t.getAs[String]("macmd5")) => "MC" + t.getAs[String]("macmd5")
      case t if StringUtils.isNotBlank(t.getAs[String]("idfamd5")) => "ID" + t.getAs[String]("idfamd5")
      case t if StringUtils.isNotBlank(t.getAs[String]("openudidmd5")) => "OD" + t.getAs[String]("openudidmd5")
      case t if StringUtils.isNotBlank(t.getAs[String]("androididmd5")) => "AD" + t.getAs[String]("androididmd5")
      case t if StringUtils.isNotBlank(t.getAs[String]("imeisha1")) => "IM" + t.getAs[String]("imeisha1")
      case t if StringUtils.isNotBlank(t.getAs[String]("macsha1")) => "MC" + t.getAs[String]("macsha1")
      case t if StringUtils.isNotBlank(t.getAs[String]("idfasha1")) => "ID" + t.getAs[String]("idfasha1")
      case t if StringUtils.isNotBlank(t.getAs[String]("openudidsha1")) => "OD" + t.getAs[String]("openudidsha1")
      case t if StringUtils.isNotBlank(t.getAs[String]("androididsha1")) => "AD" + t.getAs[String]("androididsha1")
      case _ => "其他"
    }

  }

  def getAllUserId(row: Row):mutable.HashMap[String,Int]={
    var map = new mutable.HashMap[String,Int]()

    if (StringUtils.isNotBlank(row.getAs[String]("imei")))
      map("IM" + row.getAs[String]("imei"))=0
    if(StringUtils.isNotBlank(row.getAs[String]("mac")))
      map("MC" + row.getAs[String]("mac"))=0
    if(StringUtils.isNotBlank(row.getAs[String]("idfa")))
      map("ID" + row.getAs[String]("idfa"))=0
    if (StringUtils.isNotBlank(row.getAs[String]("openudid")))
      map("OD" + row.getAs[String]("openudid"))=0
    if (StringUtils.isNotBlank(row.getAs[String]("androidid")))
      map("AD" + row.getAs[String]("androidid"))=0
    if (StringUtils.isNotBlank(row.getAs[String]("imeimd5")))
      map("IM" + row.getAs[String]("imeimd5"))=0
    if (StringUtils.isNotBlank(row.getAs[String]("macmd5")))
      map("MC" + row.getAs[String]("macmd5"))=0
    if (StringUtils.isNotBlank(row.getAs[String]("idfamd5")))
      map("ID" + row.getAs[String]("idfamd5"))=0
    if (StringUtils.isNotBlank(row.getAs[String]("openudidmd5")))
      map("OD" + row.getAs[String]("openudidmd5"))=0
    if (StringUtils.isNotBlank(row.getAs[String]("androididmd5")))
      map("AD" + row.getAs[String]("androididmd5"))=0
    if (StringUtils.isNotBlank(row.getAs[String]("imeisha1")))
      map("IM" + row.getAs[String]("imeisha1"))=0
    if (StringUtils.isNotBlank(row.getAs[String]("macsha1")))
      map("MC" + row.getAs[String]("macsha1"))=0
    if (StringUtils.isNotBlank(row.getAs[String]("idfasha1")))
      map("ID" + row.getAs[String]("idfasha1"))=0
    if (StringUtils.isNotBlank(row.getAs[String]("openudidsha1")))
      map("OD" + row.getAs[String]("openudidsha1"))=0
    if (StringUtils.isNotBlank(row.getAs[String]("androididsha1")))
      map("AD" + row.getAs[String]("androididsha1"))=0
    map

  }
}
