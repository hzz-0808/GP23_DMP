package com.util

object RtbUtil {
  def req(requestmode:Int,processnode:Int):List[Double]={
    if(requestmode==1 && processnode==1){
      List(1,0,0)
    }else if(requestmode==1 && processnode==2){
      List(1,1,0)
    }else if(requestmode==1 && processnode==3){
      List(1,1,1)
    }else{
      List(0,0,0)
    }
  }

  def hit(requestmode:Int,iseffective:Int):List[Double]={
    if(requestmode==2 && iseffective==1){
      List(1,0)
    }else if(requestmode==3 && iseffective==1){
      List(0,1)
    }else{
      List(0,0)
    }
  }

  def bidAndCost(iseffective:Int,isbilling:Int,isbid:Int,iswin:Int,adorderid:Int,winprice:Double,adpayment:Double):List[Double]={
    if(iseffective==1 && isbilling==1 && isbid==1){
      if(iseffective==1 && isbilling==1 && iswin==1 && adorderid!=0){
        List(1,1,winprice/1000,adpayment/1000)
      }else{
        List(1,0,0,0)
      }
    }else{
      List(0,0,0,0)
    }
  }

}
