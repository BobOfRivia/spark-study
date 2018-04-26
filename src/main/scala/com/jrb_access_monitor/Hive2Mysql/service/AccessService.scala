package com.jrb_access_monitor.Hive2Mysql.service

import com.jrb_access_monitor.Hive2Mysql.util.Constants

class AccessService {

  /**
    *
    * @param pv
    * @param uv
    * @param startd
    * @param endd
    */
  case class accessDetails(pv:BigInt,uv:BigInt,startd:String,endd:String)


  //TODO HIVE查询简单的PV
  def findSomeDayLogs(timeblock:Int,startDate:String,endDate:String): List[accessDetails] ={
    var baseSql=  "select * from hive_log_access "

    if(timeblock==Constants.TIME_BLOCK_DAY){

    }
    Nil
  }


  //TODO 查询简单的UV


}
