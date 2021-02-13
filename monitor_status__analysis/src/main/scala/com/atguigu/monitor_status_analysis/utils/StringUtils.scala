package com.atguigu.monitor_status_analysis.utils

object StringUtils {

  //去掉字符串首尾，
  def trimComma(str:String) ={
    var result:String = ""

    if(str.startsWith(","))
      result = str.substring(1)
    if(str.endsWith(","))
      result = str.substring(0,str.length-1)
    result

  }

}
