package com.atguigu.monitor_status_analysis

//自定义二次排序
case class SortKey(highSpd:Long,middleSpd:Long,lowSpd:Long) extends Ordered[SortKey] {
  override def compare(that: SortKey): Int = {

    // 值得正负 自动先后
    if(this.highSpd - that.highSpd != 0)
      return (this.highSpd - that.highSpd).toInt
    else if(this.middleSpd - that.middleSpd != 0)
      return (this.middleSpd - that.middleSpd).toInt
    else
    return (this.lowSpd - that.lowSpd).toInt

  }
}
