package com.atguigu.monitor_status_analysis

import org.apache.spark.util.AccumulatorV2

//待续写
class ErrorMyAccumulatorV2 extends AccumulatorV2[String,Long] {

  var countt = 0L

  override def isZero: Boolean = countt == 0

  override def copy(): AccumulatorV2[String, Long] = new ErrorMyAccumulatorV2

  override def reset(): Unit = countt = 0

  override def add(v: String): Unit = countt += 1

  override def merge(other: AccumulatorV2[String, Long]): Unit = {
    countt + other.value
  }

  override def value: Long = countt
}
