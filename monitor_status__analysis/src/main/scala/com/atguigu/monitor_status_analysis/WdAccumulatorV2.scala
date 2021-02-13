package com.atguigu.monitor_status_analysis

import java.util

import org.apache.spark.util.AccumulatorV2

class WdAccumulatorV2 extends AccumulatorV2[String,util.ArrayList[String]]{

  //确定返回结果，最终值
  val arrayList = new util.ArrayList[String]()

  override def isZero: Boolean = arrayList.isEmpty

  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = new WdAccumulatorV2

  override def reset(): Unit = arrayList.clear()

  override def add(v: String): Unit = {
    if (v.contains("c"))
      arrayList.add(v)
  }

  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
    arrayList.addAll(other.value)
  }

  override def value: util.ArrayList[String] = arrayList
}
