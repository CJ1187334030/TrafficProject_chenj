package com.atguigu.monitor_status_analysis

import java.util

import org.apache.spark.util.AccumulatorV2

//使用
class MyAccumulatorV2 extends AccumulatorV2[String,util.ArrayList[String]] {

  private val strings = new util.ArrayList[String]

  override def isZero: Boolean = strings.isEmpty

  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = new MyAccumulatorV2

  override def reset(): Unit = strings.clear()

  override def add(v: String): Unit = strings.add(v)

  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = strings addAll(other.value)

  override def value: util.ArrayList[String] = strings
}
