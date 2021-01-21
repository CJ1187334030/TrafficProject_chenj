package com.atguigu.traffic_project

import com.atguigu.traffic_project.bean.{MinitorCameraInfo, MonitorFlowAction}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object MonitorFlowAnalysis {

  def main(args: Array[String]): Unit = {

    val sc: SparkConf = new SparkConf().setAppName("MonitorFlowAnalysis").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(sc).getOrCreate()
    spark.conf.set("spark.sql.crossJoin.enabled", "true")

    val sql = "select * from traffic.monitor_flow_action"
    val sql1 = "select * from traffic.monitor_camera_info"

    import spark.implicits._
    val originFlowAction: RDD[MonitorFlowAction] = spark.sql(sql).as[MonitorFlowAction].rdd
    val originCamareInfo: RDD[MinitorCameraInfo] = spark.sql(sql1).as[MinitorCameraInfo].rdd


  }

  //方式一：直接通过spark sql
  def directSparkSql(spark: SparkSession): Unit ={

    val sql = "select * from traffic.monitor_flow_action"
    spark.sql(sql).createOrReplaceTempView("monitor_flow_action")

    val sql1 = "select * from traffic.monitor_camera_info"
    spark.sql(sql1).createOrReplaceTempView("monitor_camera_info")

    spark.sql(
      """
        |with tt as (
        |select concat_ws('~~~',collect_set(nn)) mm
        |from(
        |select concat(monitor_id,':',concat_ws(',',collect_set(camera_id))) nn
        |from (
        |select ci.monitor_id monitor_id,ci.camera_id camera_id
        |from monitor_camera_info ci
        |left join monitor_flow_action fa on fa.monitor_id = ci.monitor_id and ci.camera_id = fa.camera_id
        |where fa.camera_id is null
        |) t1
        |group by monitor_id
        |) t2
        |)
        |select count(distinct t2.monitor_id) - count(distinct t1.monitor_id) healthy_monitor_id,
        |count(distinct t1.monitor_id) error_monitor_id,
        |count(distinct t2.camera_id) - count(distinct t1.camera_id) healthy_camera_id,
        |count(distinct t1.camera_id) error_camera_id,
        |tt.mm error_camera_info
        |from
        |(
        |select ci.monitor_id monitor_id , ci.camera_id camera_id
        |from monitor_camera_info ci
        |left join monitor_flow_action fa on fa.monitor_id = ci.monitor_id and ci.camera_id = fa.camera_id
        |where fa.camera_id is null
        |) t1,
        |(select monitor_id , camera_id
        |from monitor_camera_info
        |) t2,
        |tt
        |group by tt.mm
      """.stripMargin).show()

  }
}
