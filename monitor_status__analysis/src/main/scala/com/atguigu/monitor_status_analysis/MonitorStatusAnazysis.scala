package com.atguigu.monitor_status_analysis

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.monitor_status_analysis.bean.{MonitorCameraInfo, MonitorFlowAction}
import com.atguigu.monitor_status_analysis.jdbc.JDBCHelper
import com.atguigu.monitor_status_analysis.utils.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

case class TaskParam(startDate:String,endDate:String,topNum:String,areaName:String)
case class OriginalRDD(flowAction:RDD[MonitorFlowAction],cameraInfo:RDD[MonitorCameraInfo])



case class MysqlMonitotStatus (task_id:Int, task_name:String, create_time:String, start_time:String, finish_time:String,
                               task_type:String, task_status:String, task_param:String
                              )

case class MonitorState(taskId:Int,abnormal_monitor_count:Int,normal_camera_count:Int
                        ,abnormal_camera_count:Int,abnormal_monitor_camera_infos:String)



object MonitorStatusAnazysis {

  def main(args: Array[String]): Unit = {

    val sc: SparkConf = new SparkConf().setAppName("MonitorStatusAnazysis").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(sc).enableHiveSupport().getOrCreate()
    spark.conf.set("spark.sql.crossJoin.enabled", "true")

    //获取hive原始数据
    val flowAction: RDD[MonitorFlowAction] = getOriginalRDD(sc, spark).flowAction
    val cameraInfo: RDD[MonitorCameraInfo] = getOriginalRDD(sc, spark).cameraInfo

    //sparkContext 注册累加器
    val myAccumulatorV = new MyAccumulatorV2
    spark.sparkContext.register(myAccumulatorV)

    //cameraInfo 按照monitor分组，所有camera拿出来拼接，做contain判断
    // (0005,"33745,33745,33745,33745")
    val groupByCameraInfo: RDD[(String, Iterable[MonitorCameraInfo])] = cameraInfo.map(x => (x.monitor_id, x)).groupByKey()
    //获取(0005,33745|33745|33745|33745)
    val monitor2cameraInfo: RDD[(String, String)] = getgroupByCameraInfo(groupByCameraInfo)


    val groupByMonitorFlow: RDD[(String, Iterable[MonitorFlowAction])] = flowAction.map(x => (x.monitor_id, x)).groupByKey()

    //getJoinData
    val filterDate: RDD[String] = getFinalDate(groupByCameraInfo, groupByMonitorFlow, myAccumulatorV)

    filterDate.foreach(println(_))

    //    myAccumulatorV.value.foreach(println(_))

  }

  def getFinalDate(groupByCameraInfo: RDD[(String, Iterable[MonitorCameraInfo])], groupByMonitorFlow: RDD[(String, Iterable[MonitorFlowAction])],
                   myAccumulatorV: MyAccumulatorV2) = {

    val mid2FlowCid: RDD[(String, String)] = groupByMonitorFlow.map {

      case (mid, iter) => {

        val buffer = new StringBuffer("")

        for (elem <- iter) {
          buffer.append(elem.camera_id).append(",")
        }

        val str: String = StringUtils.trimComma(buffer.toString).replaceAll(",", "\\|")

        (mid, str)

      }
    }

    //拿出异常的 (sid，camera|camera)
    //(0004,50934|21038|30782|90551|28599|88785|81632)
    val mid2ErrorCid: RDD[(String, String)] = mid2FlowCid.join(groupByCameraInfo).map {

      //val tuples = new util.ArrayList[(String,String)]()

      case (mid, (actualCid, iter)) => {

        val buffer = new StringBuffer("")

        for (elem <- iter) {
          if (!actualCid.contains(elem.camera_id)) {
            buffer.append(elem.camera_id).append(",")
          }
        }

        val str: String = StringUtils.trimComma(buffer.toString).replaceAll(",", "\\|")
        (mid, str)

      }
    }


    //异常mid总数，cid总数，合并
    val errorMidCount: Long = mid2ErrorCid.count()

    val errorCidCount: Long = mid2ErrorCid.map(x => (x._2))
      .flatMap(_.split("\\|")).count()

    val buffer: StringBuffer = new StringBuffer("")

    val value: RDD[String] = mid2ErrorCid.map {
      case (mid, cids) =>

        val info: String = mid + cids
        buffer.append(info + "~~~").toString
    }

    //正常mid总数，cid总数
    val healMidCount: Long = groupByCameraInfo.count()

    var healCidCount = 0L

    val value1: RDD[Long] = groupByCameraInfo.map {

      case (mid, iter) => {

        val size: Int = iter.size

        healCidCount += size

        healCidCount
      }
    }


    value

  }


  def getgroupByCameraInfo(groupByCameraInfo: RDD[(String, Iterable[MonitorCameraInfo])]) = {

    groupByCameraInfo.map {

      case (mid, iter) => {

        val monitorId: String = mid
        val cameraIdIter: Iterable[MonitorCameraInfo] = iter

        val stringBuffer = new StringBuffer("")

        for (elem <- cameraIdIter) {
          stringBuffer.append(elem.camera_id).append(",")
        }

        val cameraIdNoComma: String = StringUtils.trimComma(stringBuffer.toString)
        val cameraIdNoComma2: String = cameraIdNoComma.replaceAll(",", "|")

        (monitorId, cameraIdNoComma2)

      }
    }
  }


  //获取原始RDD
  def getOriginalRDD(sc: SparkConf, spark: SparkSession) = {

    import spark.implicits._
    val sqlFlowAction = "select * from traffic.monitor_flow_action"
    val flowActionRDD: RDD[MonitorFlowAction] = spark.sql(sqlFlowAction).as[MonitorFlowAction].rdd

    val sqlCameraInfo = "select * from traffic.monitor_camera_info"
    val cameraInfoRdd: RDD[MonitorCameraInfo] = spark.sql(sqlCameraInfo).as[MonitorCameraInfo].rdd

    OriginalRDD(flowActionRDD, cameraInfoRdd)
  }

  //获取mysql参数
  def getMysqlParam() = {

    //object 和 case class 可以引入 class 无法引入
    val mysqlConn: Connection = JDBCHelper.getMysqlConn()
    val sqlMysql = "SELECT task_param from task where task_id = 1"

    val psm: PreparedStatement = mysqlConn.prepareStatement(sqlMysql)
    val resultSet: ResultSet = psm.executeQuery()

    var task_parm: String = ""

    while (resultSet.next()) {
      task_parm = resultSet.getString(1)
    }

    val taskJSON: JSONObject = JSON.parseObject(task_parm)

    val startDate: String = taskJSON.getString("startDate").substring(2, 12)
    val endDate: String = taskJSON.getString("endDate").substring(2, 12)
    val topNum: String = taskJSON.getString("topNum").substring(2, 3)
    val areaName: String = taskJSON.getString("areaName").substring(2, 5)

    TaskParam(startDate, endDate, topNum, areaName)

  }
}



class MyAccumulatorV2 extends AccumulatorV2[String, mutable.HashMap[String, Int]] {

  val hashMap = new mutable.HashMap[String, Int]()

  override def isZero: Boolean = hashMap.isEmpty

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = new MyAccumulatorV2

  override def reset(): Unit = hashMap.clear()

  override def add(v: String): Unit = {
    if (!hashMap.contains(v))
      hashMap += (v -> 0)

    hashMap.update(v, hashMap(v) + 1)

  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
    other match {
      case acc: MyAccumulatorV2 => {
        acc.hashMap.foldLeft(this.hashMap) {
          case (map, (k, v)) => map += (k -> (map.getOrElse(k, 0) + v))
        }
      }
    }
  }

  override def value: mutable.HashMap[String, Int] = hashMap

}




