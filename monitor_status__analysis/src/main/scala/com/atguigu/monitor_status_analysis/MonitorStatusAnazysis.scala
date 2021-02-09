package com.atguigu.monitor_status_analysis

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.monitor_status_analysis.bean.{MonitorCameraInfo, MonitorFlowAction}
import com.atguigu.monitor_status_analysis.jdbc.JDBCHelper
import com.atguigu.monitor_status_analysis.utils.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

case class TaskParam(startDate:String,endDate:String,topNum:String,areaName:String)
case class OriginalRDD(flowAction:RDD[MonitorFlowAction],cameraInfo:RDD[MonitorCameraInfo])

case class MysqlMonitotStatus (task_id:Int, task_name:String, create_time:String, start_time:String, finish_time:String,
                               task_type:String, task_status:String, task_param:String
                              )

//需求一
case class MonitorState(taskId:Long,noraml_monitor_count:Long,normal_camera_count:Long,abnormal_monitor_count:Long
                        ,abnormal_camera_count:Long,abnormal_monitor_camera_infos:String)

//需求二(字段名称对应)：
case class TopNMonitorFlowCount(task_id:Long,monitor_id:String,carCount:Long)


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

    val groupByMonitorFlow: RDD[(String, Iterable[MonitorFlowAction])] = flowAction.map(x => (x.monitor_id, x)).groupByKey()

    //需求一： 0	9	47073	60	0000:37990,60679,75084,51053,34497,03604,93447,07714~~~xxx
//    val monitorStateRDD: RDD[MonitorState] = getFinalDate(groupByCameraInfo, groupByMonitorFlow, myAccumulatorV,spark)

    //需求二: 车流量最多的topN卡扣
    val topNFlowMonitor: RDD[(String, Int)] = monitorCarFlowCount(spark,flowAction)

    //需求三：topN卡扣下所有车辆详细信息
    topNMonitorCardetail(spark,topNFlowMonitor,flowAction)




  }


  def topNMonitorCardetail(spark: SparkSession, topNFlowMonitor: RDD[(String, Int)], flowAction: RDD[MonitorFlowAction]): Unit = {

    val tuples: Array[(String, Int)] = topNFlowMonitor.take(3)

    flowAction.map(x => (x.monitor_id,x)).groupByKey()


  }


  def monitorCarFlowCount(spark: SparkSession, flowAction: RDD[MonitorFlowAction]) = {

    val mid2CarFlowCount: RDD[(String, Int)] = flowAction.map(x => (x.monitor_id,1)).reduceByKey(_+_).sortBy(_._2,false)
//
//    import spark.implicits._
//    mid2CarFlowCount.map{
//      case(mid,count) =>
//        TopNMonitorFlowCount(1,mid,count)
//    }.toDF().write
//      .format("jdbc")
//      .option("url","jdbc:mysql://hdp101:3306/traffic")
//      .option("user","root")
//      .option("password","111111")
//      .option("dbtable","topn_monitor_car_count")
//      .mode(SaveMode.Append)
//      .save()


    mid2CarFlowCount

  }




  def getFinalDate(groupByCameraInfo: RDD[(String, Iterable[MonitorCameraInfo])], groupByMonitorFlow: RDD[(String, Iterable[MonitorFlowAction])],
                   myAccumulatorV: MyAccumulatorV2,spark: SparkSession) = {

    //获取实际mid2cid(0005,33745|33745|33745|33745)
    val ActionFloMid2Cid: RDD[(String, String)] = groupByMonitorFlow.map {

      case (mid, iter) =>

        val buffer = new StringBuffer("")

        for (elem <- iter) {
          buffer.append(elem.camera_id).append(",")
        }

        val str: String = StringUtils.trimComma(buffer.toString).replaceAll(",", "\\|")

        (mid, str)

    }

    //拿出异常的 (mid，cid1|cid2)
    //逻辑=》拼接实际mid2cid 判断包不包含实际的每一个cid
    //output：(0004,50934|21038|30782|90551|28599|88785|81632)
    val mid2ErrorCids: RDD[(String, String)] = ActionFloMid2Cid.join(groupByCameraInfo).map {

      case (mid, (actualCid, iter)) =>

        val buffer = new StringBuffer("")

        for (elem <- iter) {
          if (!actualCid.contains(elem.camera_id)) {
            buffer.append(elem.camera_id).append(",")
          }
        }

        val str: String = StringUtils.trimComma(buffer.toString).replaceAll(",", "\\|")
        (mid, str)

    }


    //获取所有mid数量
    val allMid: Long = groupByCameraInfo.count()
    //异常mid数量
    val errorMidCount: Long = mid2ErrorCids.count()
    //正常mid数量
    val healthMid: Long = allMid - errorMidCount

    //获取所有cid数量
    groupByCameraInfo.map {
      case (mid, cids) =>
        for (elem <- cids) {
          myAccumulatorV.add(elem.toString)
        }
    }.count()

//    异常cid数量
    val errorCidCount: Long = mid2ErrorCids.map(x => (x._2))
      .flatMap(_.split("\\|")).count()
//    正常cid数量
    val healthCid: Long = myAccumulatorV.value.size() - errorCidCount


    //rdd转list用collect
    val allinfo: String = mid2ErrorCids.map {
      case (a, b) =>
        a + ":" + b
    }.collect().mkString("~~~~")

    val monitorStateRDD: RDD[MonitorState] = spark.sparkContext.makeRDD(List(MonitorState(1, healthMid, healthCid, errorMidCount, errorCidCount, allinfo)))

    import spark.implicits._
    monitorStateRDD.toDF().write
      .format("jdbc")
      .option("url","jdbc:mysql://hdp101:3306/traffic")
      .option("user","root")
      .option("password","111111")
      .option("dbtable","monitor_state")
      .mode(SaveMode.Append)
      .save()

    monitorStateRDD

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
