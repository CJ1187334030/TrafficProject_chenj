package com.atguigu.monitor_status_analysis

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.monitor_status_analysis.bean._
import com.atguigu.monitor_status_analysis.jdbc.JDBCHelper
import com.atguigu.monitor_status_analysis.utils.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable.ArrayBuffer
import scala.util.Random


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
    val accumulator: LongAccumulator = spark.sparkContext.longAccumulator

    //cameraInfo 按照monitor分组，所有camera拿出来拼接，做contain判断
    // (0005,"33745,33745,33745,33745")
    val groupByCameraInfo: RDD[(String, Iterable[MonitorCameraInfo])] = cameraInfo.map(x => (x.monitor_id, x)).groupByKey()

    val groupByMonitorFlow: RDD[(String, Iterable[MonitorFlowAction])] = flowAction.map(x => (x.monitor_id, x)).groupByKey()

    //需求一： 0	9	47073	60	0000:37990,60679,75084,51053,34497,03604,93447,07714~~~xxx
    val monitorStateRDD: RDD[MonitorState] = getFinalDate(groupByCameraInfo, groupByMonitorFlow, accumulator,spark)

    //需求二: 车流量最多的topN卡扣
//    val topNFlowMonitor: RDD[(String, Int)] = monitorCarFlowCount(spark,flowAction)

    //需求三：topN卡扣下所有车辆详细信息
//    val topNCarInfo: RDD[MonitorFlowAction] = topNMonitorCardetail(spark,topNFlowMonitor,flowAction)

    //需求四：高速通过topN卡扣  每个卡扣速度最快得10辆车
//    highSpeedMonitor(spark,flowAction)

    //需求五：碰撞分析（区域，卡扣） =》 两区域共同出现的车辆
//    pengCar(flowAction,"01","02")


    //需求六：车辆轨迹 =》 车辆按照时间经过卡扣排序
    //京C49161	, 0001-->0007-->0004-->0003-->0001-->0008-->0007-->0001-->0007-->0004-->0002-->0004-->0005-->0002-->0005-->0006-->0004-->0003-->0001-->0000-->0000-->0002-->0008-->0005-->0007-->0007-->0000
//    val carsTrack: RDD[(String, String)] = carTrack(spark,flowAction)

    //需求七 随机抽取：小时流量/天流量 * 需要抽取车辆数 =》 每小时抽取的数量
//    randomExtract(spark,flowAction)

    //需求八：卡扣流量转化率
//    monitoFlowRate(spark,carsTrack)



  }


  def getFinalDate(groupByCameraInfo: RDD[(String, Iterable[MonitorCameraInfo])], groupByMonitorFlow: RDD[(String, Iterable[MonitorFlowAction])],
                   longaccu: LongAccumulator, spark: SparkSession): RDD[MonitorState] = {


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
          longaccu.add(1L)
        }
    }.count()

    //异常cid数量
    val errorCidCount: Long = mid2ErrorCids.map(x => (x._2))
      .flatMap(_.split("\\|")).count()
    //正常cid数量
    val healthCid: Long = longaccu.value - errorCidCount


    //rdd转list用collect
    val allinfo: String = mid2ErrorCids.map {
      case (a, b) =>
        a + ":" + b
    }.collect().mkString("~~~~")

    val monitorStateRDD: RDD[MonitorState] = spark.sparkContext.makeRDD(List(MonitorState(1, healthMid, healthCid, errorMidCount, errorCidCount, allinfo)))

//    import spark.implicits._
//    monitorStateRDD.toDF().write
//      .format("jdbc")
//      .option("url", "jdbc:mysql://hdp101:3306/traffic")
//      .option("user", "root")
//      .option("password", "111111")
//      .option("dbtable", "monitor_state")
//      .mode(SaveMode.Append)
//      .save()

    monitorStateRDD.foreach(println(_))

    monitorStateRDD

  }




  def monitoFlowRate(spark: SparkSession, carsTrack: RDD[(String, String)]): Unit = {

    val carsTrackCount: RDD[(String, Int)] = carsTrack.flatMap {

      case (car, trace) =>

        val strings: Array[String] = trace.split("-->")
        val tuplesCount: Array[(String, Int)] = strings.slice(0, strings.length - 1).zip(strings.tail).map {
          case (a, b) => a + "_" + b
        }.map(x => (x, 1))

        tuplesCount
    }

    carsTrackCount.reduceByKey(_+_).sortBy(_._2,false).foreach(println(_))

  }




  def randomExtract(spark: SparkSession, flowAction: RDD[MonitorFlowAction]) = {

    //output：(06,2732)
    //reduceByKey(_+_) 按照key 执行 _+_操作
    val hour2Count: RDD[(String, Int)] = flowAction.map(x =>(x.action_time.substring(11,13),1)).reduceByKey(_+_)
    val flowCount: Long = flowAction.count()

    //output：(06,4)
    val hourExtract: RDD[(String, Int)] = hour2Count.map(x => (x._1,((x._2/flowCount.toDouble) * 100).toInt))

    //获取筛选的index [0,n)
    val extractIndex: RDD[(String, util.ArrayList[Int])] = hour2Count.join(hourExtract).map {
      case (hour, (cnt, extCnt)) =>

        val ints = new util.ArrayList[Int]()
        val random = new Random()

        for (i <- 0 until extCnt) {
          val i: Int = random.nextInt(cnt)
          if (!ints.contains(i))
            ints.add(i)
        }

        (hour, ints)
    }


    val extractRDD: RDD[MonitorFlowAction] = flowAction.map(x => (x.action_time.substring(11, 13), x)).groupByKey().join(extractIndex).flatMap {

      case (hour, (iter, arrayListIndex)) =>
        var index: Int = 0

        val flowActions = new ArrayBuffer[MonitorFlowAction]()

        for (elem <- iter) {
          if (arrayListIndex.contains(index)) {
            flowActions.+=(elem)
          }
          index += 1
        }

        flowActions
    }

    import spark.implicits._
    extractRDD.map(
      x => ExtractRDD("1",x.car,x.action_time,x.action_time)
    ).toDF().write
      .format("jdbc")
      .option("url", "jdbc:mysql://hdp101:3306/traffic")
      .option("user", "root")
      .option("password", "111111")
      .option("dbtable", "random_extract_car")
      .mode(SaveMode.Append)
      .save()

  }



  def carTrack(spark: SparkSession, flowAction: RDD[MonitorFlowAction]) = {

    //思路： （车辆_卡扣，action_time） 排序
    val carTrace: RDD[(String, String)] = flowAction.map(x => (x.car +"_"+ x.monitor_id,x.action_time)).sortBy(_._2)

    val car2Mid: RDD[(String, String)] = carTrace.map {
      case (car_mid, actime) =>
        val strings: Array[String] = car_mid.split("_")
        val car: String = strings(0)
        val mid: String = strings(1)
        (car, mid)
    }

    val carsTrace: RDD[(String, String)] = car2Mid.groupByKey().map {

      case (car, iter) =>

        val buffer = new StringBuffer("")

        for (elem <- iter) {
          buffer.append(elem + "-->")
        }

        (car, buffer.toString.substring(0,buffer.toString.length - 3))

    }

//    import spark.implicits._
//    carsTrace.map(x => CarTrace("1","2018-11-05",x._1,x._2))
//      .toDF().write
//      .format("jdbc")
//      .option("url", "jdbc:mysql://hdp101:3306/traffic")
//      .option("user", "root")
//      .option("password", "111111")
//      .option("dbtable", "car_track")
//      .mode(SaveMode.Append)
//      .save()

    carsTrace

  }


  def pengCar(flowAction: RDD[MonitorFlowAction], str: String, str1: String): Unit = {
    val area01Car: RDD[String] = flowAction.filter(x => x.area_id.equals("01")).map(_.car).distinct()
    val area02Car: RDD[String] = flowAction.filter(x => x.area_id.equals("02")).map(_.car).distinct()

    //interselction替代join 避免shuffle
    val pengCars: RDD[String] = area01Car.intersection(area02Car)

    pengCars.foreach(println(_))

    pengCars

  }

  def highSpeedMonitor(spark: SparkSession, flowAction: RDD[MonitorFlowAction]): Unit = {

    val monitor2Speed: RDD[(String, String)] = flowAction.map(x => (x.monitor_id, x.speed))

    //获取最后的值排序
    //output =》 (SortKey(4745,756,1665),0008)
    val sortKeyspd: RDD[(SortKey, String)] = monitor2Speed.groupByKey().map {
      case (mid, iter) =>

        var highSpeed = 0L
        var middleSpeed = 0L
        var lowSpeed = 0L

        for (elem <- iter) {
          if (elem.toLong > 90)
            highSpeed += 1
          else if (elem.toLong > 60 && elem.toLong <= 90)
            middleSpeed += 1
          else
            lowSpeed += 1
        }

        val sortKey = new SortKey(highSpeed, middleSpeed, lowSpeed)

        (sortKey, mid)

    }

    //获取高速通过前三卡扣
    val top3highMonitor: Array[String] = sortKeyspd.sortByKey(false).take(3).map(_._2)

    val top10monitorCars: RDD[TopNMonitorCarSpd] = flowAction.filter(x => top3highMonitor.contains(x.monitor_id)).map(x => (x.monitor_id, x)).groupByKey().flatMap {
      case (mid, iter) =>
        val list: List[MonitorFlowAction] = iter.toList.sortBy(_.speed.toLong).reverse.take(10)

        list.map(x => TopNMonitorCarSpd("1", x.date, x.monitor_id, x.camera_id, x.car, x.action_time, x.speed, x.road_id))
    }

    import spark.implicits._
    top10monitorCars.toDF().write
      .format("jdbc")
      .option("url", "jdbc:mysql://hdp101:3306/traffic")
      .option("user", "root")
      .option("password", "111111")
      .option("dbtable", "top10_speed_detail")
      .mode(SaveMode.Append)
      .save()

    top10monitorCars

  }




  def topNMonitorCardetail(spark: SparkSession, topNFlowMonitor: RDD[(String, Int)], flowAction: RDD[MonitorFlowAction]) = {

    val strings: Array[String] = topNFlowMonitor.take(3).map(_._1)

    val topNCarInfos: RDD[MonitorFlowAction] = flowAction.map(x => (x.monitor_id,x)).filter(x => strings.contains(x._1)).map(_._2)

//    import spark.implicits._
//    topNCarInfos.map{
//      x =>
//        TopNCarInfo("1",x.date,x.monitor_id,x.camera_id,x.car,x.action_time,x.speed,x.road_id)
//    }.toDF().write
//      .format("jdbc")
//          .option("url","jdbc:mysql://hdp101:3306/traffic")
//          .option("user","root")
//          .option("password","111111")
//          .option("dbtable","topn_monitor_detail_info")
//          .mode(SaveMode.Append)
//          .save()


    topNCarInfos

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












