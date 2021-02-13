package com.atguigu.monitor_status_analysis.bean

import org.apache.spark.rdd.RDD


//需求一
case class MonitorState(taskId:Long,noraml_monitor_count:Long,normal_camera_count:Long,abnormal_monitor_count:Long
                        ,abnormal_camera_count:Long,abnormal_monitor_camera_infos:String)

//需求二(字段名称对应)：
case class TopNMonitorFlowCount(task_id:Long,monitor_id:String,carCount:Long)

//需求三：
case class TopNCarInfo(task_id:String,
                       date:String,
                       monitor_id:String,
                       camera_id:String,
                       car:String,
                       action_time:String,
                       speed:String,
                       road_id:String)


//需求四：
case class TopNMonitorCarSpd(task_id:String,
                       date:String,
                       monitor_id:String,
                       camera_id:String,
                       car:String,
                       action_time:String,
                       speed:String,
                       road_id:String)

//需求六：
case class CarTrace(task_id:String,
                    date:String,
                    car:String,
                    car_track:String
                   )
//需求七：
case class ExtractRDD(task_id:String,
                    car_info:String,
                    date_d:String,
                    date_hour:String
                   )


case class TaskParam(startDate:String,endDate:String,topNum:String,areaName:String)

case class OriginalRDD(flowAction:RDD[MonitorFlowAction],cameraInfo:RDD[MonitorCameraInfo])

case class MysqlMonitotStatus (task_id:Int, task_name:String, create_time:String, start_time:String, finish_time:String,
                               task_type:String, task_status:String, task_param:String
                              )