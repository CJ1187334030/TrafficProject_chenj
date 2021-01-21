package com.atguigu.traffic_project.bean

case class MonitorFlowAction(date:String,
                             monitor_id:String,
                             camera_id:String,
                             car:String,
                             action_time:String,
                             speed:String,
                             road_id:String,
                             area_id:String)
