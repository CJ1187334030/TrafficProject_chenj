package com.atguigu.monitor_status_analysis

import com.atguigu.monitor_status_analysis.bean.MonitorFlowAction
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}


object RtmRoad {

  def main(args: Array[String]): Unit = {

    val sc: SparkConf = new SparkConf().setAppName("RtmRoad").setMaster("local[*]")
    val ssc = new StreamingContext(sc, Seconds(5))

    val group_id = "test01"
    val topic = "RoadRealTimeLog"
    val zkbrokes = "hdp101:2181,hdp102:2181,hdp103:2181"

    val kafkaDS: DStream[String] = KafkaUtils.createStream(ssc, zkbrokes, group_id, Map(topic -> 3)).map(x => (x._2))

    //output:(0002,30)
    val mid2spdDS: DStream[(String, String)] = kafkaDS
      .filter(x => x.contains("2021-02-13"))
     .map {
      x =>
        val strings: Array[String] = x.split("\t")
        MonitorFlowAction(strings(0), strings(1), strings(2), strings(3), strings(4), strings(5), strings(6), strings(7))
    }
      .map(x => (x.monitor_id, x.speed))


    //output: (0002,30_30_30_30_30_30_30_30)
    val midCar2SpdDS: DStream[(String, String)] = mid2spdDS.reduceByKeyAndWindow((a: String, b: String) => (a +"_"+ b), Minutes(10), Seconds(5))


    val value: DStream[(String, Long)] = midCar2SpdDS.transform {

      rdd =>

        rdd.map {
          case (mid, spds) =>
            val longs: Array[Long] = spds.split("_").map(_.toLong)
            var cnt: Int = longs.length
            val sum: Long = longs.sum

            if (cnt == 0)
              cnt = 1

            val avgSpd: Long = sum / cnt

            (mid, avgSpd)

        }

    }

    value.foreachRDD(_.foreach(println(_)))


    ssc.start()
    ssc.awaitTermination()

  }

}
