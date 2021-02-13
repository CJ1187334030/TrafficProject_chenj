package com.atguigu.monitor_status_analysis.jdbc

import java.sql.{Connection, DriverManager}

object JDBCHelper {

  def getMysqlConn():Connection = {

    val username = "root"
    val passwd = "111111"
    val url = "jdbc:mysql://hdp101:3306/traffic"

    DriverManager.getConnection(url,username,passwd)

  }

}
