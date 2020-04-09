package com.stey.index.pipeline.builder

import java.util.Properties

/**
  * @author gaowei
  *         2019-12-05 19:24
  */
class SqlServerDWBuilder(isDW:Boolean) extends Serializable {
  private val prop = new Properties()
  private val DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
  private val USER = "ops"
  private var PASSWORD = "SteyTest2018"
  private var URL = "jdbc:sqlserver://dev-mssql-01.database.chinacloudapi.cn:1433;DatabaseName=dev-stey-01"
  isDW match {
    case true => {
      PASSWORD = "3DdF*a9=7b"
      URL = "jdbc:sqlserver://prod-mssql-dw-01.database.chinacloudapi.cn:1433;DatabaseName=prod-dw-01"
    }
    case false => {
      PASSWORD = "SteyTest2018"
      URL = "jdbc:sqlserver://dev-mssql-01.database.chinacloudapi.cn:1433;DatabaseName=dev-stey-01"
    }
  }

  prop.put("driver", DRIVER)
  prop.put("user", USER)
  prop.put("password", PASSWORD)
  prop.put("url", URL)

  def buildProperties:Properties={
    prop
  }

}
