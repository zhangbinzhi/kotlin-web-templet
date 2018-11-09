package com.lealone.kotlin.db

import com.mchange.v2.c3p0.ComboPooledDataSource
import java.sql.Connection

private var initDb = false
var cpds = ComboPooledDataSource()

@Throws(Exception::class)
fun getConnectionFromPool(): Connection {
    val driver = "com.mysql.jdbc.Driver"
    // URL指向要访问的数据库名scutcs
    val url = ""
    // MySQL配置时的用户名
    val user = ""
    // MySQL配置时的密码
    val password = ""
    if (!initDb) {
        initDb = true
        cpds = ComboPooledDataSource()
        cpds.driverClass = driver
        cpds.jdbcUrl = url
        cpds.user = user
        cpds.password = password
        cpds.acquireIncrement = 1
        cpds.initialPoolSize = 15
        cpds.maxPoolSize = 30
    }
    // 连接数据库
    val conn = cpds.connection
    if (conn.isClosed)
        throw Exception("connection is closed!")
    return conn
}