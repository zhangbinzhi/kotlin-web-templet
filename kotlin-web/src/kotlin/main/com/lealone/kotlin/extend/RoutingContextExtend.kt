package com.lealone.harbor.extend

import com.lealone.kotlin.db.getConnectionFromPool
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.RoutingContext
import java.math.BigDecimal
import java.sql.*
import java.sql.Date
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.*

fun RoutingContext.getParam(parameterName: String): String? {
    return this.request().getParam(parameterName)
}

fun RoutingContext.redirection(uri: String) {
    this.response().putHeader("Location", uri).setStatusCode(302).end()
}

fun RoutingContext.json(obj: JsonObject) {
    this.response().putHeader("content-type", "application/json; charset=utf-8").end(obj.toString())
}

fun RoutingContext.sendFail(msg: String) {
    this.json(JsonObject().put("status", "fail").put("msg", msg))
}

fun RoutingContext.sendSuccess(msg: String) {
    this.json(JsonObject().put("status", "success").put("msg", msg))
}

fun RoutingContext.query(sql: String, handler: Handler<AsyncResult<MutableList<JsonObject>>>) {
    var conn: Connection? = null
    var stmt: Statement? = null
    var rs: ResultSet? = null
    try {
        conn = getConnectionFromPool()
//        println(sql)
        stmt = conn.createStatement()
        rs = stmt.executeQuery(sql)
        handler.handle(Future.succeededFuture(asList(rs)))
    } catch (e: Exception) {
        handler.handle(Future.failedFuture(e))
    } finally {
        rs?.close()
        stmt?.close()
        conn?.close()
    }
}

fun RoutingContext.queryWithParams(sql: String, params: JsonArray, handler: Handler<AsyncResult<MutableList<JsonObject>>>) {
    var conn: Connection? = null
    var pStmt: Statement? = null
    var rs: ResultSet? = null
    try {
        conn = getConnectionFromPool()
//        println(cntSql.toString())
        pStmt = conn.prepareStatement(sql)
//        println(sql)
        fillStatement(pStmt, params)
        rs = pStmt.executeQuery()
        handler.handle(Future.succeededFuture(asList(rs)))
    } catch (e: Exception) {
        handler.handle(Future.failedFuture(e))
    } finally {
        rs?.close()
        pStmt?.close()
        conn?.close()
    }
}


fun RoutingContext.updateTable(tableName: String, keyCol: String, cols: MutableList<String>, handler: Handler<AsyncResult<Void>>) {
    updateTable(tableName, keyCol, cols, null, handler)
}


fun RoutingContext.updateTable(tableName: String, keyCol: String, cols: MutableList<String>, extendCol: JsonObject?, handler: Handler<AsyncResult<Void>>) {
    if (cols.size < 1) {
        this.sendFail("主键不能为空")
        handler.handle(Future.failedFuture("cols can not be empty"))
        return
    }
    val key = this.request().getParam(keyCol)
    if (key == null) {
        this.sendFail("主键不能为空")
        handler.handle(Future.failedFuture("keyCol can not be empty"))
    }
    val updateSql = StringBuilder("update $tableName set ")
    val params = JsonArray()
    cols.forEach {
        var col = this.request().getParam(it)
        updateSql.append("$it =?,")
        if (col == null) {
            col = ""
        }
        params.add(col)
    }
    extendCol?.forEach {
        updateSql.append("${it.key}=?,")
        params.add(it.value)
    }

    updateSql.setLength(updateSql.length - 1)
    updateSql.append("where $keyCol=?")
    params.add(key)
//    println(updateSql.toString())
    var con: Connection? = null
    var pStmt: PreparedStatement? = null
    try {
        con = getConnectionFromPool()
        pStmt = con.prepareStatement(updateSql.toString())
        fillStatement(pStmt, params)
        pStmt.executeUpdate()
        this.sendSuccess("操作成功")
        handler.handle(Future.succeededFuture())
    } catch (e: Exception) {
        this.sendFail("操作失败")
        handler.handle(Future.failedFuture(e))
    } finally {
        pStmt?.close()
        con?.close()
    }
}


fun RoutingContext.insertTable(tableName: String, cols: MutableList<String>, handler: Handler<AsyncResult<Void>>) {
    insertTable(tableName, cols, null, handler)
}

fun RoutingContext.insertTable(tableName: String, cols: MutableList<String>, extendCol: JsonObject?, handler: Handler<AsyncResult<Void>>) {
    if (cols.size < 1) {
        this.sendFail("cols can not be empty")
        handler.handle(Future.failedFuture("cols can not be empty"))
        return
    }
    val insertSql = StringBuilder("insert into $tableName (")
    val valueSql = StringBuilder(" values(")
    val params = JsonArray()
    cols.forEach {
        var col = this.request().getParam(it)
        insertSql.append("$it,")
        valueSql.append("?,")
        if (col == null) {
            col = ""
        }
        params.add(col)
    }
    extendCol?.forEach {
        insertSql.append("${it.key},")
        valueSql.append("?,")
        params.add(it.value)
    }

    insertSql.setLength(insertSql.length - 1)
    valueSql.setLength(valueSql.length - 1)
    insertSql.append(")").append(valueSql).append(")")
//    println(insertSql.toString())
    var con: Connection? = null
    var pStmt: PreparedStatement? = null
    try {
        con = getConnectionFromPool()
        pStmt = con.prepareStatement(insertSql.toString())
        fillStatement(pStmt, params)
        pStmt.executeUpdate()
        this.sendSuccess("操作成功")
        handler.handle(Future.succeededFuture())
    } catch (e: Exception) {
        this.sendFail("操作失败")
        handler.handle(Future.failedFuture(e))
    } finally {
        pStmt?.close()
        con?.close()
    }
}

fun RoutingContext.batchSql(sql: String, params: MutableList<JsonArray>, handler: Handler<AsyncResult<Void>>) {
    var conn: Connection? = null
    var pStmt: PreparedStatement? = null

    try {
        conn = getConnectionFromPool()
        pStmt = conn.prepareStatement(sql)
        params.forEach {
            fillStatement(pStmt, it)
            pStmt.addBatch()
        }
        pStmt.executeBatch()
        handler.handle(Future.succeededFuture())
    } catch (e: Exception) {
        handler.handle(Future.failedFuture(e))
    } finally {
        pStmt?.close()
        conn?.close()
    }
}

fun RoutingContext.deleteTable(tableName: String, keyName: String, handler: Handler<AsyncResult<Void>>) {
    val keys = this.request().getParam("keys")
    if (keys == null) {
        this.sendFail("参数不全")
        handler.handle(Future.failedFuture("参数不全，无法删除 $tableName "))
        return
    }
    var con: Connection? = null
    var pStmt: PreparedStatement? = null
    try {
        con = getConnectionFromPool()
        pStmt = con.prepareStatement("delete from $tableName where $keyName = ?")
        if (keys.contains(",")) {
            val ids = keys.split(",")
            ids.forEach {
                fillStatement(pStmt, JsonArray().add(it))
                pStmt.addBatch()
            }
            pStmt.executeBatch()
        } else {
            fillStatement(pStmt, JsonArray().add(keys))
            pStmt.executeUpdate()
        }
        this.sendSuccess("操作成功")
        handler.handle(Future.succeededFuture())
    } catch (e: Exception) {
        this.sendFail("操作失败")
        handler.handle(Future.failedFuture(e))
    } finally {
        pStmt?.close()
        con?.close()
    }

}

fun RoutingContext.endWithPage(queryCol: String, tableName: String, handler: Handler<AsyncResult<Void>>) {
    this.endWithPage(queryCol, tableName, null, null, null, handler)
}

fun RoutingContext.endWithPage(queryCol: String, tableName: String, whereStr: StringBuilder?,
                               orderSql: String?, queryParams: JsonArray?, handler: Handler<AsyncResult<Void>>) {
    var pageIndex = this.request().getFormAttribute("pageIndex")
    val pageRows = this.request().getFormAttribute("pageRows")
    val pageSize = this.request().getFormAttribute("pageSize")
    val querySql = StringBuilder("select ")
    val cntSql = StringBuilder("select count(*) as TOTALROWS from ")
    querySql.append(queryCol).append(" from ").append(tableName)
    cntSql.append(tableName)
    val condtions = this.request().getParam("condtions")
    val where = whereStr ?: StringBuilder()
    val params = queryParams ?: JsonArray()
    try {
        if (condtions != null && condtions.trim { it <= ' ' }.length > 10) {
            val temp = buildSqlFromJsonArray(JsonArray(condtions), params)
            if (temp != null) {
                if (where.isNotEmpty()) {
                    where.append(" and ").append(temp)
                } else {
                    where.append(" where ").append(temp)
                }
            }
        }
    } catch (e: Exception) {
        this.sendFail("illegal conditions")
        handler.handle(Future.failedFuture(e))
        return
    }

    querySql.append(" ").append(where)
    cntSql.append(" ").append(where)

    if (orderSql != null) {
        querySql.append(" ").append(orderSql)
    }
    var page = pageRows ?: pageSize
    if (pageIndex != null && page != null) {
        val rows = Integer.parseInt(page)
        val startLine = (Integer.parseInt(pageIndex) - 1) * rows
        querySql.append(" limit $startLine,$rows")
    } else {
        pageIndex = "1"
    }


    var conn: Connection? = null
    var stmt: PreparedStatement? = null
    var cntStmt: PreparedStatement? = null
    var rs: ResultSet? = null
    try {
        conn = getConnectionFromPool()
//        println(cntSql.toString())
        cntStmt = conn.prepareStatement(cntSql.toString())
        fillStatement(cntStmt, params)
        rs = cntStmt.executeQuery()
        rs.next()
        val CNT = rs.getInt(1)
        rs.close()
        stmt = conn.prepareStatement(querySql.toString())
        fillStatement(stmt, params)
        rs = stmt.executeQuery()
        var totalPages: Int
        if (page == null) {
            page = CNT.toString()
            totalPages = 1
        } else {
            val pageI = Integer.parseInt(page)
            totalPages = CNT / pageI
            if (CNT % pageI > 0) {
                totalPages++
            }
        }
        this.json(getSuccessJson().put("total", CNT)
                .put("items", asList(rs)).put("pageIndex", pageIndex)
                .put("pageRows", page).put("totalPages", totalPages)
                .put("totalRows", CNT))
        handler.handle(Future.succeededFuture())
    } catch (e: Exception) {
        this.sendFail("查询失败：${e.message}")
        handler.handle(Future.failedFuture(e))
    } finally {
        rs?.close()
        cntStmt?.close()
        stmt?.close()
        conn?.close()
    }
}

@Throws(SQLException::class)
private fun asList(rs: ResultSet): MutableList<JsonObject> {
    val columnNames = mutableListOf<String>()
    val metaData = rs.metaData
    val cols = metaData.columnCount

    for (i in 1..cols) {
        columnNames.add(metaData.getColumnLabel(i))
    }
    val results = mutableListOf<JsonObject>()
    while (rs.next()) {
        val result = JsonObject()

        for (i in 0 until columnNames.size) {
            val res = convertSqlValue(rs.getObject(i + 1))
            if (res != null) {
                result.put(columnNames[i], res)
            } else {
                result.put(columnNames[i], "")
            }
        }
        results.add(result)
    }
    return results
}

@Throws(SQLException::class)
private fun convertSqlValue(value: Any?): Any? {
    if (value == null) {
        return null
    } else if (value !is Boolean && value !is String) {
        return when (value) {
            is Number -> if (value is BigDecimal) {
                val d = value as BigDecimal?
                if (d!!.scale() == 0) {
                    value.toBigInteger()
                } else value.toDouble()
            } else {
                value
            }
            is Time -> value.toLocalTime().atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_LOCAL_TIME)
            is Date -> value.toLocalDate().format(DateTimeFormatter.ISO_LOCAL_DATE)
            is Timestamp -> OffsetDateTime.ofInstant(value.toInstant(), ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
            else -> value.toString()
        }
    } else {
        return value
    }
}

@Throws(SQLException::class)
private fun fillStatement(statement: PreparedStatement, params: JsonArray?) {
    if (params == null) {
        return
    }
    for (i in 0 until params.size()) {
        val value = params.getValue(i)
        if (value != null) {
            if (value is String) {
                statement.setString(i + 1, value)
            } else {
                statement.setObject(i + 1, value)
            }
        } else {
            statement.setObject(i + 1, null as Any?)
        }
    }
}


// 用于将请求中的参数转换成sql语句
private fun buildSqlFromJsonArray(condtionJsonArray: JsonArray, paramsJsonArray: JsonArray): StringBuilder? {
    if (condtionJsonArray.size() < 1) {
        return null
    }
    val whereStr = StringBuilder("(")
    var i = 0
    val size = condtionJsonArray.size()
    while (i < size) {
        val condtion = condtionJsonArray.getJsonObject(i)
        val symbol = condtion.getString("symbol")
        if (symbol == null) {
            i++
            continue
        }
        // 第一个条件不作连接
        if (i == 0) {
            condtion.put("combine", " ")
        } else {
            whereStr.append(" and ")
        }
        when {
            symbol.contains("in") -> {
                val values = condtion.getString("value")
                values.replace("，".toRegex(), ",")// 替换掉中文的逗号
                val splitValues = values.split(",".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
                val buff = StringBuilder("(")
                if (condtion.getBoolean("isNum", false)!!) {
                    buff.append(Arrays.toString(splitValues)).append(",")
                } else {
                    for (splitValue in splitValues) {
                        buff.append("'").append(splitValue).append("',")
                    }
                }
                buff.setLength(buff.length - 1)
                buff.append(") ")
                whereStr.append(" ").append(condtion.getString("combine")).append(" ").append(condtion.getString("name"))
                        .append(" ").append(symbol).append(" ").append(buff)

            }
            symbol.contains("like") -> whereStr.append(" ").append(condtion.getString("combine"))
                    .append(" ").append(condtion.getString("name")).append(" ").append(symbol).append(" '%")
                    .append(transactSQLInjection(condtion.getString("value"))).append("%'")
            else -> {
                whereStr.append(" ").append(condtion.getString("combine")).append(" ")
                        .append(condtion.getString("name")).append(" ").append(symbol).append(" ").append("? ")
                paramsJsonArray.add(condtion.getString("value"))
            }
        }
        i++
    }
    return whereStr.append(")")
}

fun transactSQLInjection(str: String): String {
    return str.replace(".*([';]+|(--)+).*".toRegex(), " ")

    // 我认为 应该是return str.replaceAll("([';])+|(--)+","");

}

fun getSuccessJson(): JsonObject {
    return JsonObject().put("status", "success")
}