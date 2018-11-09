package com.lealone.kotlin

import com.lealone.kotlin.http.HttpServerVerticle

import io.vertx.core.DeploymentOptions
import io.vertx.core.Vertx

fun main(args: Array<String>) {

    val vertx = Vertx.vertx()
    vertx.deployVerticle(HttpServerVerticle::class.java, DeploymentOptions().setInstances(1)) {
        run {
            println("deployVerticle HttpServerVerticle success")
        }
    }
}