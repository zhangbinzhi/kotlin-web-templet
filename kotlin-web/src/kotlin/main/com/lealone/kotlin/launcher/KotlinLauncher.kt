package com.lealone.kotlin.launcher

import com.lealone.kotlin.http.HttpServerVerticle
import io.vertx.core.DeploymentOptions
import io.vertx.core.Launcher
import io.vertx.core.Vertx
import io.vertx.core.VertxOptions


class KotlinLauncher : Launcher() {
    override fun beforeStartingVertx(options: VertxOptions?) {
        val option = options ?: VertxOptions()
        option.maxWorkerExecuteTime = Long.MAX_VALUE
        super.beforeStartingVertx(option)
    }

    override fun afterStartingVertx(vertx: Vertx?) {
        if (vertx == null) {
            return
        }
        vertx.deployVerticle(HttpServerVerticle::class.java, DeploymentOptions().setInstances(1)) {
            run {
                println("deployVerticle HttpServerVerticle success")
            }
        }
    }

}