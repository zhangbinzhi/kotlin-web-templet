package com.lealone.kotlin.http


import com.lealone.harbor.extend.getParam
import com.lealone.harbor.extend.json
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.http.HttpMethod
import io.vertx.core.http.HttpServer
import io.vertx.core.http.HttpServerOptions
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.*
import io.vertx.ext.web.sstore.LocalSessionStore
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.awaitResult

class HttpServerVerticle : CoroutineVerticle() {
    override suspend fun start() {
        super.start()
        val router = Router.router(vertx)
        router.route().handler(BodyHandler.create())
        // 允许跨域访问
        router.route().handler(CorsHandler.create("*").allowedMethod(HttpMethod.GET).allowedMethod(HttpMethod.POST))
        //cookie
        router.route().handler(CookieHandler.create())
//        router.route().handler(FaviconHandler.create(Config.favicon))
        router.route().handler(SessionHandler.create(LocalSessionStore.create(vertx)).setSessionCookieName("kotlin-web.session"))
        router.route().handler(UserSessionHandler.create(null)) // 保存登录后的session

        router.route("/*").handler { it -> sayHello(it) }
        // Start the server
        awaitResult<HttpServer> {
            //            HttpServerOptions().setCompressionSupported(true)
            vertx.createHttpServer(HttpServerOptions().setCompressionSupported(true))
                    .requestHandler(router::accept)
                    .listen(8080, it)
        }
    }

    private fun sayHello(ctx: RoutingContext) {
        ctx.json(JsonObject().put("msg", "hello ${ctx.getParam("user")}"))
    }
}