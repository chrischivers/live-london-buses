package lbt.web

import cats.effect.{IO, _}
import cats.implicits._
import fs2._
import org.http4s._
import org.http4s.dsl._
import org.http4s.dsl.{Http4sDsl, Root}
import org.http4s.server.blaze.BlazeBuilder
import org.http4s.server.websocket._
import org.http4s.util.StreamApp
import org.http4s.websocket.WebsocketBits._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class WSServlet(implicit F: Effect[IO]) extends StreamApp[IO] with Http4sDsl[IO] {

  def route(scheduler: Scheduler): HttpService[IO] = HttpService[IO] {
    case GET -> Root / "hello" =>
      Ok("Hello world.")

    case GET -> Root / "ws" =>
      val toClient: Stream[IO, WebSocketFrame] =
        scheduler.awakeEvery[IO](1.seconds).map(d => Text(s"Ping! $d"))
      val fromClient: Sink[IO, WebSocketFrame] = _.evalMap { (ws: WebSocketFrame) =>
        ws match {
          case Text(t, _) => F.delay(println(t))
          case f => F.delay(println(s"Unknown type: $f"))
        }
      }
      WS(toClient, fromClient)
  }

  def stream(args: List[String], requestShutdown: IO[Unit]): Stream[IO, Nothing] =
    Scheduler[IO](corePoolSize = 2).flatMap { scheduler =>
      BlazeBuilder[IO]
        .bindHttp(8080)
        .withWebSockets(true)
        .mountService(route(scheduler), "/http4s")
        .serve
    }

}