package http

import akka.actor.{ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.pattern.ask
import akka.routing.RoundRobinPool
import akka.stream.ActorMaterializer
import akka.util.Timeout
import spray.json.DefaultJsonProtocol._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.io.StdIn
import scala.util.{Failure, Success}

object UsersHandler {
  def main(args: Array[String]): Unit = {
    val host = "localhost"
    val port = 9000

    implicit val system = ActorSystem("users-handler")
    implicit val materializer = ActorMaterializer()

    implicit val userFormat = jsonFormat2(User.apply)

    implicit val timeout = Timeout(5 seconds)

    import system.dispatcher

    val cp = 'mimoza
    DBWorker.setup(cp)

    val dbWorker = system.actorOf(RoundRobinPool(5).props(Props(classOf[DBWorker], cp)), "db-workers")

    val route =
      pathPrefix("users") {
        get {
          path(Segment) { user =>
            val res = Await.result(dbWorker ? GetUser(user), 5 seconds)
            res match {
              case (StatusCodes.OK, u: User) => complete(StatusCodes.OK -> u)
              case (StatusCodes.NotFound, _) => complete(StatusCodes.NotFound -> s"User $user not registered")
            }
          }
        } ~
        post {
          entity(as[User]) { user =>
            val res = Await.result(dbWorker ? AddUser(user), 5 seconds)
            res match {
              case (StatusCodes.Created, _) => complete(StatusCodes.Created -> s"User ${user.name} persisted")
              case (StatusCodes.Conflict, ex: Exception) => complete(StatusCodes.Conflict -> s"User could not be persisted: ${ex.getMessage}")
            }
          }
        }
      }

    Http().bindAndHandleAsync(Route.asyncHandler(route), host, port)
      .onComplete {
        case Success(_) =>
          println(s"Users handler runs on $host:$port. Press ENTER to terminate")
          StdIn.readLine()
          system.terminate()
        case Failure(ex) =>
          println(s"Users handler failed to start")
          ex.printStackTrace()
          system.terminate()
      }
  }
}
