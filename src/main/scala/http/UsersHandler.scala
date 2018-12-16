package http

import java.sql.SQLException

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import spray.json.DefaultJsonProtocol._

import scala.io.StdIn
import scala.util.{Failure, Success}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._

final case class User(name: String, email: String)

object UsersHandler {
  def main(args: Array[String]): Unit = {
    val host = "localhost"
    val port = 9000

    val dbCon = DBSwitch("localhost", 3306, "root", "m1FuckinMySQL")

    implicit val system = ActorSystem("users-handler")
    implicit val materializer = ActorMaterializer()

    implicit val userFormat = jsonFormat2(User.apply)

    val route =
      pathPrefix("users") {
        get {
          path(Segment) { user =>
            val u = dbCon.getUser(user)
            if (u != null)
              complete(dbCon.getUser(user))
            else
              complete(s"User $user not registered")
          }
        } ~
        post {
          entity(as[User]) { user =>
            try {
              dbCon.addUser(user)
              complete(s"User $user persisted")
            } catch {
              case ex: SQLException => complete(s"User cannot be persisted: ${ex.getMessage}")
            }
          }
        }
      }

    import system.dispatcher

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
