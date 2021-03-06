package http

import akka.actor.{ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.{ContentTypes, StatusCodes}
import akka.http.scaladsl.model.headers.{`Access-Control-Allow-Headers`, `Access-Control-Allow-Origin`, Allow, `Content-Type`}
import akka.http.scaladsl.model.HttpMethods.{GET, POST, PATCH, DELETE}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.pattern.ask
import akka.routing.RoundRobinPool
import akka.stream.ActorMaterializer
import akka.util.Timeout
import io.minio.MinioClient
import redis.RedisClient
import spray.json.DefaultJsonProtocol._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.io.StdIn
import scala.util.{Failure, Success}

object UsersHandler {
  def main(args: Array[String]): Unit = {
    val host = "localhost"
    val port = 4000

    implicit val system = ActorSystem("users-handler")
    implicit val materializer = ActorMaterializer()

    implicit val userFormat = jsonFormat6(User.apply)

    implicit val timeout = Timeout(10 seconds)

    import system.dispatcher

    val redisHost = "localhost"
    val redisPort = 6379

    val redis = RedisClient(host = redisHost, port = redisPort)

    val minio = new MinioClient("http://localhost:9000",
                               "Q3AM3UQ867SPQQA43P2F",
                               "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG")

    import User.UserJSONFormat

    val cp = 'mimoza
    DBWorker.setup(cp)

    val dbWorker = system.actorOf(RoundRobinPool(5).props(Props(classOf[DBWorker], cp, redis, minio)), "db-workers")

    val route =
      pathPrefix("users") {
        get {
          path(Segment) { name =>
            respondWithHeaders(`Access-Control-Allow-Origin`.*, `Content-Type`(ContentTypes.`application/json`)) {
              val res = Await.result(dbWorker ? GetUser(name), 10 seconds)
              res match {
                case (StatusCodes.OK, u: User) => complete(StatusCodes.OK -> u)
                case (StatusCodes.NotFound, _) => complete(StatusCodes.NotFound -> s"User $name not registered")
              }
            }
          }
        } ~
        post {
          entity(as[User]) { user =>
            println(user)
            respondWithHeaders(`Access-Control-Allow-Origin`.*) {
              val res = Await.result(dbWorker ? AddUser(user), 10 seconds)
              res match {
                case (StatusCodes.Created, _) => complete(StatusCodes.Created -> s"User ${user.name} persisted")
                case (StatusCodes.Conflict, ex: Exception) => complete(StatusCodes.Conflict -> s"User could not be persisted: ${ex.getMessage}")
              }
            }
          }
        } ~
        patch {
          path(Segment) { name =>
            entity(as[User]) { user =>
              val res = Await.result(dbWorker ? UpdateUser(name, user), 10 seconds)
              res match {
                case (StatusCodes.OK, _) => complete(StatusCodes.OK -> "User updated")
                case (StatusCodes.NotFound, ex: Exception) => complete(StatusCodes.NotFound -> s"User could not be updated: ${ex.getMessage}")
              }
            }
          }
        } ~
        delete {
          path(Segment) { name =>
            val res = Await.result(dbWorker ? DeleteUser(name), 10 seconds)
            res match {
              case (StatusCodes.OK, _) => complete(StatusCodes.OK -> s"User $name deleted")
              case (StatusCodes.NotFound, ex: Exception) => complete(StatusCodes.NotFound -> s"User could not be deleted ${ex.getMessage}")
            }
          }
        } ~
        options {
          respondWithHeaders(`Access-Control-Allow-Headers`("Content-Type"),
                             `Access-Control-Allow-Origin`.*,
                             Allow(GET, POST, PATCH, DELETE)) {
            complete(StatusCodes.OK)
          }
        }
      } ~
      pathPrefix("login") {
        post {
          respondWithHeaders(`Access-Control-Allow-Origin`.*) {
            entity(as[User]) { user =>
              val res = Await.result(dbWorker ? LoginUser(user), 10 seconds)
              res match {
                case (StatusCodes.OK, u: User) => complete(StatusCodes.Created -> u)
                case (StatusCodes.NotFound, _) => complete(StatusCodes.NotFound -> s"Wrong username or password")
              }
            }
          }
        } ~
        options {
          respondWithHeaders(`Access-Control-Allow-Headers`("Content-Type"),
                             `Access-Control-Allow-Origin`.*,
                             Allow(POST)) {
            complete(StatusCodes.OK)
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
