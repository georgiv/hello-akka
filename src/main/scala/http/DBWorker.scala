package http

import java.sql.SQLException

import akka.actor.Actor
import akka.http.scaladsl.model.StatusCodes
import io.minio.MinioClient
import redis.RedisClient
import scalikejdbc._
import spray.json.DefaultJsonProtocol._
import spray.json._

case class User(name: String,
                email: String,
                password: String,
                avatar: String,
                created: Long,
                last_login: Long)

object User extends SQLSyntaxSupport[User] {
  implicit object UserJSONFormat extends RootJsonFormat[User] {
    def read(json: JsValue) = {
      val fields = json.asJsObject().fields
      val name = fields.get("name").map(_.convertTo[String]).getOrElse("")
      val email = fields.get("email").map(_.convertTo[String]).getOrElse("")
      val password = fields.get("password").map(_.convertTo[String]).getOrElse("")
      val avatar = fields.get("avatar").map(_.convertTo[String]).getOrElse("")
      val created = fields.get("created").map(_.convertTo[Long]).getOrElse(0L)
      val last_login = fields.get("last_login").map(_.convertTo[Long]).getOrElse(0L)
      User(name, email, password, avatar, created, last_login)
    }

    def write(obj: User) = JsObject("name" -> JsString(obj.name),
                                    "email" -> JsString(obj.email),
                                    "password" -> JsString(obj.password),
                                    "avatar" -> JsString(obj.avatar),
                                    "created" -> JsNumber(obj.created),
                                    "last_login" -> JsNumber(obj.last_login))
  }

  override val tableName = "user"
  override val connectionPoolName = 'mimoza

  def apply(name: String,
            email: String,
            password: String,
            avatar: String,
            created: Long,
            last_login: Long) = new User(name, email, password, avatar, created, last_login)

  def apply(u: ResultName[User])(rs: WrappedResultSet) = {
    new User(rs.string(u.name),
      rs.string(u.email),
      rs.string(u.password),
      rs.string(u.avatar),
      rs.long(u.created),
      rs.long(u.last_login))
  }
}

case class AddUser(user: User)
case class GetUser(name: String)
case class UpdateUser(name: String, user: User)
case class DeleteUser(name: String)

object DBWorker {
  def setup(connectionPoolName: Symbol) = scalikejdbc.config.DBsWithEnv("test").setup(connectionPoolName)

  def apply(connectionPoolName: Symbol, redis: RedisClient) = new DBWorker(connectionPoolName, redis)

  def main(args: Array[String]): Unit = {
//    implicit val system = ActorSystem("users-handler")
//    implicit val materializer = ActorMaterializer()
//
//    scalikejdbc.config.DBsWithEnv("test").setup('mimoza)
//    val dbWorker = system.actorOf(RoundRobinPool(5).props(Props(classOf[DBWorker], 'mimoza, RedisClient())), "db-workers")
//
//    NamedDB('mimoza) readOnly { implicit session =>
//      val us = User.syntax("u")
//      val res = withSQL {
//        select.from(User as us).where.eq(us.name, "cassandra")
//      }.map(User(us.resultName)).single.apply()
//      res match {
//        case Some(x) => println("FOUND")
//        case None => println("NOT FOUND")
//      }
//    }

    val minio = new MinioClient("http://localhost:9001",
                               "AKIAIOSFODNN7EXAMPLE",
                               "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
    println(minio.listBuckets())
    println(minio.makeBucket("dummy"))
    println(minio.listBuckets())
  }
}

class DBWorker(connectionPoolName: Symbol, redis: RedisClient) extends Actor {
  import redis.executionContext

  def receive = {
    case AddUser(u) =>
      try {
        NamedDB(connectionPoolName) autoCommit { implicit session =>
          val uc = User.column
          withSQL {
            insert.into(User).namedValues(uc.name -> u.name,
                                          uc.email -> u.email,
                                          uc.password -> u.password,
                                          uc.avatar -> u.avatar,
                                          uc.created -> u.created,
                                          uc.last_login -> u.last_login)
          }.update.apply()
        }

        redis.hmset(s"user:${u.name}", Map("name" -> u.name,
                                                "email" -> u.email,
                                                "password" -> u.password,
                                                "avatar" -> u.avatar,
                                                "created" -> s"${u.created}",
                                                "last_login" -> s"${u.last_login}"))

        sender() ! (StatusCodes.Created, Nil)
      } catch {
        case ex: SQLException => sender() ! (StatusCodes.Conflict, ex)
      }

    case GetUser(n) =>
      val s = sender()

      redis.exists(s"user:$n").map({
        case true => {
          redis.hgetall(s"user:$n").
            map(m => {
              val u = User(m("name").decodeString("UTF-8"),
                           m("email").decodeString("UTF-8"),
                           m("password").decodeString("UTF-8"),
                           m("avatar").decodeString("UTF-8"),
                           m("created").decodeString("UTF-8").toLong,
                           m("last_login").decodeString("UTF-8").toLong)
              s ! (StatusCodes.OK, u)
            })
        }
        case false => {
          NamedDB(connectionPoolName) readOnly { implicit session =>
            val us = User.syntax("u")
            val res = withSQL {
              select.from(User as us).where.eq(us.name, n)
            }.map(User(us.resultName)).single.apply()
            res match {
              case Some(x) =>
                redis.hmset(s"user:${x.name}", Map("name" -> x.name,
                                                        "email" -> x.email,
                                                        "password" -> x.password,
                                                        "avatar" -> x.avatar,
                                                        "created" -> s"${x.created}",
                                                        "last_login" -> s"${x.last_login}"))
                s ! (StatusCodes.OK, x)
              case None => s ! (StatusCodes.NotFound, None)
            }
          }
        }
      })

    case UpdateUser(n, u) =>
      val s = sender()
      try {
        val existing = NamedDB(connectionPoolName) readOnly { implicit session =>
          val us = User.syntax("u")
          withSQL {
            select.from(User as us).where.eq(us.name, n)
          }.map(User(us.resultName)).single.apply.get
        }

        val changes = u.getClass.getDeclaredFields.filter(f => { f.setAccessible(true)
                                                                 val fv = f.get(u).toString
                                                                 fv == "" || fv == "0"}).map(_ getName)

        changes.foreach(c => { val f = u.getClass.getDeclaredField(c)
                               f.setAccessible(true)
                               f.set(u, f.get(existing)) })

        NamedDB(connectionPoolName) autoCommit { implicit session =>
          val uc = User.column
          withSQL {
            update(User).set(User.column.name -> u.name,
              User.column.email -> u.email,
              User.column.password -> u.password,
              User.column.avatar -> u.avatar,
              User.column.created -> u.created,
              User.column.last_login -> u.last_login).where.eq(uc.name, n)
          }.update.apply()
        }

        redis.exists(s"user:$n").map(if (_) redis.del(s"user:$n"))
        redis.hmset(s"user:${u.name}", Map("name" -> u.name,
                                                "email" -> u.email,
                                                "password" -> u.password,
                                                "avatar" -> u.avatar,
                                                "created" -> s"${u.created}",
                                                "last_login" -> s"${u.last_login}"))

        s ! (StatusCodes.OK, None)
      } catch {
        case ex: SQLException => s ! (StatusCodes.NotFound, ex)
      }

    case DeleteUser(n) =>
      val s = sender()

      try {
        redis.exists(s"user:$n").map(if (_) redis.del(s"user:$n"))

        NamedDB(connectionPoolName) autoCommit { implicit session =>
          withSQL {
            delete.from(User).where.eq(User.column.name, n)
          }.update.apply()
        }

        s ! (StatusCodes.OK, None)
      } catch {
        case ex: SQLException => s ! (StatusCodes.NotFound, ex)
      }
  }
}
