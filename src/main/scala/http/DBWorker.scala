package http

import java.sql.SQLException

import akka.actor.Actor
import akka.http.scaladsl.model.StatusCodes
import redis.RedisClient
import scalikejdbc._
import spray.json.DefaultJsonProtocol._
import spray.json._

case class User(name: String, email: String)

object User extends SQLSyntaxSupport[User] {
  implicit object UserJSONFormat extends RootJsonFormat[User] {
    def read(json: JsValue) = {
      val fields = json.asJsObject().fields
      val name = fields.get("name").map(_.convertTo[String]).getOrElse("")
      val email = fields.get("email").map(_.convertTo[String]).getOrElse("")
      User(name, email)
    }

    def write(obj: User) = JsObject("name" -> JsString(obj.name),
                                    "email" -> JsString(obj.email))
  }

  override val tableName = "user"
  override val connectionPoolName = 'mimoza

  def apply(name: String, email: String) = new User(name, email)

  def apply(u: ResultName[User])(rs: WrappedResultSet) = {
    new User(rs.string(u.name), rs.string(u.email))
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
    User("", "").getClass.getFields.foreach(println)
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
            insert.into(User).namedValues(uc.name -> u.name, uc.email -> u.email)
          }.update.apply()
        }

        redis.hmset(s"user:${u.name}", Map("name" -> u.name, "email" -> u.email))

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
                           m("email").decodeString("UTF-8"))
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
                redis.hmset(s"user:${x.name}", Map("name" -> x.name, "email" -> x.email))
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
                                                                 f.get(u).toString == "" }).map(_ getName)

        changes.foreach(c => { val f = u.getClass.getDeclaredField(c)
                               f.setAccessible(true)
                               f.set(u, f.get(existing)) })

        NamedDB(connectionPoolName) autoCommit { implicit session =>
          val uc = User.column
          withSQL {
            update(User).set(User.column.name -> u.name, User.column.email -> u.email).where.eq(uc.name, n)
          }.update.apply()
        }

        redis.exists(s"user:$n").map(if (_) redis.del(s"user:$n"))
        redis.hmset(s"user:${u.name}", Map("name" -> u.name, "email" -> u.email))

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
