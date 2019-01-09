package http

import java.sql.{DriverManager, SQLException}

import akka.actor.Actor
import akka.http.scaladsl.model.StatusCodes
import scalikejdbc._

case class User(name: String, email: String)

object User extends SQLSyntaxSupport[User] {
  override val tableName = "user"
  override val connectionPoolName = 'mimoza

  def apply(u: ResultName[User])(rs: WrappedResultSet) = {
    new User(rs.string(u.name), rs.string(u.email))
  }
}

case class AddUser(user: User)
case class GetUser(name: String)

object DBWorker {
  def setup(connectionPoolName: Symbol) = scalikejdbc.config.DBsWithEnv("test").setup(connectionPoolName)

  def apply(connectionPoolName: Symbol) = new DBWorker(connectionPoolName)

  def main(args: Array[String]): Unit = {
    scalikejdbc.config.DBsWithEnv("test").setup('mimoza)

    val res = NamedDB('mimoza) autoCommit { implicit session =>
      val user = User("venci_man", "venci_man@hotmail.bg")
      val u = User.syntax("u")
      val uc = User.column
      val res = withSQL {
        insert.into(User).namedValues(uc.name -> user.name, uc.email -> user.email)
      }.update.apply()
    }
    println(res)
  }
}

class DBWorker(connectionPoolName: Symbol) extends Actor {
  def receive = {
    case AddUser(u) =>
      try {
        NamedDB(connectionPoolName) autoCommit { implicit session =>
          val us = User.syntax("u")
          val uc = User.column
          val res = withSQL {
            insert.into(User).namedValues(uc.name -> u.name, uc.email -> u.email)
          }.update.apply()
        }
        sender() ! (StatusCodes.Created, Nil)
      } catch {
        case ex: SQLException => sender() ! (StatusCodes.Conflict, ex)
      }

    case GetUser(n) =>
      NamedDB(connectionPoolName) readOnly { implicit session =>
        val us = User.syntax("u")
        val res = withSQL {
          select.from(User as us).where.eq(us.name, n)
        }.map(User(us.resultName)).single.apply()
        res match {
          case Some(x) => sender() ! (StatusCodes.OK, x)
          case None => sender() ! (StatusCodes.NotFound, Nil)
        }
      }
  }
}
