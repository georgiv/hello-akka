package http

import java.sql.{DriverManager, SQLException}

import akka.actor.Actor
import akka.http.scaladsl.model.StatusCodes
import scalikejdbc._

case class User(name: String, email: String)

object User extends SQLSyntaxSupport[User] {
  override val tableName = "user"
  override val connectionPoolName = "mimozaDB"

  def apply(u: ResultName[User])(rs: WrappedResultSet) = {
    new User(rs.string(u.name), rs.string(u.email))
  }
}

case class AddUser(user: User)
case class GetUser(name: String)

object DBWorker {
  def main(args: Array[String]): Unit = {
    val host = "localhost"
    val port = 3306
    val user = "root"
    val password = "m1FuckinMySQL"

    val settings = ConnectionPoolSettings(
      initialSize = 5,
      maxSize = 20,
      connectionTimeoutMillis = 3000L,
      validationQuery = "select 1 from dual",
    )

    ConnectionPool.add("mimozaDB", s"jdbc:mysql://$host:$port/mimoza ", user, password, settings)

    NamedDB("mimozaDB") readOnly { implicit session =>
      val u = User.syntax("u")
      val res = sql"select ${u.result.*} from ${User.as(u)}"
        .map(User(u.resultName)).list.apply()
      res.foreach(u => println(s"user: ${u.name}, email: ${u.email}"))
    }
  }
}

class DBWorker(host: String, port: Integer, user: String, password: String) extends Actor {
  val con = DriverManager.getConnection(s"jdbc:mysql://$host:$port/mimoza ", user, password)

  def receive = {
    case AddUser(u) =>
      try {
        con.createStatement().execute(s"INSERT INTO user (name, email) VALUES ('${u.name}', '${u.email}')")
        sender() ! (StatusCodes.Created, Nil)
      } catch {
        case ex: SQLException => sender() ! (StatusCodes.Conflict, ex)
      }

    case GetUser(n) =>
      val rs = con.createStatement().executeQuery(s"SELECT * FROM user WHERE name = '$n'")
      if (rs.next())
        sender() ! (StatusCodes.OK, User(rs.getString("name"), rs.getString("email")))
      else
        sender() ! (StatusCodes.NotFound, Nil)
  }
}
