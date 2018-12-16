package http

import java.sql.{DriverManager, ResultSet}

object DBSwitch {
  def apply(host: String, port: Integer, user: String, password: String): DBSwitch = new DBSwitch(host, port, user, password)

  def main(args: Array[String]): Unit = {
    val con = DBSwitch("localhost", 3306, "root", "m1FuckinMySQL")
    println("Connection successful")

    println(con.getUser("cranki"))
    con.addUser(User("cranki", "cranki2"))
  }
}

class DBSwitch(host: String, port: Integer, user: String, password: String) {
  val con = DriverManager.getConnection(s"jdbc:mysql://$host:$port/mimoza ", user, password)

  def addUser(user: User) =
    con.createStatement().execute(s"INSERT INTO user (name, email) VALUES ('${user.name}', '${user.email}')")

  def getUser(name: String): User = {
    val rs = con.createStatement().executeQuery(s"SELECT * FROM user WHERE name = '$name'")
    if (rs.next()) User(rs.getString("name"), rs.getString("email")) else null
  }
}
