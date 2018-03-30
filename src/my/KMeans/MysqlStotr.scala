package my.KMeans

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
  * Created by 张俊鹏 on 2017/6/4.
  */
class MysqlStotr {
  def mySQLInsert(iterator:Iterator[(Int, String)])={
    val jdbc="com.mysql.jdbc.Driver"
    Class.forName(jdbc)
    var conn: Connection = null
    var ps:PreparedStatement =null
    val sql="insert into clustered(cluster,city) values (?,?)"
    try {
      conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/clusterAnalyze", "root", "strongs")
      iterator.foreach(data => {
        ps = conn.prepareStatement(sql)
        ps.setInt(1, data._1)
        ps.setString(2, data._2)
        ps.executeUpdate()
      })
      println("存储成功！")
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }
}
