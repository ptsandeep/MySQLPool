package sqlconnector

//import org.apache.commons.pool2
import java.sql.Connection
import java.sql.DriverManager

import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}
import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool}

object MySQLConnectorClientPool {
  val poolsize = 8
  val url = "jdbc:mysql://localhost:8080"
  val clientPool = new GenericObjectPool[Connection](new MySQLPooledObjectFactory(url))
  clientPool.setMaxTotal(poolsize)
  sys.addShutdownHook{
    clientPool.close()
  }

  def apply():GenericObjectPool[Connection] = {
    clientPool
  }
}

class MySQLPooledObjectFactory(brokerUrl: String) extends BasePooledObjectFactory[Connection]{

  override def create() = {
    Class.forName("com.mysql.jdbc.Driver")
    val client = DriverManager.getConnection(brokerUrl)
    client
  }

  override def wrap(client: Connection): PooledObject[Connection] = new DefaultPooledObject[Connection](client)

  override def validateObject(p: PooledObject[Connection]): Boolean = p.getObject.isValid(10)

  override def destroyObject(p: PooledObject[Connection]): Unit = {
    p.getObject.close()
  }
}
