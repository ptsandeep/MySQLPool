package mysqlconnector


import java.sql.Connection
import java.sql.DriverManager

import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}
import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool}
//Connector Pool
object MySQLConnectorClientPool {
  //We can create singleton by calling MySQLConnectorClientPool(<pool size>, <jdbc url>)
  def apply(poolSize : Int, url:String):GenericObjectPool[Connection] = {
    //Create Client pool for Connection objects
    val clientPool = new GenericObjectPool[Connection](new MySQLPooledObjectFactory(url))
    //Set Maximum pool size
    clientPool.setMaxTotal(poolSize)
    //Failure Handle mechanism
    sys.addShutdownHook{
      clientPool.close()
    }
    clientPool
  }
}

class MySQLPooledObjectFactory(brokerUrl: String) extends BasePooledObjectFactory[Connection]{
  //Create function to create single connection
  override def create() = {
    Class.forName("com.mysql.jdbc.Driver")
    val client = DriverManager.getConnection(brokerUrl)
    client
  }
  //Wrap the connection in pooled object
  override def wrap(client: Connection): PooledObject[Connection] = new DefaultPooledObject[Connection](client)
  //Validate
  override def validateObject(p: PooledObject[Connection]): Boolean = p.getObject.isValid(10)
  //Distroy functionality
  override def destroyObject(p: PooledObject[Connection]): Unit = {
    p.getObject.close()
  }
}
