package test.hgs

import javax.xml.parsers.DocumentBuilderFactory
import org.apache.spark.sql.HGSSparkSession
object Main {
  def main(args: Array[String]): Unit = {
    val session = HGSSparkSession.builder()
      .master("local[*]")
      .enableHGSSupport()
      .getOrCreate()
    session.sql("use xxxx")
    val frame = session.sql("show databases like 'x*'")
    frame.show()
    session.stop()
    //val duilder =  DocumentBuilderFactory.newInstance()



  }

}
