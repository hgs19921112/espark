package test.hgs

import org.apache.spark.sql.HGSSparkSession
object Main {
  def main(args: Array[String]): Unit = {
    val session = HGSSparkSession.builder().master("local[*]").enableHGSSupport().getOrCreate()
       val frame = session.sql("show databases")
    frame.show()
    session.stop()
    //  val url = Thread.currentThread().getContextClassLoader.getResource("/core-site.xml")
    //val file = new File("src/main/core-site.xml")
   // val conf = new HdfsConfiguration
    //conf.addResource(file.toURI.toURL)

    //val fis = FileSystem.get(conf)
   // val bool = fis.exists(new Path("/user"))

    //println (bool)
  }

}
