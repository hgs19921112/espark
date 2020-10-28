package org.apache.spark.sql

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogFunction, CatalogStatistics, CatalogTable, CatalogTablePartition, ExternalCatalog}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.StructType

class HGSExternalCatalog(conf: SparkConf, hadoopConf: Configuration) extends ExternalCatalog with Logging{
  def checkConnect(): Unit ={

  }

  def getFileSystem(): FileSystem = synchronized{
    FileSystem.get(hadoopConf)
  }
  var hdfsClient = getFileSystem()

  val hiveMetaDir = hadoopConf.get("hive.metastore.warehouse.dir","/user/hive/warehouse/metastore")
  override def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit =
    synchronized {
      val databaseDir = hiveMetaDir+"/"+dbDefinition.name
      val path = new Path(databaseDir)

      logInfo("xxxxxxxxxxxxxxxxxxxx create database:"+dbDefinition.name)
      if(!ignoreIfExists){
        println("hgsyge=--------------"+databaseDir)
        hdfsClient.mkdirs(path)
      }else{
        if(!hdfsClient.exists(path)){
          hdfsClient.mkdirs(path)
        }else{
          logInfo(s"database '${dbDefinition.name}' is already exists!")
          throw new SparkException("error!")
        }

      }
    }

  override def dropDatabase(db: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit =
    synchronized {
      logInfo("xxxxxxxxxxxxxxxx  drop databaes:"+db)
      val databaseDir = hiveMetaDir+"/"+db
      val path = new Path(databaseDir)
      if(hdfsClient.exists(path)){
        hdfsClient.delete(path,cascade)
      }
  }

  override def alterDatabase(dbDefinition: CatalogDatabase): Unit =  synchronized {
    println("........................")

  }

  override def getDatabase(db: String): CatalogDatabase =  synchronized {
    println("........................")
    null
  }

  override def databaseExists(db: String): Boolean =  synchronized {
    val databaseDir = hiveMetaDir+"/"+db
    logInfo("xxxxxxxxxxxxxxxxxxxxx  is database exists:"+databaseDir)

    try{
     val exists = hdfsClient.exists(new Path(databaseDir))
      logInfo("database xxxxxxxxxxx exists:"+exists)
      exists
    }catch{
      case _ => false
    }
  }

  override def listDatabases(): Seq[String] =  synchronized {
    println("........................")
    null
  }

  override def listDatabases(pattern: String): Seq[String] =  synchronized {
    println("........................")
    null
  }

  override def setCurrentDatabase(db: String): Unit =  synchronized {
    println("........................")

  }

  override def createTable(tableDefinition: CatalogTable, ignoreIfExists: Boolean): Unit =  synchronized {
    println("........................")

  }

  override def dropTable(db: String, table: String, ignoreIfNotExists: Boolean, purge: Boolean): Unit =  synchronized {
    println("........................")

  }

  override def renameTable(db: String, oldName: String, newName: String): Unit =  synchronized {
    println("........................")

  }

  override def alterTable(tableDefinition: CatalogTable): Unit =  synchronized {
    println("........................")

  }

  override def alterTableDataSchema(db: String, table: String, newDataSchema: StructType): Unit =  synchronized {
    println("........................")

  }

  override def alterTableStats(db: String, table: String, stats: Option[CatalogStatistics]): Unit =  synchronized {
    println("........................")

  }

  override def getTable(db: String, table: String): CatalogTable =  synchronized {
    println("........................")
    null
  }

  override def tableExists(db: String, table: String): Boolean =  synchronized {
    println("........................")
    false
  }

  override def listTables(db: String): Seq[String] =  synchronized {
    println("........................")
    null
  }

  override def listTables(db: String, pattern: String): Seq[String] =  synchronized {
    println("........................")
    null
  }

  override def loadTable(db: String, table: String, loadPath: String, isOverwrite: Boolean, isSrcLocal: Boolean): Unit =  synchronized {
    println("........................")

  }

  override def loadPartition(db: String, table: String, loadPath: String, partition: TablePartitionSpec, isOverwrite: Boolean, inheritTableSpecs: Boolean, isSrcLocal: Boolean): Unit =
  {
  }

  override def loadDynamicPartitions(db: String, table: String, loadPath: String, partition: TablePartitionSpec, replace: Boolean, numDP: Int): Unit =
  {
  }

  override def createPartitions(db: String, table: String, parts: Seq[CatalogTablePartition], ignoreIfExists: Boolean): Unit =
  {
  }

  override def dropPartitions(db: String, table: String, parts: Seq[TablePartitionSpec], ignoreIfNotExists: Boolean, purge: Boolean, retainData: Boolean): Unit =
  {

  }

  override def renamePartitions(db: String, table: String, specs: Seq[TablePartitionSpec], newSpecs: Seq[TablePartitionSpec]): Unit =
  {
    null
  }

  override def alterPartitions(db: String, table: String, parts: Seq[CatalogTablePartition]): Unit =
  {
  }

  override def getPartition(db: String, table: String, spec: TablePartitionSpec): CatalogTablePartition =
  {
    null
  }

  override def getPartitionOption(db: String, table: String, spec: TablePartitionSpec): Option[CatalogTablePartition] =
  {
    null
  }

  override def listPartitionNames(db: String, table: String, partialSpec: Option[TablePartitionSpec]): Seq[String] =
  {
    null
  }

  override def listPartitions(db: String, table: String, partialSpec: Option[TablePartitionSpec]): Seq[CatalogTablePartition] =
  {
    null
  }

  override def listPartitionsByFilter(db: String, table: String, predicates: Seq[Expression], defaultTimeZoneId: String): Seq[CatalogTablePartition] =
  {
    null
  }

  override def createFunction(db: String, funcDefinition: CatalogFunction): Unit =
  {
  }

  override def dropFunction(db: String, funcName: String): Unit =
  {
  }

  override def alterFunction(db: String, funcDefinition: CatalogFunction): Unit =
  {
  }

  override def renameFunction(db: String, oldName: String, newName: String): Unit =
  {
  }

  override def getFunction(db: String, funcName: String): CatalogFunction =
  {
    null
  }

  override def functionExists(db: String, funcName: String): Boolean =
  {
    false
  }

  override def listFunctions(db: String, pattern: String): Seq[String] =
  {
    null
  }
}
