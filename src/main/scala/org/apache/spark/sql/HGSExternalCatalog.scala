package org.apache.spark.sql


import java.net.URI
import java.util.regex.Pattern

import com.sun.xml.internal.stream.writers.XMLWriter
import javax.xml.parsers.{DocumentBuilderFactory, SAXParserFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogFunction, CatalogStatistics, CatalogTable, CatalogTablePartition, ExternalCatalog}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.StructType
import org.w3c.dom.bootstrap.DOMImplementationRegistry
import org.w3c.dom.ls.DOMImplementationLS

class HGSExternalCatalog(conf: SparkConf, hadoopConf: Configuration) extends ExternalCatalog with Logging{
  var hdfsClient = getFileSystem()

  val hiveMetaDir = hadoopConf.get("hive.metastore.warehouse.dir","/user/hive/warehouse/metastore")

  var currentDatabase = CatalogDatabase("default",""
    ,new URI(hiveMetaDir+"/default")
    ,Map.empty[String,String])

  def checkConnect(): Unit ={

  }

  def getFileSystem(): FileSystem = synchronized{
    FileSystem.get(hadoopConf)
  }
 override def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit =
    synchronized {
      val databaseDir = hiveMetaDir+"/"+dbDefinition.name
      val path = new Path(databaseDir)

      if(ignoreIfExists){
        hdfsClient.mkdirs(path)
      }else{
        if(!hdfsClient.exists(path)){
          hdfsClient.mkdirs(path)
        }else{
          logInfo(s"database '${dbDefinition.name}' is already exists!")
          throw new SparkException(s"database '${dbDefinition.name}' is already exists!")
        }

      }
    }

  override def dropDatabase(db: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit =
    synchronized {
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
    val path = new Path(hiveMetaDir+"/"+"db")
      CatalogDatabase(db,"",new URI(path.toString),Map.empty[String,String])
  }

  override def databaseExists(db: String): Boolean =  synchronized {
    val databaseDir = hiveMetaDir+"/"+db
    try{
     val exists = hdfsClient.exists(new Path(databaseDir))
      exists
    }catch{
      case _ => false
    }
  }

  override def listDatabases(): Seq[String] =  synchronized {
    val path = new Path(hiveMetaDir)
    val value = hdfsClient.listStatus(path)
    value.map(_.getPath.getName)
  }

  override def listDatabases(pattern: String): Seq[String] =  synchronized {
    val path = new Path(hiveMetaDir)
    val value = hdfsClient.listStatus(path)
    value.map(_.getPath.getName).filter{
      part=>{
          try{
            part.matches(pattern)
          }catch{
            case _:Exception => false
          }
      }
    }
  }

  override def setCurrentDatabase(db: String): Unit =  synchronized {
    if(hdfsClient.exists(new Path(hiveMetaDir+"/"+db))) {

      currentDatabase = CatalogDatabase(db,""
                ,new URI(hiveMetaDir+"/"+db)
                ,Map.empty[String,String])
    }else{
      throw new SparkException("database not exits!")
    }

  }


  override def createTable(tableDefinition: CatalogTable, ignoreIfExists: Boolean): Unit =
    synchronized {
      val document = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument()
      val root = document.createElement(HGSExternalCatalog.tableRootIdentifier)

      val tableType = document.createElement(HGSExternalCatalog.tableTypeIdentifier)
      root.appendChild(tableType)
      val tablename = document.createElement(HGSExternalCatalog.tableNameIdentifier)
      root.appendChild(tablename)
      val tableDatabase = document.createElement(HGSExternalCatalog.tableDatabaseIdentifier)
      root.appendChild(tableDatabase)
      val storageFormat = document.createElement(HGSExternalCatalog.storageFormatIdentifier)
      root.appendChild(storageFormat)
      val storageOutput = document.createElement(HGSExternalCatalog.outPutIdentifier)
      storageFormat.appendChild(storageOutput)
      storageOutput.appendChild(document.createTextNode(tableDefinition.storage.outputFormat.getOrElse("")))
      val storageInput = document.createElement(HGSExternalCatalog.inputPutIdentifier)
      storageFormat.appendChild(storageInput)
      storageInput.appendChild(document.createTextNode(tableDefinition.storage.inputFormat.getOrElse("")))
      val schema = document.createElement(HGSExternalCatalog.schemaIdentifier)
      root.appendChild(schema)
      val provider = document.createElement(HGSExternalCatalog.propertiesIdentifier)
      root.appendChild(provider)
      val partitionColumn = document.createElement(HGSExternalCatalog.partitionColumnName)
      root.appendChild(partitionColumn)
      val owner = document.createElement(HGSExternalCatalog.ownerIdentifier)
      root.appendChild(owner)
      val createtime = document.createElement(HGSExternalCatalog.createTimeIdentifier)
      root.appendChild(createtime)
      val properties = document.createElement(HGSExternalCatalog.propertiesIdentifier)
      root.appendChild(properties)
      val comment = document.createElement(HGSExternalCatalog.commentIdentifier)
      root.appendChild(comment)


      //root.add(new DefaultAttribute(new QName("tablename"),tableDefinition.identifier.identifier))
      tableType.appendChild(document.createTextNode(tableDefinition.tableType.name))
      tablename.appendChild(document.createTextNode(tableDefinition.identifier.identifier))
      tableDatabase.appendChild(document.createTextNode(tableDefinition.database))
      storageOutput.appendChild(document.createTextNode(tableDefinition.storage.outputFormat.get))
      storageInput.appendChild(document.createTextNode(tableDefinition.storage.inputFormat.get))
      tableDefinition.schema.foreach{
        filed=>{
          val column = document.createElement(HGSExternalCatalog.schemaColumn)
          val columntype = document.createElement(HGSExternalCatalog.columntype)
          columntype.appendChild(document.createTextNode(filed.dataType.simpleString))
          column.appendChild(columntype)

          val columnname = document.createElement(HGSExternalCatalog.columnName)
          columnname.appendChild(document.createTextNode(filed.name))
          column.appendChild(columnname)

          val nullable = document.createElement(HGSExternalCatalog.columnnullable)
          nullable.appendChild(document.createTextNode(filed.nullable.toString))
          column.appendChild(nullable)

          val comment = document.createElement(HGSExternalCatalog.commentIdentifier)
          comment.appendChild(document.createTextNode(filed.getComment().getOrElse("")))
          column.appendChild(comment)

          schema.appendChild(column)

        }
      }
      //columns

      provider.appendChild(document.createTextNode(tableDefinition.provider.getOrElse("")))
      partitionColumn.appendChild(document.createTextNode(tableDefinition.partitionColumnNames.mkString(",")))
      owner.appendChild(document.createTextNode(tableDefinition.owner))
      createtime.appendChild(document.createTextNode(tableDefinition.createTime.toString))
      comment.appendChild(document.createTextNode(tableDefinition.comment.getOrElse("")))

      val dest = hdfsClient.create(
        new Path(hiveMetaDir+"/"+tableDefinition.database+"/"+tableDefinition.identifier.identifier+".xml"),true)
      document.appendChild(root)
      document.setXmlVersion("1.0")
      val registry = DOMImplementationRegistry.newInstance()
      val  impl =
        registry.getDOMImplementation("LS").asInstanceOf[DOMImplementationLS]
      val writer = impl.createLSSerializer

      val str = writer.writeToString(document)
      val buf = str.getBytes
      dest.write(str.getBytes,0,buf.length)
      dest.flush()
      dest.close()

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

object  HGSExternalCatalog{
  val tableRootIdentifier = "table"
  val tableTypeIdentifier = "type"
  val storageFormatIdentifier = "storageType"
  val schemaIdentifier = "schema"
  val schemaColumns = "columns"
  val schemaColumn = "column"
  val columnName = "columnname"
  val columntype = "columntype"
  val columnnullable = "nullable"
  val providerIdentifier = "provider"
  val partitionColumnName = "partitionColumn"
  val ownerIdentifier = "owner"
  val createTimeIdentifier = "createtime"
  val propertiesIdentifier = "properties"
  val propertyIdentifier = "property"
  val commentIdentifier = "comment"
  val tableNameIdentifier = "tablename"
  val tableDatabaseIdentifier = "database"
  val outPutIdentifier = "outputformat"
  val inputPutIdentifier = "inputformat"
}
