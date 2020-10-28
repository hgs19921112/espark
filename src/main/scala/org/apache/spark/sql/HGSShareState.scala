package org.apache.spark.sql

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogUtils, ExternalCatalog, ExternalCatalogEvent, ExternalCatalogEventListener, ExternalCatalogWithListener, InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.internal.SharedState
import org.apache.spark.sql.HGSStaticSQLConf.OPT_CATALOG_IMPLEMENTATION
import org.apache.spark.util.Utils

import scala.reflect.ClassTag
import scala.util.control.NonFatal

class HGSSharedState(override val sparkContext: SparkContext)  extends SharedState(sparkContext) with Logging{

  lazy override  val externalCatalog :ExternalCatalogWithListener = {

    val externalCatalog = HGSSharedState.reflect[ExternalCatalog, SparkConf, Configuration](
      HGSSharedState.externalCatalogClassName(sparkContext.conf),
      sparkContext.conf,
      sparkContext.hadoopConfiguration)

    val defaultDbDefinition = CatalogDatabase(
      SessionCatalog.DEFAULT_DATABASE,
      "default database",
      CatalogUtils.stringToURI(warehousePath),
      Map())
    // Create default database if it doesn't exist
    if (!externalCatalog.databaseExists(SessionCatalog.DEFAULT_DATABASE)) {
      // There may be another Spark application creating default database at the same time, here we
      // set `ignoreIfExists = true` to avoid `DatabaseAlreadyExists` exception.
      externalCatalog.createDatabase(defaultDbDefinition, ignoreIfExists = true)
    }

    // Wrap to provide catalog events
    val wrapped = new ExternalCatalogWithListener(externalCatalog)

    // Make sure we propagate external catalog events to the spark listener bus
    wrapped.addListener(new ExternalCatalogEventListener {
      override def onEvent(event: ExternalCatalogEvent): Unit = {
        sparkContext.listenerBus.post(event)
      }
    })

    wrapped
  }



}


object HGSSharedState extends Logging{

  /**
   * Helper method to create an instance of [[T]] using a single-arg constructor that
   * accepts an [[Arg1]] and an [[Arg2]].
   */
  private def reflect[T, Arg1 <: AnyRef, Arg2 <: AnyRef](
                    className: String,
                    ctorArg1: Arg1,
                    ctorArg2: Arg2)(
                    implicit ctorArgTag1: ClassTag[Arg1],
                    ctorArgTag2: ClassTag[Arg2]): T = {
    try {
      val clazz = Utils.classForName(className)
      val ctor = clazz.getDeclaredConstructor(ctorArgTag1.runtimeClass, ctorArgTag2.runtimeClass)
      val args = Array[AnyRef](ctorArg1, ctorArg2)
      ctor.newInstance(args: _*).asInstanceOf[T]
    } catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException(s"Error while instantiating '$className':", e)
    }
  }

  private val HIVE_EXTERNAL_CATALOG_CLASS_NAME = "org.apache.spark.sql.hive.HiveExternalCatalog"
  private val HGS_EXTERNAL_CATALOG_CLASS_NAME = "org.apache.spark.sql.HGSExternalCatalog"
  private def externalCatalogClassName(conf: SparkConf): String = {
    conf.get(OPT_CATALOG_IMPLEMENTATION) match {
      case "hgs" => HGS_EXTERNAL_CATALOG_CLASS_NAME
      case "hive" => HIVE_EXTERNAL_CATALOG_CLASS_NAME
      case "in-memory" => classOf[InMemoryCatalog].getCanonicalName
    }
  }
}
