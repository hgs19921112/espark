package org.apache.spark.sql

import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}


object HGSStaticSQLConf {

  import SQLConf.buildStaticConf


  val OPT_CATALOG_IMPLEMENTATION = buildStaticConf("spark.sql.catalogImplementation.opt")
    .internal()
    .stringConf
    .checkValues(Set("hgs","hive", "in-memory"))
    .createWithDefault("in-memory")

}
