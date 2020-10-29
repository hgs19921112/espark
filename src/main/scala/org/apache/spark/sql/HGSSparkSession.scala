package org.apache.spark.sql

import java.io.File
import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.apache.spark.annotation.InterfaceStability
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.sql.HGSStaticSQLConf.OPT_CATALOG_IMPLEMENTATION
import org.apache.spark.sql.internal.{BaseSessionStateBuilder, SQLConf, SessionState, SessionStateBuilder, SharedState, StaticSQLConf}
import org.apache.spark.util.{CallSite, Utils}

import scala.util.control.NonFatal
@InterfaceStability.Stable
class HGSSparkSession(@transient override val sparkContext: SparkContext
                      ,@transient override private[sql] val extensions: SparkSessionExtensions)
                      extends SparkSession(sparkContext) with Logging { self =>
   private  val creationSite: CallSite = Utils.getCallSite()
  /* ----------------------- *
   |  Session-related state  |
   * ----------------------- */

  /**
   * State shared across sessions, including the `SparkContext`, cached data, listener,
   * and a catalog that interacts with external systems.
   *
   * This is internal to Spark and there is no guarantee on interface stability.
   *
   * @since 2.2.0
   */
  @InterfaceStability.Unstable
  @transient
  override lazy val sharedState: SharedState = {
    new HGSSharedState(sparkContext)
  }
  @InterfaceStability.Unstable
  @transient
  override lazy val sessionState: SessionState = {
        val state = HGSSparkSession.instantiateSessionState(
          HGSSparkSession.sessionStateClassName(sparkContext.conf),
          self)
        initialSessionOptions.foreach { case (k, v) => state.conf.setConfString(k, v) }
        state

  }

}

@InterfaceStability.Stable
object HGSSparkSession extends Logging {

  /**
   * Builder for [[SparkSession]].
   */
  @InterfaceStability.Stable
  class Builder extends Logging {

    private[this] val options = new scala.collection.mutable.HashMap[String, String]

    private[this] val extensions = new SparkSessionExtensions

    private[this] var userSuppliedContext: Option[SparkContext] = None

    private[spark] def sparkContext(sparkContext: SparkContext): Builder = synchronized {
      userSuppliedContext = Option(sparkContext)
      this
    }

    /**
     * Sets a name for the application, which will be shown in the Spark web UI.
     * If no application name is set, a randomly generated name will be used.
     *
     * @since 2.0.0
     */
    def appName(name: String): Builder = config("spark.app.name", name)

    /**
     * Sets a config option. Options set using this method are automatically propagated to
     * both `SparkConf` and SparkSession's own configuration.
     *
     * @since 2.0.0
     */
    def config(key: String, value: String): Builder = synchronized {
      options += key -> value
      this
    }

    /**
     * Sets a config option. Options set using this method are automatically propagated to
     * both `SparkConf` and SparkSession's own configuration.
     *
     * @since 2.0.0
     */
    def config(key: String, value: Long): Builder = synchronized {
      options += key -> value.toString
      this
    }

    /**
     * Sets a config option. Options set using this method are automatically propagated to
     * both `SparkConf` and SparkSession's own configuration.
     *
     * @since 2.0.0
     */
    def config(key: String, value: Double): Builder = synchronized {
      options += key -> value.toString
      this
    }

    /**
     * Sets a config option. Options set using this method are automatically propagated to
     * both `SparkConf` and SparkSession's own configuration.
     *
     * @since 2.0.0
     */
    def config(key: String, value: Boolean): Builder = synchronized {
      options += key -> value.toString
      this
    }

    /**
     * Sets a list of config options based on the given `SparkConf`.
     *
     * @since 2.0.0
     */
    def config(conf: SparkConf): Builder = synchronized {
      conf.getAll.foreach { case (k, v) => options += k -> v }
      this
    }

    /**
     * Sets the Spark master URL to connect to, such as "local" to run locally, "local[4]" to
     * run locally with 4 cores, or "spark://master:7077" to run on a Spark standalone cluster.
     *
     * @since 2.0.0
     */
    def master(master: String): Builder = config("spark.master", master)

    /**
     * Enables Hive support, including connectivity to a persistent Hive metastore, support for
     * Hive serdes, and Hive user-defined functions.
     *
     * @since 2.0.0
     */
    def enableHiveSupport(): Builder = synchronized {
      if (hiveClassesArePresent) {
        config(OPT_CATALOG_IMPLEMENTATION.key, "hive")
      } else {
        throw new IllegalArgumentException(
          "Unable to instantiate SparkSession with Hive support because " +
            "Hive classes are not found.")
      }
    }

    def enableHGSSupport(): Builder = synchronized {
      if (hiveClassesArePresent) {
        config(HGSStaticSQLConf.OPT_CATALOG_IMPLEMENTATION.key, "hgs")
      } else {
        throw new IllegalArgumentException(
          "Unable to instantiate SparkSession with HGS support because " +
            "HGS classes are not found.")
      }
    }
    /**
     * Inject extensions into the [[SparkSession]]. This allows a user to add Analyzer rules,
     * Optimizer rules, Planning Strategies or a customized parser.
     *
     * @since 2.2.0
     */
    def withExtensions(f: SparkSessionExtensions => Unit): Builder = synchronized {
      f(extensions)
      this
    }

    /**
     * Gets an existing [[SparkSession]] or, if there is no existing one, creates a new
     * one based on the options set in this builder.
     *
     * This method first checks whether there is a valid thread-local SparkSession,
     * and if yes, return that one. It then checks whether there is a valid global
     * default SparkSession, and if yes, return that one. If no valid global default
     * SparkSession exists, the method creates a new SparkSession and assigns the
     * newly created SparkSession as the global default.
     *
     * In case an existing SparkSession is returned, the non-static config options specified in
     * this builder will be applied to the existing SparkSession.
     *
     * @since 2.0.0
     */
    def getOrCreate(): SparkSession = synchronized {
      assertOnDriver()
      // Get the session from current thread's active session.
      var session = activeThreadSession.get()
      if ((session ne null) && !session.sparkContext.isStopped) {
        applyModifiableSettings(session)
        return session
      }

      // Global synchronization so we will only set the default session once.
      HGSSparkSession.synchronized {
        // If the current thread does not have an active session, get it from the global session.
        session = defaultSession.get()
        if ((session ne null) && !session.sparkContext.isStopped) {
          applyModifiableSettings(session)
          return session
        }

        // No active nor global default session. Create a new one.
        val sparkContext = userSuppliedContext.getOrElse {
          val sparkConf = new SparkConf()
          options.foreach { case (k, v) => sparkConf.set(k, v) }

          // set a random app name if not given.
          if (!sparkConf.contains("spark.app.name")) {
            sparkConf.setAppName(java.util.UUID.randomUUID().toString)
          }

          SparkContext.getOrCreate(sparkConf)
          // Do not update `SparkConf` for existing `SparkContext`, as it's shared by all sessions.
        }

        // Initialize extensions if the user has defined a configurator class.
        val extensionConfOption = sparkContext.conf.get(StaticSQLConf.SPARK_SESSION_EXTENSIONS)
        if (extensionConfOption.isDefined) {
          val extensionConfClassName = extensionConfOption.get
          try {
            val extensionConfClass = Utils.classForName(extensionConfClassName)
            val extensionConf = extensionConfClass.newInstance()
              .asInstanceOf[SparkSessionExtensions => Unit]
            extensionConf(extensions)
          } catch {
            // Ignore the error if we cannot find the class or when the class has the wrong type.
            case e @ (_: ClassCastException |
                      _: ClassNotFoundException |
                      _: NoClassDefFoundError) =>
              logWarning(s"Cannot use $extensionConfClassName to configure session extensions.", e)
          }
        }
        //hive.metastore.warehouse.dir
        {
          sparkContext.hadoopConfiguration.addResource(new File("src/main/hdfs-site.xml").toURI.toURL)
          sparkContext.hadoopConfiguration.addResource(new File("src/main/core-site.xml").toURI.toURL)
          sparkContext.hadoopConfiguration.set("hive.metastore.warehouse.dir","/user/hive/warehouse/metastore")
        }
        session = new HGSSparkSession(sparkContext, extensions)
        options.foreach { case (k, v) => session.initialSessionOptions.put(k, v) }
        setDefaultSession(session)
        setActiveSession(session)

        // Register a successfully instantiated context to the singleton. This should be at the
        // end of the class definition so that the singleton is updated only if there is no
        // exception in the construction of the instance.
        sparkContext.addSparkListener(new SparkListener {
          override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
            defaultSession.set(null)
          }
        })
      }

      return session
    }

    private def applyModifiableSettings(session: SparkSession): Unit = {
      val (staticConfs, otherConfs) =
        options.partition(kv => SQLConf.staticConfKeys.contains(kv._1))

      otherConfs.foreach { case (k, v) => session.sessionState.conf.setConfString(k, v) }

      if (staticConfs.nonEmpty) {
        logWarning("Using an existing SparkSession; the static sql configurations will not take" +
          " effect.")
      }
      if (otherConfs.nonEmpty) {
        logWarning("Using an existing SparkSession; some spark core configurations may not take" +
          " effect.")
      }
    }
  }

  /**
   * Creates a [[SparkSession.Builder]] for constructing a [[SparkSession]].
   *
   * @since 2.0.0
   */
  def builder(): Builder = new Builder

  /**
   * Changes the SparkSession that will be returned in this thread and its children when
   * SparkSession.getOrCreate() is called. This can be used to ensure that a given thread receives
   * a SparkSession with an isolated session, instead of the global (first created) context.
   *
   * @since 2.0.0
   */
  def setActiveSession(session: HGSSparkSession): Unit = {
    activeThreadSession.set(session)
  }

  /**
   * Clears the active SparkSession for current thread. Subsequent calls to getOrCreate will
   * return the first created context instead of a thread-local override.
   *
   * @since 2.0.0
   */
  def clearActiveSession(): Unit = {
    activeThreadSession.remove()
  }

  /**
   * Sets the default SparkSession that is returned by the builder.
   *
   * @since 2.0.0
   */
  def setDefaultSession(session: HGSSparkSession): Unit = {
    defaultSession.set(session)
  }

  /**
   * Clears the default SparkSession that is returned by the builder.
   *
   * @since 2.0.0
   */
  def clearDefaultSession(): Unit = {
    defaultSession.set(null)
  }

  /**
   * Returns the active SparkSession for the current thread, returned by the builder.
   *
   * @note Return None, when calling this function on executors
   *
   * @since 2.2.0
   */
  def getActiveSession: Option[HGSSparkSession] = {
    if (TaskContext.get != null) {
      // Return None when running on executors.
      None
    } else {
      Option(activeThreadSession.get)
    }
  }

  /**
   * Returns the default SparkSession that is returned by the builder.
   *
   * @note Return None, when calling this function on executors
   *
   * @since 2.2.0
   */
  def getDefaultSession: Option[HGSSparkSession] = {
    if (TaskContext.get != null) {
      // Return None when running on executors.
      None
    } else {
      Option(defaultSession.get)
    }
  }

  /**
   * Returns the currently active SparkSession, otherwise the default one. If there is no default
   * SparkSession, throws an exception.
   *
   * @since 2.4.0
   */
  def active: SparkSession = {
    getActiveSession.getOrElse(getDefaultSession.getOrElse(
      throw new IllegalStateException("No active or default Spark session found")))
  }

  ////////////////////////////////////////////////////////////////////////////////////////
  // Private methods from now on
  ////////////////////////////////////////////////////////////////////////////////////////

  /** The active SparkSession for the current thread. */
  private val activeThreadSession = new InheritableThreadLocal[HGSSparkSession]

  /** Reference to the root SparkSession. */
  private val defaultSession = new AtomicReference[HGSSparkSession]

  private val HIVE_SESSION_STATE_BUILDER_CLASS_NAME =
    "org.apache.spark.sql.hive.HiveSessionStateBuilder"

  private def sessionStateClassName(conf: SparkConf): String = {
    conf.get(OPT_CATALOG_IMPLEMENTATION) match {
      case "hgs" => HIVE_SESSION_STATE_BUILDER_CLASS_NAME
      //case "hive" => HIVE_SESSION_STATE_BUILDER_CLASS_NAME
      case "in-memory" => classOf[SessionStateBuilder].getCanonicalName
    }
  }

  private def assertOnDriver(): Unit = {
    if (Utils.isTesting && TaskContext.get != null) {
      // we're accessing it during task execution, fail.
      throw new IllegalStateException(
        "SparkSession should only be created and accessed on the driver.")
    }
  }

  /**
   * Helper method to create an instance of `SessionState` based on `className` from conf.
   * The result is either `SessionState` or a Hive based `SessionState`.
   */
  private def instantiateSessionState(
                                       className: String,
                                       sparkSession: SparkSession): SessionState = {
    try {
      // invoke `new [Hive]SessionStateBuilder(SparkSession, Option[SessionState])`
      val clazz = Utils.classForName(className)
      val ctor = clazz.getConstructors.head
      ctor.newInstance(sparkSession, None).asInstanceOf[BaseSessionStateBuilder].build()
    } catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException(s"Error while instantiating '$className':", e)
    }
  }

  /**
   * @return true if Hive classes can be loaded, otherwise false.
   */
  private[spark] def hiveClassesArePresent: Boolean = {
    try {
      Utils.classForName(HIVE_SESSION_STATE_BUILDER_CLASS_NAME)
      Utils.classForName("org.apache.hadoop.hive.conf.HiveConf")
      true
    } catch {
      case _: ClassNotFoundException | _: NoClassDefFoundError => false
    }
  }

  private[spark] def cleanupAnyExistingSession(): Unit = {
    val session = getActiveSession.orElse(getDefaultSession)
    if (session.isDefined) {
      logWarning(
        s"""An existing Spark session exists as the active or default session.
           |This probably means another suite leaked it. Attempting to stop it before continuing.
           |This existing Spark session was created at:
           |
           |${session.get.creationSite.longForm}
           |
         """.stripMargin)
      session.get.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }
}
