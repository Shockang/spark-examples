package com.shockang.study.spark.internal

import org.apache.log4j._
import org.slf4j.bridge.SLF4JBridgeHandler
import org.slf4j.impl.StaticLoggerBinder
import org.slf4j.{Logger, LoggerFactory}

/**
 * 日志的工具特质。参考 Apache Spark 的源码实现： [[org.apache.spark.internal.Logging]]
 */
trait Logging {

  // log 字段设置为 transient
  // 这样实现了 Logging 接口的对象就可以序列化，并且可以在其他机器上使用。
  @transient private var log_ : Logger = null

  // 获得当前对象的日志名称
  protected def logName = {
    // 忽略 Scala 对象名称的 "$" 符号后面的内容
    this.getClass.getName.stripSuffix("$")
  }

  // 得到或者创建当前对象的 logger
  protected def log: Logger = {
    if (log_ == null) {
      initializeLogIfNecessary(false)
      log_ = LoggerFactory.getLogger(logName)
    }
    log_
  }

  // =======================================================================
  // 处理单个字符串的方法
  // =======================================================================

  protected def logInfo(msg: => String): Unit = {
    if (log.isInfoEnabled) log.info(msg)
  }

  protected def logDebug(msg: => String): Unit = {
    if (log.isDebugEnabled) log.debug(msg)
  }

  protected def logTrace(msg: => String): Unit = {
    if (log.isTraceEnabled) log.trace(msg)
  }

  protected def logWarning(msg: => String): Unit = {
    if (log.isWarnEnabled) log.warn(msg)
  }

  protected def logError(msg: => String): Unit = {
    if (log.isErrorEnabled) log.error(msg)
  }

  // =======================================================================
  // 处理字符串和 Throwable 的方法
  // =======================================================================

  protected def logInfo(msg: => String, throwable: Throwable): Unit = {
    if (log.isInfoEnabled) log.info(msg, throwable)
  }

  protected def logDebug(msg: => String, throwable: Throwable): Unit = {
    if (log.isDebugEnabled) log.debug(msg, throwable)
  }

  protected def logTrace(msg: => String, throwable: Throwable): Unit = {
    if (log.isTraceEnabled) log.trace(msg, throwable)
  }

  protected def logWarning(msg: => String, throwable: Throwable): Unit = {
    if (log.isWarnEnabled) log.warn(msg, throwable)
  }

  protected def logError(msg: => String, throwable: Throwable): Unit = {
    if (log.isErrorEnabled) log.error(msg, throwable)
  }

  protected def isTraceEnabled(): Boolean = {
    log.isTraceEnabled
  }

  // =======================================================================
  // 日志对象的初始化
  // =======================================================================

  protected def initializeLogIfNecessary(isInterpreter: Boolean): Unit = {
    initializeLogIfNecessary(isInterpreter, silent = false)
  }

  protected def initializeLogIfNecessary(
                                          isInterpreter: Boolean,
                                          silent: Boolean = false): Boolean = {
    if (!Logging.initialized) {
      Logging.initLock.synchronized {
        if (!Logging.initialized) {
          initializeLogging(isInterpreter, silent)
          return true
        }
      }
    }
    false
  }

  // 测试使用
  private[spark] def initializeForcefully(isInterpreter: Boolean, silent: Boolean): Unit = {
    initializeLogging(isInterpreter, silent)
  }

  private def initializeLogging(isInterpreter: Boolean, silent: Boolean): Unit = {
    // 如果 Log4j 1.2 使用了，但是没有初始化，加载默认的 log4j 配置文件。
    if (Logging.isLog4j12()) {
      val log4j12Initialized = LogManager.getRootLogger.getAllAppenders.hasMoreElements
      // scalastyle:off println
      if (!log4j12Initialized) {
        Logging.defaultLog4jConfig = true
        val defaultLogProps = "conf/log4j-defaults.properties"
        Option(getClass.getClassLoader.getResource(defaultLogProps)) match {
          case Some(url) =>
            PropertyConfigurator.configure(url)
            if (!silent) {
              System.err.println(s"使用默认的 log4j 配置文件: $defaultLogProps")
            }
          case None =>
            System.err.println(s"无法加载配置文件： $defaultLogProps")
        }
        // scalastyle:on println
      }

      val rootLogger = LogManager.getRootLogger()
      if (Logging.defaultRootLevel == null) {
        Logging.defaultRootLevel = rootLogger.getLevel()
      }
      // scalastyle:on println
    }
    Logging.initialized = true

    // 强制调用 slf4j 来初始化，避免通过多线程来触发：
    // http://mailman.qos.ch/pipermail/slf4j-dev/2010-April/002956.html
    log
  }
}

private[spark] object Logging {
  @volatile private var initialized = false
  @volatile private var defaultRootLevel: Level = null
  @volatile private var defaultLog4jConfig = false

  val initLock = new Object()
  try {
    // 如果用户为了使用 JUL 进行日志打印，移除了 slf4j 到 JUL 的桥接方法，
    // 这里我们使用反射来处理这种场景。
    // scalastyle:off classforname
    val bridgeClass = Class.forName("org.slf4j.bridge.SLF4JBridgeHandler",
      true, Thread.currentThread().getContextClassLoader)
      .asInstanceOf[Class[SLF4JBridgeHandler]]
    // scalastyle:on classforname
    bridgeClass.getMethod("removeHandlersForRootLogger").invoke(null)
    val installed = bridgeClass.getMethod("isInstalled").invoke(null).asInstanceOf[Boolean]
    if (!installed) {
      bridgeClass.getMethod("install").invoke(null)
    }
  } catch {
    case e: ClassNotFoundException => // 无法进行日志打印所以直接静默失败
  }

  /**
   * 将日志记录系统标记为未初始化。
   * 这将尽最大努力将日志记录系统重置为其初始状态，以便下一个使用日志记录的类再次触发初始化。
   */
  def uninitialize(): Unit = initLock.synchronized {
    if (isLog4j12()) {
      if (defaultLog4jConfig) {
        defaultLog4jConfig = false
        LogManager.resetConfiguration()
      } else {
        val rootLogger = LogManager.getRootLogger()
        rootLogger.setLevel(defaultRootLevel)
      }
    }
    this.initialized = false
  }

  private def isLog4j12(): Boolean = {
    // 这里将区别绑定的是 log4j 1.2（使用的 org.slf4j.impl.Log4jLoggerFactory）
    // 还是 log4j 2.0（使用的 org.apache.logging.slf4j.Log4jLoggerFactory）
    val binderClass = StaticLoggerBinder.getSingleton.getLoggerFactoryClassStr
    "org.slf4j.impl.Log4jLoggerFactory".equals(binderClass)
  }
}