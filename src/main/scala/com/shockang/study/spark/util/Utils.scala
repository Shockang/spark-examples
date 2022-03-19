package com.shockang.study.spark.util

import org.apache.spark.internal.Logging

import java.io.{File, IOException}
import java.nio.file.Files
import java.util.UUID
import scala.util.control.ControlThrowable

/**
 *
 * @author shockang
 */
object Utils extends Logging {
  val MAX_DIR_CREATION_ATTEMPTS: Int = 10

  def formatPrint(s: String): Unit = println(formatStr(s))

  def formatStr(s: String): String = "=" * 10 + " " + s + " " + "=" * 10


  /**
   * 在给定父目录内创建临时目录。
   * 当VM关闭时，该目录将自动删除。
   */
  def createTempDir(
                     root: String = System.getProperty("java.io.tmpdir"),
                     namePrefix: String = "spark"): File = {
    val dir = createDirectory(root, namePrefix)
    ShutdownHookManager.registerShutdownDeleteDir(dir)
    dir
  }

  /**
   * 在给定的父目录中创建一个目录。
   * 该目录保证是新创建的，并且未标记为自动删除。
   */
  def createDirectory(root: String, namePrefix: String = "spark"): File = {
    var attempts = 0
    val maxAttempts = MAX_DIR_CREATION_ATTEMPTS
    var dir: File = null
    while (dir == null) {
      attempts += 1
      if (attempts > maxAttempts) {
        throw new IOException("Failed to create a temp directory (under " + root + ") after " +
          maxAttempts + " attempts!")
      }
      try {
        dir = new File(root, namePrefix + "-" + UUID.randomUUID.toString)
        // SPARK-35907:
        // 如果目录创建失败，这可能会抛出更有意义的异常信息。
        Files.createDirectories(dir.toPath)
      } catch {
        case e@(_: IOException | _: SecurityException) =>
          logError(s"Failed to create directory $dir", e)
          dir = null
      }
    }

    dir.getCanonicalFile
  }

  /**
   * 如果文件/目录存在，就删除它
   *
   * @param filePath 文件/目录 路径
   */
  def deleteIfExists(filePath: String): Unit = {
    val file = new File(filePath)
    if (file.exists()) {
      deleteRecursively(file)
    }
  }

  /**
   * 递归删除文件或目录及其内容。
   * 如果目录是符号链接，则不要跟随它们。
   * 如果删除失败，则引发异常。
   */
  def deleteRecursively(file: File): Unit = {
    if (file != null) {
      JavaUtils.deleteRecursively(file)
      ShutdownHookManager.removeShutdownDeleteDir(file)
    }
  }

  /**
   * 执行给定的块，记录并重新抛出任何未捕获的异常。
   * 这对于包装线程中运行的代码特别有用，以确保打印异常，并避免捕获可丢弃的代码。
   */
  def logUncaughtExceptions[T](f: => T): T = {
    try {
      f
    } catch {
      case ct: ControlThrowable =>
        throw ct
      case t: Throwable =>
        logError(s"Uncaught exception in thread ${Thread.currentThread().getName}", t)
        throw t
    }
  }

  /**
   * 相比 Class.forName(className) 更好的选择
   * Class.forName(className, initialize, loader) 携带当前线程的 ContextClassLoader 也是同理。
   */
  def classForName[C](
                       className: String,
                       initialize: Boolean = true,
                       noSparkClassLoader: Boolean = false): Class[C] = {
    if (!noSparkClassLoader) {
      Class.forName(className, initialize, getContextOrSparkClassLoader).asInstanceOf[Class[C]]
    } else {
      Class.forName(className, initialize, Thread.currentThread().getContextClassLoader).
        asInstanceOf[Class[C]]
    }
  }

  /**
   * 获取此线程上的上下文类加载器，如果不存在，则获取加载Spark的类加载器。
   * 当将类加载器传递给Class.ForName或在设置类加载器委派链时查找当前活动的加载器时，应使用此选项。
   */
  def getContextOrSparkClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getSparkClassLoader)

  /**
   * 获取加载Spark的类加载器。
   */
  def getSparkClassLoader: ClassLoader = getClass.getClassLoader

  def localFsPath(filePath: String): String = {
    "file://" + filePath
  }

  def writableLocalFsPath(filePath: String): String = {
    deleteIfExists(filePath)
    localFsPath(filePath)
  }
}
