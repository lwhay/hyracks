package com.yahoo.scalops.util

import java.io._
import java.net.InetAddress
import java.util.concurrent.{Executors, ThreadFactory, ThreadPoolExecutor}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import java.util.{Locale, UUID}

object Utils {
  def serialize[T](o: T): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(o)
    oos.close
    return bos.toByteArray
  }
  
  def deserialize[T](bytes: Array[Byte]): T = {
    return deserialize(bytes, 0, bytes.length)
  }
  
  def deserialize[T](bytes: Array[Byte], idx : Int, length : Int) : T = {
	val bis = new ByteArrayInputStream(bytes, idx, length)
    val ois = new ObjectInputStream(bis)
    return ois.readObject.asInstanceOf[T]
  }
  
  def deserialize[T](bytes: Array[Byte], loader: ClassLoader): T = 
    deserialize(bytes, 0, bytes.length, loader)

  def deserialize[T](bytes: Array[Byte], idx : Int, length : Int, loader: ClassLoader): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis) {
      override def resolveClass(desc: ObjectStreamClass) =
        Class.forName(desc.getName, false, loader)
    }
    return ois.readObject.asInstanceOf[T]
  }

  def isAlpha(c: Char): Boolean = {
    (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
  }

  def splitWords(s: String): Seq[String] = {
    val buf = new ArrayBuffer[String]
    var i = 0
    while (i < s.length) {
      var j = i
      while (j < s.length && isAlpha(s.charAt(j))) {
        j += 1
      }
      if (j > i) {
        buf += s.substring(i, j);
      }
      i = j
      while (i < s.length && !isAlpha(s.charAt(i))) {
        i += 1
      }
    }
    return buf
  }

  /**
   * Get the local host's IP address in dotted-quad format (e.g. 1.2.3.4).
   */
  def localIpAddress(): String = InetAddress.getLocalHost.getHostAddress
  
  /**
   * Returns a standard ThreadFactory except all threads are daemons.
   */
  private def newDaemonThreadFactory: ThreadFactory = {
    new ThreadFactory {
      def newThread(r: Runnable): Thread = {
        var t = Executors.defaultThreadFactory.newThread (r)
        t.setDaemon (true)
        return t
      }
    }
  }

  /**
   * Wrapper over newCachedThreadPool.
   */
  def newDaemonCachedThreadPool(): ThreadPoolExecutor = {
    var threadPool = Executors.newCachedThreadPool.asInstanceOf[ThreadPoolExecutor]

    threadPool.setThreadFactory (newDaemonThreadFactory)

    return threadPool
  }

  /**
   * Wrapper over newFixedThreadPool.
   */
  def newDaemonFixedThreadPool(nThreads: Int): ThreadPoolExecutor = {
    var threadPool = Executors.newFixedThreadPool(nThreads).asInstanceOf[ThreadPoolExecutor]

    threadPool.setThreadFactory(newDaemonThreadFactory)

    return threadPool
  }

  /**
   * Get the local machine's hostname.
   */
  def localHostName(): String = InetAddress.getLocalHost.getHostName

  /**
   * Get current host
   */
  def getHost = System.getProperty("spark.hostname", localHostName())

  /**
   * Delete a file or directory and its contents recursively.
   */
  def deleteRecursively(file: File) {
    if (file.isDirectory) {
      for (child <- file.listFiles()) {
        deleteRecursively(child)
      }
    }
    if (!file.delete()) {
      throw new IOException("Failed to delete: " + file)
    }
  }

  /**
   * Use unit suffixes (Byte, Kilobyte, Megabyte, Gigabyte, Terabyte and
   * Petabyte) in order to reduce the number of digits to four or less. For
   * example, 4,000,000 is returned as 4MB.
   */
  def memoryBytesToString(size: Long): String = {
    val GB = 1L << 30
    val MB = 1L << 20
    val KB = 1L << 10

    val (value, unit) = {
      if (size >= 2*GB) {
        (size.asInstanceOf[Double] / GB, "GB")
      } else if (size >= 2*MB) {
        (size.asInstanceOf[Double] / MB, "MB")
      } else if (size >= 2*KB) {
        (size.asInstanceOf[Double] / KB, "KB")
      } else {
        (size.asInstanceOf[Double], "B")
      }
    }
    "%.1f%s".formatLocal(Locale.US, value, unit)
  }
}
