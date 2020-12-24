package com._4paradigm.fesql.spark.utils

import java.io.FileNotFoundException
import java.net.URI
import java.nio.file.Paths

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.shell.PathData
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory


object HDFSUtil {

  private val logger = LoggerFactory.getLogger(this.getClass)

  case class DirectoryInfo(totalSize: Long, everyFileInfo: Array[(String, Long)])

  val INVALID_PATH: String = "-1"

  val hostFSMap: scala.collection.mutable.Map[String, FileSystem] = scala.collection.mutable.Map()

  val conf = new Configuration()

  def isAllExist(uris: String, sep: String) = {
    val paths = StringUtils.split(uris, sep)
    paths.forall(path => isExist(path))
  }

  def getFileInfo(uri: String) = {
    val fs = getFileSystem(uri)
    val status = fs.listStatus(new Path(uri))
    var totalSize = 0L
    val everyFilesInfo = status.flatMap {
      fileStatus =>
        if(fileStatus.isFile){
          totalSize += fileStatus.getLen
          Some(fileStatus.getPath.getName, fileStatus.getLen)
        }else{
          None
        }
    }
    DirectoryInfo(totalSize, everyFilesInfo)
  }

  def getFileSize(uri: String): String = {
    val fs = getFileSystem(uri)
    try {
      // todo: calculate size of glob path
      val contentSummary = fs.getContentSummary(new Path(uri))
      val fileSize: Long = contentSummary.getLength
      if (fileSize < 1024) {
        "%d B".format(fileSize)
      }
      else if (fileSize < 1024 * 1024) {
        "%.2f KB".format(fileSize / 1024.0)
      }
      else if (fileSize / 1024.0 < 1024 * 1024) {
        "%.2f MB".format(fileSize / 1024.0 / 1024.0)
      }
      else {
        "%.2f GB".format(fileSize / 1024.0 / 1024.0 / 1024.0)
      }
    } catch {
      case fnfe: FileNotFoundException =>
        logger.warn(s"Unsupported calculation size of file $uri, return size [0 B]", fnfe)
        "0 B"
      case iae: IllegalArgumentException =>
        logger.warn(s"Unsupported calculation size of file $uri, return size [0 B]", iae)
        "0 B"
      case e : Throwable => throw new RuntimeException(s"Fail to getFileSize of $uri", e)
    }
  }

  def getFilesSize(uris: String*): String = {
    var fileSize: Long = 0l
    uris.map(
      uri => {
        val fs = getFileSystem(uri)
        try {
          // todo: calculate size of glob path
          val contentSummary = fs.getContentSummary(new Path(uri))
          fileSize += contentSummary.getLength
        } catch {
          case fnfe: FileNotFoundException =>
            logger.warn(s"Unsupported calculation size of file $uri, return size [0 B]", fnfe)
            "0 B"
          case iae: IllegalArgumentException =>
            logger.warn(s"Unsupported calculation size of file $uri, return size [0 B]", iae)
            "0 B"
          case e : Throwable => throw new RuntimeException(s"Fail to getFileSize of $uri", e)
        }
      }
    )
    if (fileSize < 1024) {
      "%d B".format(fileSize)
    }
    else if (fileSize < 1024 * 1024) {
      "%.2f KB".format(fileSize / 1024.0)
    }
    else if (fileSize / 1024.0 < 1024 * 1024) {
      "%.2f MB".format(fileSize / 1024.0 / 1024.0)
    }
    else {
      "%.2f GB".format(fileSize / 1024.0 / 1024.0 / 1024.0)
    }

  }

  def getFileSizeNum(uri: String): Long = {
    val fs = getFileSystem(uri)
    try {
      // todo: calculate size of glob path
      val contentSummary = fs.getContentSummary(new Path(uri))
      val fileSize: Long = contentSummary.getLength
      fileSize
    } catch {
      case fnfe: FileNotFoundException =>
        logger.warn(s"Unsupported calculation size of file $uri, return size [0 B]", fnfe)
        0
      case iae: IllegalArgumentException =>
        logger.warn(s"Unsupported calculation size of file $uri, return size [0 B]", iae)
        0
      case e : Throwable => throw new RuntimeException(s"Fail to getFileSize of $uri", e)
    }
  }

  /**
   *
   * @param uri
   *            如果uri是绝对路径,如"hdfs://qa-hdp00:8020/user",对应的key是"hdfs:qa-hdp00:8020",用来标识一个fs;
   *            如果uri是相对路径,key为"default",对应默认产生的fs;
   * @return
   */
  def getFileSystem(uri: String) = {
    logger.info("Going to get file system from uri:{}", uri)
    val u = new URI(uri)
    val key = u.getScheme match {
      case null => "default"
      case _ => String.valueOf(u.getScheme) + String.valueOf(u.getHost) + String.valueOf(u.getPort)
    }
    if (hostFSMap.keySet.contains(key)){
      logger.info(s"File System has key:${key}, value:${hostFSMap.get(key).toString}")
      hostFSMap(key)
    }
    else {
      val fs = FileSystem.get(u, conf)
      hostFSMap.put(key, fs)
      logger.info(s"File System create key:${key}, value:${fs.toString}")
      fs
    }
  }

  def deleteIfExist(uri: String) = {
    if (null != uri && notSkip(uri) && isExist(uri)) {
      logger.warn(s"File exists $uri which will be deleted first.")
      delete(uri)
    }
  }

  def isExist(uri: String): Boolean = {
    if (null == uri || uri.isEmpty) {
      logger.error(s"Invalid(empty) path $uri")
      return false
    }
    if (notSkip(uri)) {
      val pd = PathData.expandAsGlob(braceWrap(uri), conf)
      return pd != null && pd.nonEmpty && pd(0).stat != null
    }
    true // 如果是一个非法路径,默认是存在的
  }

  def notSkip(uri: String) = !uri.equals(INVALID_PATH)

  def delete(uri: String) = {
    if (notSkip(uri)) {
      val fs = getFileSystem(uri)
      fs.delete(new Path(uri), true)
    }
  }

  def dumpTextWithoutSc(data: String, uri: String) = {
    val fs = getFileSystem(uri)
    // Force to write
    val os = fs.create(new Path(uri), true)
    os.writeBytes(data)
    os.close()
  }

  def braceWrap(path: String) = {
    // todo "hdfs:///a, hdfs:///b"
    val paths = {
      // remove empty path
      val rawPaths = path.split(",").filter(_.size > 0)
      // remove abandunt paths
      val repPaths = scala.collection.mutable.ArrayBuffer.empty[String]
      for (uri <- rawPaths)
        if (!rawPaths.exists(ele => Paths.get(uri).startsWith(Paths.get(ele))
          && 0 != Paths.get(ele).compareTo(Paths.get(uri))))
          repPaths += uri
      repPaths.toArray
    }
    val commonDir = {
      var rawPrefix = FindCommonDirectoryPath(paths.toList)
      if (!path.startsWith("/") && !path.startsWith("hdfs://"))
        rawPrefix
      else {
        while (rawPrefix.endsWith("/"))
          rawPrefix = rawPrefix.stripSuffix("/")
        rawPrefix
      }
    }
    val remaining = {
      var rawSuffix = paths.map(_.stripPrefix(commonDir))
      while (rawSuffix.exists(_.startsWith("/")))
        rawSuffix = rawSuffix.map(_.stripPrefix("/"))
      rawSuffix
    }
    val suffixes = remaining.filter(_.length > 0)
    if (suffixes.length > 0) {
      if (commonDir.length > 0 || path.startsWith("/"))
        commonDir.concat("/").concat("{").concat(suffixes.mkString(",")).concat("}")
      else
        "{".concat(suffixes.mkString(",")).concat("}")
    } else commonDir
  }

  object FindCommonDirectoryPath {
    val test = List(
      "/home/user1/tmp//coverage/test",
      "/home3/user1/tmp//covert/operator",
      "/home2/user1/tmp//coven/members"
    )

    def apply(paths: List[String]): String = {
      def common(a: List[String], b: List[String]): List[String] = (a, b) match {
        case (a :: as, b :: bs) if a equals b => a :: common(as, bs)
        case _ => Nil
      }
      if (paths.length < 2) paths.headOption.getOrElse("")
      else paths.map(_.split("/").toList).reduceLeft(common).mkString("/")
    }
  }
  def getFileSize2(uri:String):Long ={
    val fs = getFileSystem(uri)
    try {
      val contentSummary = fs.getContentSummary(new Path(uri))
      val fileSize: Long = contentSummary.getLength
      fileSize
    } catch{
      case fnfe: FileNotFoundException =>
        logger.warn(s"Unsupported calculation size of file $uri, return size [0 B]", fnfe)
        0
      case iae: IllegalArgumentException =>
        logger.warn(s"Unsupported calculation size of file $uri, return size [0 B]", iae)
        0
      case e : Throwable => throw new RuntimeException(s"Fail to getFileSize of $uri", e)
    }
  }
}


