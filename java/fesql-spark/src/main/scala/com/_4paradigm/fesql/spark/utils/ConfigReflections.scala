package com._4paradigm.fesql.spark.utils

import com._4paradigm.fesql.spark.FeSQLConfig
import org.slf4j.LoggerFactory

import scala.annotation.StaticAnnotation
import scala.collection.mutable

object ConfigReflections {

  private val logger = LoggerFactory.getLogger(this.getClass)

  private lazy val setters = createSetters()

  private def createSetters(): Map[String, (FeSQLConfig, Any) => Unit] = {
    val universe = scala.reflect.runtime.universe
    val mirror = universe.runtimeMirror(FeSQLConfig.getClass.getClassLoader)
    val configType = universe.typeOf[FeSQLConfig]
    val setters = mutable.HashMap[String, (FeSQLConfig, Any) => Unit]()

    val fields = configType.members.filter(_.asTerm.isVar)
    fields.foreach(symbol => {
      val optionName = symbol.asTerm.name.toString
      val term = configType.decl(universe.TermName(optionName)).asTerm
      val configOption = term.annotations.find(_.tree.tpe == universe.typeOf[ConfigOption]).orNull
      if (configOption != null) {
        val args = configOption.tree.children.tail
        val name = args.headOption.map(_.toString()).map(_.replace("\"", "")).orNull
        // val doc = args.get("doc").map(_.toString()).map(_.replace("\"", "")).orNull
        if (name != null) {
          setters += name -> { (config: FeSQLConfig, value: Any) =>
            try {
              val im = mirror.reflect(config)
              logger.info("FeSQL Configuration: " + name + " -> " + value)
              val typedValue = parseValue(value, term.info.toString)
              if (typedValue != null) {
                im.reflectField(term).set(typedValue)
              }
            } catch {
              case e: Exception => e.printStackTrace()
            }
          }
        }
      }
    })
    setters.toMap
  }

  def createConfig[T](source: T)(func: (T, String) => Any): FeSQLConfig = {
    val config = new FeSQLConfig
    applySetters(source, config)(func)
    config
  }

  def applySetters[T](source: T, config: FeSQLConfig)(func: (T, String) => Any): Unit = {
    for ((name, setter) <- setters) {
      try {
        val value = func(source, name)
        if (value != null) {
          setter.apply(config, value)
        }
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }

  private def parseValue(value: Any, typename: String): Any = {
    typename match {
      case "Int" => value.toString.toInt
      case "Long" => value.toString.toLong
      case "Boolean" => value.toString.toLowerCase() == "true"
      case "Double" => value.toString.toDouble
      case "Float" => value.toString.toFloat
      case "String" => value.toString
      case _ =>
        logger.warn(s"Can not set $typename typed option with value: $value")
    }
  }
}

class ConfigOption(name: String, doc: String = "") extends StaticAnnotation {}
