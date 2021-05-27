/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com._4paradigm.hybridsql.spark.utils

import com._4paradigm.hybridsql.spark.SparkFeConfig
import org.slf4j.LoggerFactory

import scala.annotation.StaticAnnotation
import scala.collection.mutable


object ConfigReflections {

  private val logger = LoggerFactory.getLogger(this.getClass)

  private lazy val setters = createSetters()

  private def createSetters(): Map[String, (SparkFeConfig, Any) => Unit] = {
    val universe = scala.reflect.runtime.universe
    val mirror = universe.runtimeMirror(SparkFeConfig.getClass.getClassLoader)
    val configType = universe.typeOf[SparkFeConfig]
    val setters = mutable.HashMap[String, (SparkFeConfig, Any) => Unit]()

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
          setters += name -> { (config: SparkFeConfig, value: Any) =>
            try {
              val im = mirror.reflect(config)
              logger.info("Native Spark Configuration: " + name + " -> " + value)
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

  def createConfig[T](source: T)(func: (T, String) => Any): SparkFeConfig = {
    val config = new SparkFeConfig
    applySetters(source, config)(func)
    config
  }

  def applySetters[T](source: T, config: SparkFeConfig)(func: (T, String) => Any): Unit = {
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
