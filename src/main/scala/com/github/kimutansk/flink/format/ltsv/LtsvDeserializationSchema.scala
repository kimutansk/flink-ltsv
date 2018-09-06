/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.kimutansk.flink.format.ltsv

import java.nio.charset.StandardCharsets
import java.sql.{Date, Time, Timestamp}
import java.time.Instant
import java.time.format.DateTimeFormatter

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.types.Row
import org.apache.flink.util.Preconditions

object LtsvDeserializationSchema {
  def apply(typeInfo: TypeInformation[Row], failOnMissingField: Boolean, timestampFormat: String): LtsvDeserializationSchema = {
    Preconditions.checkNotNull(typeInfo)
    new LtsvDeserializationSchema(typeInfo.asInstanceOf[RowTypeInfo], failOnMissingField, timestampFormat)
  }

  /**
    * Parse ltsv line to String maps.
    *
    * @param line ltsv line
    * @return result map
    */
  def parseLtsvLine(line: String): Map[String, String] = {
    val columns = line.split("\t", -1)
    columns.flatMap { column =>
      column.split(":", 2) match {
        case Array(key, value) => Some(key -> value)
        case _ => None
      }
    }.toMap
  }
}

/**
  * Deserialization schema from Ltsv to Flink types.
  *
  * <p>Deserializes a <code>byte[]</code> message as a Ltsv object and reads the specified fields.
  *
  * <p>Failure during deserialization are forwarded as wrapped IOExceptions.
  */
class LtsvDeserializationSchema(typeInfo: RowTypeInfo, failOnMissingField: Boolean, timestampFormat: String) extends DeserializationSchema[Row] {

  private lazy val timestampFormatter = DateTimeFormatter.ofPattern(timestampFormat)

  override def deserialize(message: Array[Byte]): Row = {
    convertRow(message)
  }

  override def isEndOfStream(nextElement: Row): Boolean = false

  override def getProducedType: TypeInformation[Row] = typeInfo

  /**
    * Convert input message bytes to row.
    *
    * @param message Input message bytes
    * @return created row
    */
  def convertRow(message: Array[Byte]): Row = {
    val columnMap = LtsvDeserializationSchema.parseLtsvLine(new String(message, StandardCharsets.UTF_8))
    val fieldNames = typeInfo.getFieldNames
    val fieldTypes = typeInfo.getFieldTypes
    val row = new Row(fieldNames.length)

    fieldNames.zipWithIndex.foreach(f = f => {
      if (failOnMissingField && !columnMap.contains(f._1)) {
        throw new IllegalStateException("Could not find field with name '%s' from %s.".format(f._1, new String(message, StandardCharsets.UTF_8)))
      }

      val column = fieldTypes(f._2) match {
        case Types.VOID => null
        case Types.STRING => columnMap.getOrElse(f._1, "")
        case Types.BYTE => columnMap.getOrElse(f._1, "0").toByte
        case Types.BOOLEAN => columnMap.getOrElse(f._1, "false").toBoolean
        case Types.SHORT => columnMap.getOrElse(f._1, "0").toShort
        case Types.INT => columnMap.getOrElse(f._1, "0").toInt
        case Types.LONG => columnMap.getOrElse(f._1, "0").toLong
        case Types.FLOAT => columnMap.getOrElse(f._1, "0.0").toFloat
        case Types.DOUBLE => columnMap.getOrElse(f._1, "0.0").toDouble
        case Types.CHAR => columnMap.getOrElse(f._1, "0").toCharArray.head
        case Types.BIG_DEC => new java.math.BigDecimal(columnMap.getOrElse(f._1, "0.0"))
        case Types.BIG_INT => new java.math.BigInteger(columnMap.getOrElse(f._1, "0"))
        case Types.SQL_DATE => Date.valueOf(columnMap.getOrElse(f._1, ""))
        case Types.SQL_TIME => Time.valueOf(columnMap.getOrElse(f._1, ""))
        case Types.SQL_TIMESTAMP => Timestamp.from(Instant.from(timestampFormatter.parse(columnMap.getOrElse(f._1, ""))))
        case _ => throw new IllegalStateException("Unsupported type information %s for column %s.".format(fieldTypes(f._2), columnMap.getOrElse(f._1, "")))
      }

      row.setField(f._2, column)
    })

    row
  }
}
