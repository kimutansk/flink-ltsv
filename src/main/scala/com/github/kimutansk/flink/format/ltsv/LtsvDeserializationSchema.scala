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

import java.time.format.DateTimeFormatter

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
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

  override def deserialize(message: Array[Byte]): Row = ???

  override def isEndOfStream(nextElement: Row): Boolean = false

  override def getProducedType: TypeInformation[Row] = typeInfo
}
