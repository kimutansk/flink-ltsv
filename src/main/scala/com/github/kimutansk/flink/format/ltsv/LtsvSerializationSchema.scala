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
import java.sql.{Time, Timestamp}
import java.time.format.DateTimeFormatter

import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.Types
import org.apache.flink.types.Row
import org.apache.flink.util.Preconditions

object LtsvSerializationSchema {
  def apply(typeInfo: TypeInformation[Row], timestampFormat: String): LtsvSerializationSchema = {
    Preconditions.checkNotNull(typeInfo)
    new LtsvSerializationSchema(typeInfo.asInstanceOf[RowTypeInfo], timestampFormat)
  }
}

/**
  * Serialization schema that serializes an object of Flink types into a Ltsv bytes.
  *
  * <p>Serializes the input Flink object into a Ltsv string and converts it into <code>byte[]</code>.
  *
  * <p>Result <code>byte[]</code> messages can be deserialized using LtsvDeserializationSchema.
  */
class LtsvSerializationSchema(typeInfo: RowTypeInfo, timestampFormat: String) extends SerializationSchema[Row] {

  private lazy val timeFormatter = DateTimeFormatter.ISO_LOCAL_TIME

  private lazy val timestampFormatter = DateTimeFormatter.ofPattern(timestampFormat)

  override def serialize(element: Row): Array[Byte] = {
    val ltsvString = convertRowToLtsvString(element)
    ltsvString.getBytes(StandardCharsets.UTF_8)
  }

  /**
    * Convert Row to specified columned Ltsv string.
    *
    * @param row Row
    * @return Ltsv string
    */
  def convertRowToLtsvString(row: Row): String = {
    val fieldNames = typeInfo.getFieldNames
    val fieldTypes = typeInfo.getFieldTypes
    val fieldNum = fieldNames.length
    if (row.getArity != fieldNum) {
      throw new IllegalStateException(s"Number of elements in the row $row is different from number of field names: $fieldNum")
    }

    fieldNames.zipWithIndex.map(field => {
      val columnValue = convertColumnToLtsvString(fieldTypes(field._2), row.getField(field._2))
      field._1 + ":" + columnValue
    }).mkString("\t")
  }

  /**
    * Convert object to specified column value string.
    *
    * @param fieldTypeInfo output type information
    * @param column        column
    * @return column value string
    */
  def convertColumnToLtsvString(fieldTypeInfo: TypeInformation[_], column: AnyRef): String = {
    fieldTypeInfo match {
      case Types.SQL_TIME => timeFormatter.format(column.asInstanceOf[Time].toInstant)
      case Types.SQL_TIMESTAMP => timestampFormatter.format(column.asInstanceOf[Timestamp].toInstant)
      case _ => column.toString
    }
  }
}
