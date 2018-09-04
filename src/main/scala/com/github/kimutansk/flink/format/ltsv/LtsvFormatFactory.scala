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

import java.util

import collection.JavaConverters._
import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema}
import org.apache.flink.table.descriptors.{FormatDescriptorValidator, SchemaValidator}
import org.apache.flink.table.factories.{DeserializationSchemaFactory, SerializationSchemaFactory}
import org.apache.flink.types.Row

import scala.collection.JavaConverters

object LtsvFormatFactory {
  val REQURED_CONTEXT: Map[String, String] = Map(FormatDescriptorValidator.FORMAT_TYPE -> Ltsv.FORMAT_TYPE_VALUE, FormatDescriptorValidator.FORMAT_PROPERTY_VERSION -> "1")
  val SUPPORTED_PROPERTIES: Seq[String] = List.concat(Seq(Ltsv.FORMAT_SCHEMA, Ltsv.FORMAT_FAIL_ON_MISSING_FIELD, FormatDescriptorValidator.FORMAT_DERIVE_SCHEMA),
    JavaConverters.asScalaIteratorConverter(SchemaValidator.getSchemaDerivationKeys.iterator()).asScala.toSeq)
}

class LtsvFormatFactory extends SerializationSchemaFactory[Row] with DeserializationSchemaFactory[Row] {

  override def requiredContext(): util.Map[String, String] = {
    LtsvFormatFactory.REQURED_CONTEXT.asJava
  }

  override def supportedProperties(): util.List[String] = {
    LtsvFormatFactory.SUPPORTED_PROPERTIES.asJava
  }

  override def supportsSchemaDerivation(): Boolean = true

  override def createSerializationSchema(properties: util.Map[String, String]): SerializationSchema[Row] = ???

  override def createDeserializationSchema(properties: util.Map[String, String]): DeserializationSchema[Row] = ???
}
