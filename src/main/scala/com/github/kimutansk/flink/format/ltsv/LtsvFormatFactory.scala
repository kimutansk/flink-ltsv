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

import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema}
import org.apache.flink.table.factories.{DeserializationSchemaFactory, SerializationSchemaFactory}
import org.apache.flink.types.Row

class LtsvFormatFactory extends SerializationSchemaFactory[Row] with DeserializationSchemaFactory[Row] {
  override def createSerializationSchema(properties: util.Map[String, String]): SerializationSchema[Row] = ???

  override def createDeserializationSchema(properties: util.Map[String, String]): DeserializationSchema[Row] = ???

  override def supportsSchemaDerivation(): Boolean = ???

  override def supportedProperties(): util.List[String] = ???

  override def requiredContext(): util.Map[String, String] = ???
}
