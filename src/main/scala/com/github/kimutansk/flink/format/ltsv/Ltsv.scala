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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.descriptors.{DescriptorProperties, FormatDescriptor, FormatDescriptorValidator}
import org.apache.flink.table.typeutils.TypeStringUtils
import org.apache.flink.types.Row
import org.apache.flink.util.Preconditions

object Ltsv {
  /** flink-ltsv common constants */
  val FORMAT_TYPE_VALUE = "ltsv"
  val FORMAT_SCHEMA = "format.schema"
  val FORMAT_TIMESTAMP_FORMAT = "format.timestamp-format"
  val FORMAT_FAIL_ON_MISSING_FIELD = "format.fail-on-missing-field"
  val DEFAULT_TIMESTAMP_FORMAT = ""

  /**
    * Create Ltsv format descryptor with default configs.
    *
    * @return Ltsv format descryptor
    */
  def apply(): Ltsv = new Ltsv(LtsvConf())

  /**
    * Create Ltsv format descryptor with specified configs.
    *
    * @param conf Config
    * @return Ltsv format descryptor
    */
  def apply(conf: LtsvConf): Ltsv = new Ltsv(conf)
}

case class LtsvConf(driveSchema: Boolean = false, failOnMissingField: Boolean = false, schema: String = "", timestampFormat: String = Ltsv.DEFAULT_TIMESTAMP_FORMAT)

/**
  * Ltsv format descryptor
  *
  * @param conf Configuration
  */
class Ltsv(conf: LtsvConf) extends FormatDescriptor(Ltsv.FORMAT_TYPE_VALUE, version = 1) {

  /**
    * Create driveSchema=true configured Ltsv format descryptor.
    *
    * @return Ltsv format descryptor
    */
  def driveSchema(): Ltsv = {
    Ltsv(LtsvConf(driveSchema = true, conf.failOnMissingField, conf.schema, conf.timestampFormat))
  }

  /**
    * Create failOnMissingField=true configured Ltsv format descryptor.
    *
    * @return Ltsv format descryptor
    */
  def failOnMissingField(): Ltsv = {
    Ltsv(LtsvConf(conf.driveSchema, failOnMissingField = true, conf.schema, conf.timestampFormat))
  }

  /**
    * Create schemaType configured Ltsv format descryptor.
    *
    * @param schemaType TypeInformation to configure.
    * @return Ltsv format descryptor
    */
  def schema(schemaType: TypeInformation[Row]): Ltsv = {
    Preconditions.checkNotNull(schemaType)
    Ltsv(LtsvConf(conf.driveSchema, conf.failOnMissingField, TypeStringUtils.writeTypeInfo(schemaType), conf.timestampFormat))
  }

  /**
    * Create schema configured Ltsv format descryptor.
    *
    * @param schema schema to configure.
    * @return Ltsv format descryptor
    */
  def schema(schema: String): Ltsv = {
    Preconditions.checkNotNull(schema)
    Ltsv(LtsvConf(conf.driveSchema, conf.failOnMissingField, schema, conf.timestampFormat))
  }

  /**
    * Create timestampFormat configured Ltsv format descryptor.
    *
    * @param timestampFormat timestampFormat to configure.
    * @return Ltsv format descryptor
    */
  def timestampFormat(timestampFormat: String): Ltsv = {
    Preconditions.checkNotNull(timestampFormat)
    Ltsv(LtsvConf(conf.driveSchema, conf.failOnMissingField, conf.schema, timestampFormat))
  }

  override protected def addFormatProperties(properties: DescriptorProperties): Unit = {
    properties.putBoolean(FormatDescriptorValidator.FORMAT_DERIVE_SCHEMA, conf.driveSchema)
    properties.putBoolean(Ltsv.FORMAT_FAIL_ON_MISSING_FIELD, conf.failOnMissingField)
    properties.putString(Ltsv.FORMAT_SCHEMA, conf.schema)
    properties.putString(Ltsv.FORMAT_TIMESTAMP_FORMAT, conf.timestampFormat)
  }
}
