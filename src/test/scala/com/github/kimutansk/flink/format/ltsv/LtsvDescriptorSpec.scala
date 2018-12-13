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
import org.apache.flink.table.descriptors.{DescriptorProperties, FormatDescriptorValidator}
import org.apache.flink.table.utils.TypeStringUtils
import org.apache.flink.types.Row
import org.scalatest.{FlatSpec, Matchers, PrivateMethodTester}

import scala.language.reflectiveCalls

class LtsvDescriptorSpec extends FlatSpec with PrivateMethodTester with Matchers {

  def fixture =
    new {
      val descriptorProperties = new DescriptorProperties(true)
      val addFormatProperties = PrivateMethod[Unit]('addFormatProperties)
    }

  "toFormatProperties" should "initial applied config" in {
    // prepare
    val f = fixture
    val target = Ltsv()

    // execute
    val result = target.toFormatProperties

    // verify
    f.descriptorProperties.putProperties(result)
    f.descriptorProperties.getBoolean(FormatDescriptorValidator.FORMAT_DERIVE_SCHEMA) should be(false)
    f.descriptorProperties.getBoolean(Ltsv.FORMAT_FAIL_ON_MISSING_FIELD) should be(false)
    f.descriptorProperties.getString(Ltsv.FORMAT_SCHEMA) should be("")
    f.descriptorProperties.getString(Ltsv.FORMAT_TIMESTAMP_FORMAT) should be(Ltsv.DEFAULT_TIMESTAMP_FORMAT)
  }

  "toFormatProperties" should "boolean set method applied config" in {
    // prepare
    val f = fixture
    val target = Ltsv().driveSchema().failOnMissingField()

    // execute
    val result = target.toFormatProperties

    // verify
    f.descriptorProperties.putProperties(result)
    f.descriptorProperties.getBoolean(FormatDescriptorValidator.FORMAT_DERIVE_SCHEMA) should be(true)
    f.descriptorProperties.getBoolean(Ltsv.FORMAT_FAIL_ON_MISSING_FIELD) should be(true)
    f.descriptorProperties.getString(Ltsv.FORMAT_SCHEMA) should be("")
    f.descriptorProperties.getString(Ltsv.FORMAT_TIMESTAMP_FORMAT) should be(Ltsv.DEFAULT_TIMESTAMP_FORMAT)
  }

  "toFormatProperties" should "string set method applied config" in {
    // prepare
    val f = fixture
    val target = Ltsv().schema("ROW<temperature FLOAT>").timestampFormat("TestFormat")

    // execute
    val result = target.toFormatProperties

    // verify
    f.descriptorProperties.putProperties(result)
    f.descriptorProperties.getBoolean(FormatDescriptorValidator.FORMAT_DERIVE_SCHEMA) should be(false)
    f.descriptorProperties.getBoolean(Ltsv.FORMAT_FAIL_ON_MISSING_FIELD) should be(false)
    f.descriptorProperties.getString(Ltsv.FORMAT_SCHEMA) should be("ROW<temperature FLOAT>")
    f.descriptorProperties.getString(Ltsv.FORMAT_TIMESTAMP_FORMAT) should be("TestFormat")
  }

  "toFormatProperties" should "TypeInformation set method applied config" in {
    // prepare
    val f = fixture
    val target = Ltsv().schema(TypeStringUtils.readTypeInfo("ROW<a BOOLEAN, b STRING>").asInstanceOf[TypeInformation[Row]])

    // execute
    val result = target.toFormatProperties

    // verify
    f.descriptorProperties.putProperties(result)
    f.descriptorProperties.getBoolean(FormatDescriptorValidator.FORMAT_DERIVE_SCHEMA) should be(false)
    f.descriptorProperties.getBoolean(Ltsv.FORMAT_FAIL_ON_MISSING_FIELD) should be(false)
    f.descriptorProperties.getString(Ltsv.FORMAT_SCHEMA) should be("ROW<a BOOLEAN, b VARCHAR>")
    f.descriptorProperties.getString(Ltsv.FORMAT_TIMESTAMP_FORMAT) should be(Ltsv.DEFAULT_TIMESTAMP_FORMAT)
  }
}
