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

import org.apache.flink.table.descriptors.{DescriptorProperties, FormatDescriptorValidator}
import org.scalatest.FlatSpec

import scala.language.reflectiveCalls

class LtsvFormatFactorySpec extends FlatSpec {
  def fixture =
    new {
      val descriptorProperties = new DescriptorProperties(true)
      descriptorProperties.putString(FormatDescriptorValidator.FORMAT_TYPE, "ltsv")
      val factory = new LtsvFormatFactory
    }

  "createSerializationSchema" should "drived schema setting passed" in {
    // prepare
    val f = fixture
    f.descriptorProperties.putString(Ltsv.FORMAT_SCHEMA, "ROW(temperature FLOAT)")

    // execute
    f.factory.createSerializationSchema(f.descriptorProperties.asMap)

    // verify
    succeed
  }

  "createDeserializationSchema" should "drived schema setting passed" in {
    // prepare
    val f = fixture
    f.descriptorProperties.putString(Ltsv.FORMAT_SCHEMA, "ROW(temperature FLOAT)")

    // execute
    f.factory.createDeserializationSchema(f.descriptorProperties.asMap)

    // verify
    succeed
  }
}
