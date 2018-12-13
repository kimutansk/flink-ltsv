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

import org.apache.flink.table.descriptors.{DescriptorProperties, FormatDescriptorValidator}
import org.apache.flink.types.Row
import org.scalatest.{FlatSpec, Matchers}

import scala.language.reflectiveCalls

class LtsvSerializationSchemaSpec extends FlatSpec with Matchers {
  def fixture =
    new {
      val descriptorProperties = new DescriptorProperties(true)
      descriptorProperties.putString(FormatDescriptorValidator.FORMAT_TYPE, "ltsv")
      val factory = new LtsvFormatFactory
    }

  "serialize" should "serialized base pattern" in {
    // prepare
    val f = fixture
    f.descriptorProperties.putString(Ltsv.FORMAT_SCHEMA, "ROW<a INT, b FLOAT, c TIME, d TIMESTAMP>")
    val serializationSchema = f.factory.createSerializationSchema(f.descriptorProperties.asMap)
    val input = Row.of(Integer.valueOf("1"),java.lang.Float.valueOf("1.1"),Time.valueOf("19:38:01"), Timestamp.valueOf("2018-08-28 19:38:01"))

    // execute
    val result = serializationSchema.serialize(input)

    // verify
    new String(result, StandardCharsets.UTF_8) should be("a:1\tb:1.1\tc:19:38:01\td:2018-08-28T19:38:01+09:00")
  }
}
