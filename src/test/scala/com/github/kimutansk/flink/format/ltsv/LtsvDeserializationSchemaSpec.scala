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

import org.apache.flink.table.descriptors.{DescriptorProperties, FormatDescriptorValidator}
import org.scalatest.{FlatSpec, Matchers}

import scala.language.reflectiveCalls

class LtsvDeserializationSchemaSpec extends FlatSpec with Matchers {
  def fixture =
    new {
      val descriptorProperties = new DescriptorProperties(true)
      descriptorProperties.putString(FormatDescriptorValidator.FORMAT_TYPE, "ltsv")
      val factory = new LtsvFormatFactory
    }

  "deserialize" should "deserialized base pattern" in {
    // prepare
    val f = fixture
    f.descriptorProperties.putString(Ltsv.FORMAT_SCHEMA, "ROW<temperature FLOAT, whc FLOAT, humidity FLOAT, ph FLOAT, id STRING, time TIMESTAMP>")
    val deserializationSchema = f.factory.createDeserializationSchema(f.descriptorProperties.asMap)
    val input = "temperature:21.0\twhc:24.6\thumidity:85.0\tph:6.3\tid:sensor_01\ttime:2018-08-28T19:38:01+09:00".getBytes(StandardCharsets.UTF_8)

    // execute
    val result = deserializationSchema.deserialize(input)

    // verify
    result.getArity should be(6)
    result.getField(0).isInstanceOf[Float] should be(true)
    result.getField(4).isInstanceOf[String] should be(true)
    result.getField(5).isInstanceOf[Timestamp] should be(true)
    result.toString should be("21.0,24.6,85.0,6.3,sensor_01,2018-08-28 19:38:01.0")
  }

  "deserialize" should "when column does not exists, throw IllegalStateException" in {
    // prepare
    val f = fixture
    f.descriptorProperties.putString(Ltsv.FORMAT_SCHEMA, "ROW<temperature FLOAT, whc FLOAT, humidity FLOAT, ph FLOAT, id STRING, time TIMESTAMP>")
    f.descriptorProperties.putBoolean(Ltsv.FORMAT_FAIL_ON_MISSING_FIELD, true)
    val deserializationSchema = f.factory.createDeserializationSchema(f.descriptorProperties.asMap)
    val input = "temperature:21.0\twhc:24.6\thumidity:85.0\tph:6.3\ttime:2018-08-28T19:38:01+09:00".getBytes(StandardCharsets.UTF_8)

    // execute & velify
    assertThrows[IllegalStateException] {
      deserializationSchema.deserialize(input)
    }
  }

  "deserialize" should "deserialize time columns" in {
    // prepare
    val f = fixture
    f.descriptorProperties.putString(Ltsv.FORMAT_TIMESTAMP_FORMAT, "yyyy-MM-dd HH:mm:ssXXX")
    f.descriptorProperties.putString(Ltsv.FORMAT_SCHEMA, "ROW<testtimestamp TIMESTAMP, testdate DATE, testtime TIME>")
    val deserializationSchema = f.factory.createDeserializationSchema(f.descriptorProperties.asMap)
    val input = "testtimestamp:2018-08-28 19:38:01+09:00\ttestdate:2018-08-28\ttesttime:19:38:01".getBytes(StandardCharsets.UTF_8)

    // execute
    val result = deserializationSchema.deserialize(input)

    // verify
    result.getArity should be(3)
    result.getField(0).isInstanceOf[Timestamp] should be(true)
    result.getField(1).isInstanceOf[Date] should be(true)
    result.getField(2).isInstanceOf[Time] should be(true)
    result.toString should be("2018-08-28 19:38:01.0,2018-08-28,19:38:01")
  }

  "deserialize" should "deserialize number columns part1" in {
    // prepare
    val f = fixture
    f.descriptorProperties.putString(Ltsv.FORMAT_SCHEMA, "ROW<a BOOLEAN, b BYTE, c SHORT, d INT, e LONG, f FLOAT, g DOUBLE, h DECIMAL>")
    val deserializationSchema = f.factory.createDeserializationSchema(f.descriptorProperties.asMap)
    val input = "a:true\tb:2\tc:3\td:4\te:5\tf:6\tg:7\th:8.008".getBytes(StandardCharsets.UTF_8)

    // execute
    val result = deserializationSchema.deserialize(input)

    // verify
    result.getArity should be(8)
    result.getField(0).isInstanceOf[Boolean] should be(true)
    result.getField(1).isInstanceOf[Byte] should be(true)
    result.getField(2).isInstanceOf[Short] should be(true)
    result.getField(3).isInstanceOf[Int] should be(true)
    result.getField(4).isInstanceOf[Long] should be(true)
    result.getField(5).isInstanceOf[Float] should be(true)
    result.getField(6).isInstanceOf[Double] should be(true)
    result.getField(7).isInstanceOf[java.math.BigDecimal] should be(true)
    result.toString should be("true,2,3,4,5,6.0,7.0,8.008")
  }
}
