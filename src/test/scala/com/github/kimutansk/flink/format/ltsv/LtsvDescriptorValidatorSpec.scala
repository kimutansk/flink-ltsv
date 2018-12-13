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

import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.descriptors.{DescriptorProperties, FormatDescriptorValidator}
import org.scalatest.FlatSpec

import scala.language.reflectiveCalls

class LtsvDescriptorValidatorSpec extends FlatSpec {
  def fixture =
    new {
      val descriptorProperties = new DescriptorProperties(true)
      descriptorProperties.putString(FormatDescriptorValidator.FORMAT_TYPE, "ltsv")
      val validator = new LtsvDescriptorValidator
    }

  "validate" should "drived schema setting passed" in {
    // prepare
    val f = fixture
    f.descriptorProperties.putBoolean(FormatDescriptorValidator.FORMAT_DERIVE_SCHEMA, true)

    // execute
    f.validator.validate(f.descriptorProperties)

    // verify
    succeed
  }

  "validate" should "drived schema and schema setting not passed" in {
    // prepare
    val f = fixture
    f.descriptorProperties.putBoolean(FormatDescriptorValidator.FORMAT_DERIVE_SCHEMA, true)
    f.descriptorProperties.putString(Ltsv.FORMAT_SCHEMA, "Test")

    // execute & velify
    assertThrows[ValidationException] {
      f.validator.validate(f.descriptorProperties)
    }
  }

  "validate" should "schema setting passed" in {
    // prepare
    val f = fixture
    f.descriptorProperties.putString(Ltsv.FORMAT_SCHEMA, "ROW<temperature FLOAT>")

    // execute
    f.validator.validate(f.descriptorProperties)

    // verify
    succeed
  }
}
