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

/**
  * Ltsv Descriptor properties validator.
  */
class LtsvDescriptorValidator extends FormatDescriptorValidator {

  override def validate(properties: DescriptorProperties): Unit = {
    super.validate(properties)
    properties.validateBoolean(FormatDescriptorValidator.FORMAT_DERIVE_SCHEMA, true)
    val deriveSchema = properties.getOptionalBoolean(FormatDescriptorValidator.FORMAT_DERIVE_SCHEMA).orElse(false)
    val hasSchema = !properties.getOptionalString(Ltsv.FORMAT_SCHEMA).orElse("").isEmpty
    if (deriveSchema && hasSchema) {
      throw new ValidationException(
        "Format cannot define a schema and derive from the table's schema at the same time.")
    } else if (!deriveSchema) {
      properties.validateString(Ltsv.FORMAT_SCHEMA, false, 1)
    }
    properties.validateBoolean(Ltsv.FORMAT_FAIL_ON_MISSING_FIELD, true)
    properties.validateString(Ltsv.FORMAT_TIMESTAMP_FORMAT, true, 1)
  }
}
