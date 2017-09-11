/**
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
  * implied.  See the License for the specific language governing
  * permissions and limitations under the License.
  *//**
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
  * implied.  See the License for the specific language governing
  * permissions and limitations under the License.
  */
package avro

/**
  * <p>
  * A {@link SchemaValidator} for validating the provided schema against the
  * first {@link Schema} in the iterable in {@link #validate(Schema, Iterable)}.
  * </p>
  * <p>
  * Uses the {@link SchemaValidationStrategy} provided in the constructor to
  * validate the schema against the first Schema in the iterable, if it exists,
  * via {@link SchemaValidationStrategy#validate(Schema, Schema)}.
  * </p>
  */
final class ValidateLatest(val strategy: SchemaValidationStrategy)
  extends SchemaValidator {
//  @throws[SchemaValidationException]
  def validate(
    toValidate: Schema,
    schemasInOrder: Iterable[Schema]
  ): Unit = {
    val schemas = schemasInOrder.iterator
    if (schemas.hasNext) {
      val existing = schemas.next
      strategy.validate(toValidate, existing)
    }
  }
}
