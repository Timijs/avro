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
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
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
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package avro

/**
  * <p>
  * A Builder for creating SchemaValidators.
  * </p>
  */
final class SchemaValidatorBuilder {
  private var strategy: SchemaValidationStrategy = _

  def strategy(strategy: SchemaValidationStrategy): SchemaValidatorBuilder = {
    this.strategy = strategy
    this
  }

  /**
    * Use a strategy that validates that a schema can be used to read existing
    * schema(s) according to the Avro default schema resolution.
    */
  def canReadStrategy: SchemaValidatorBuilder = {
    this.strategy = new ValidateCanRead
    this
  }

  /**
    * Use a strategy that validates that a schema can be read by existing
    * schema(s) according to the Avro default schema resolution.
    */
  def canBeReadStrategy: SchemaValidatorBuilder = {
    this.strategy = new ValidateCanBeRead
    this
  }

  /**
    * Use a strategy that validates that a schema can read existing schema(s),
    * and vice-versa, according to the Avro default schema resolution.
    */
  def mutualReadStrategy: SchemaValidatorBuilder = {
    this.strategy = new ValidateMutualRead
    this
  }

  def validateLatest: SchemaValidator = {
    valid()
    new ValidateLatest(strategy)
  }

  def validateAll: SchemaValidator = {
    valid()
    new ValidateAll(strategy)
  }

  private def valid() = if (null == strategy) throw new AvroRuntimeException("SchemaValidationStrategy not specified in builder")
}
