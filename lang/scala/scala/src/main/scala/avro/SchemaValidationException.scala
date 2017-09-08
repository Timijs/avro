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
  * Thrown when {@link SchemaValidator} fails to validate a schema.
  */
object SchemaValidationException {
  private def getMessage(reader: Schema, writer: Schema) =
    s"Unable to read schema: \n${writer.toString(true)}\nusing schema:\n${reader.toString(true)}"
}

class SchemaValidationException extends Exception {
  def this(reader: Schema, writer: Schema) {
    this()
    new Exception(SchemaValidationException.getMessage(reader, writer))
  }

  def this(reader: Schema, writer: Schema, cause: Throwable) {
    this()
    new Exception(SchemaValidationException.getMessage(reader, writer), cause)
  }
}
