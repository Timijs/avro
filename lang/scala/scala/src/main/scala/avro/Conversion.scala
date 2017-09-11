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

import java.nio.ByteBuffer
import java.util
import avro.generic.GenericEnumSymbol
import avro.generic.GenericFixed
import avro.generic.IndexedRecord

/**
  * Conversion between generic and logical type instances.
  * <p>
  * Instances of this class are added to GenericData to convert a logical type
  * to a particular representation.
  * <p>
  * Implementations must provide:
  * * {@link #getConvertedType()}: get the Java class used for the logical type
  * * {@link #getLogicalTypeName()}: get the logical type this implements
  * <p>
  * Subclasses must also override all of the conversion methods for Avro's base
  * types that are valid for the logical type, or else risk causing
  * {@code UnsupportedOperationException} at runtime.
  * <p>
  * Optionally, use {@link #getRecommendedSchema()} to provide a Schema that
  * will be used when a Schema is generated for the class returned by
  * {@code getConvertedType}.
  *
  * @param < T> a Java type that generic data is converted to
  */
abstract class Conversion[T] {
  def getConvertedType: Class[T]

  def getLogicalTypeName: String

  def getRecommendedSchema = throw new UnsupportedOperationException("No recommended schema for " + getLogicalTypeName)

  def fromBoolean(
    value: Boolean,
    schema: Schema,
    `type`: LogicalType
  ) = throw new UnsupportedOperationException("fromBoolean is not supported for " + `type`.getName)

  def fromInt(
    value: Integer,
    schema: Schema,
    `type`: LogicalType
  ) = throw new UnsupportedOperationException("fromInt is not supported for " + `type`.getName)

  def fromLong(
    value: Long,
    schema: Schema,
    `type`: LogicalType
  ) = throw new UnsupportedOperationException("fromLong is not supported for " + `type`.getName)

  def fromFloat(
    value: Float,
    schema: Schema,
    `type`: LogicalType
  ) = throw new UnsupportedOperationException("fromFloat is not supported for " + `type`.getName)

  def fromDouble(
    value: Double,
    schema: Schema,
    `type`: LogicalType
  ) = throw new UnsupportedOperationException("fromDouble is not supported for " + `type`.getName)

  def fromCharSequence(
    value: CharSequence,
    schema: Schema,
    `type`: LogicalType
  ) = throw new UnsupportedOperationException("fromCharSequence is not supported for " + `type`.getName)


  def fromEnumSymbol(
    value: Nothing,
    schema: Schema,
    `type`: LogicalType
  ) = throw new UnsupportedOperationException("fromEnumSymbol is not supported for " + `type`.getName)

  def fromFixed(
    value: Nothing,
    schema: Schema,
    `type`: LogicalType
  ) = throw new UnsupportedOperationException("fromFixed is not supported for " + `type`.getName)

  def fromBytes(
    value: ByteBuffer,
    schema: Schema,
    `type`: LogicalType
  ) = throw new UnsupportedOperationException("fromBytes is not supported for " + `type`.getName)

  def fromArray(
    value: util.Collection[_],
    schema: Schema,
    `type`: LogicalType
  ) = throw new UnsupportedOperationException("fromArray is not supported for " + `type`.getName)

  def fromMap(
    value: util.Map[_, _],
    schema: Schema,
    `type`: LogicalType
  ) = throw new UnsupportedOperationException("fromMap is not supported for " + `type`.getName)

  def fromRecord(
    value: Nothing,
    schema: Schema,
    `type`: LogicalType
  ) = throw new UnsupportedOperationException("fromRecord is not supported for " + `type`.getName)

  def toBoolean(
    value: T,
    schema: Schema,
    `type`: LogicalType
  ) = throw new UnsupportedOperationException("toBoolean is not supported for " + `type`.getName)

  def toInt(
    value: T,
    schema: Schema,
    `type`: LogicalType
  ) = throw new UnsupportedOperationException("toInt is not supported for " + `type`.getName)

  def toLong(
    value: T,
    schema: Schema,
    `type`: LogicalType
  ) = throw new UnsupportedOperationException("toLong is not supported for " + `type`.getName)

  def toFloat(
    value: T,
    schema: Schema,
    `type`: LogicalType
  ) = throw new UnsupportedOperationException("toFloat is not supported for " + `type`.getName)

  def toDouble(
    value: T,
    schema: Schema,
    `type`: LogicalType
  ) = throw new UnsupportedOperationException("toDouble is not supported for " + `type`.getName)

  def toCharSequence(
    value: T,
    schema: Schema,
    `type`: LogicalType
  ) = throw new UnsupportedOperationException("toCharSequence is not supported for " + `type`.getName)

  def toEnumSymbol(
    value: T,
    schema: Schema,
    `type`: LogicalType
  ) = throw new UnsupportedOperationException("toEnumSymbol is not supported for " + `type`.getName)

  def toFixed(
    value: T,
    schema: Schema,
    `type`: LogicalType
  ) = throw new UnsupportedOperationException("toFixed is not supported for " + `type`.getName)

  def toBytes(
    value: T,
    schema: Schema,
    `type`: LogicalType
  ) = throw new UnsupportedOperationException("toBytes is not supported for " + `type`.getName)

  def toArray(
    value: T,
    schema: Schema,
    `type`: LogicalType
  ) = throw new UnsupportedOperationException("toArray is not supported for " + `type`.getName)

  def toMap(
    value: T,
    schema: Schema,
    `type`: LogicalType
  ) = throw new UnsupportedOperationException("toMap is not supported for " + `type`.getName)

  def toRecord(
    value: T,
    schema: Schema,
    `type`: LogicalType
  ) = throw new UnsupportedOperationException("toRecord is not supported for " + `type`.getName)
}
