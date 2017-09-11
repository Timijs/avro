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

import java.math.BigDecimal
import java.math.BigInteger
import java.nio.ByteBuffer
import java.util
import java.util.UUID
import avro.generic.GenericData
import avro.generic.GenericEnumSymbol
import avro.generic.GenericFixed
import avro.generic.IndexedRecord

object Conversions {

  class UUIDConversion extends Conversion[UUID] {
    def getConvertedType: Class[UUID] = classOf[UUID]

    def getLogicalTypeName = "uuid"

    override def getRecommendedSchema: Schema = LogicalTypes.uuid.addToSchema(Schema.create(Schema.Type.STRING))

    override def fromCharSequence(
      value: CharSequence,
      schema: Schema,
      `type`: LogicalType
    ): UUID = UUID.fromString(value.toString)

    override def toCharSequence(
      value: UUID,
      schema: Schema,
      `type`: LogicalType
    ): CharSequence = value.toString
  }

  class DecimalConversion extends Conversion[BigDecimal] {
    override def getConvertedType: Class[BigDecimal] = classOf[BigDecimal]

    override def getRecommendedSchema = throw new UnsupportedOperationException("No recommended schema for decimal (scale is required)")

    override def getLogicalTypeName = "decimal"

    override def fromBytes(
      value: ByteBuffer,
      schema: Schema,
      `type`: LogicalType
    ): BigDecimal = {
      val scale = `type`.asInstanceOf[LogicalTypes.Decimal].getScale
      // always copy the bytes out because BigInteger has no offset/length ctor
      val bytes = new Array[Byte](value.remaining)
      value.get(bytes)
      new BigDecimal(new BigInteger(bytes), scale)
    }

    override def toBytes(
      value: BigDecimal,
      schema: Schema,
      `type`: LogicalType
    ): ByteBuffer = {
      val scale = `type`.asInstanceOf[LogicalTypes.Decimal].getScale
      if (scale != value.scale) throw new AvroTypeException("Cannot encode decimal with scale " + value.scale + " as scale " + scale)
      ByteBuffer.wrap(value.unscaledValue.toByteArray)
    }

    def fromFixed(
      value: GenericFixed,
      schema: Schema,
      `type`: LogicalType
    ): BigDecimal = {
      val scale = `type`.asInstanceOf[LogicalTypes.Decimal].getScale
      new BigDecimal(new BigInteger(value.bytes), scale)
    }

    override def toFixed(
      value: BigDecimal,
      schema: Schema,
      `type`: LogicalType
    ): GenericFixed = {
      val scale = `type`.asInstanceOf[LogicalTypes.Decimal].getScale
      if (scale != value.scale) throw new AvroTypeException("Cannot encode decimal with scale " + value.scale + " as scale " + scale)
      val fillByte = (if (value.signum < 0) 0xFF
      else 0x00).toByte
      val unscaled = value.unscaledValue.toByteArray
      val bytes = new Array[Byte](schema.getFixedSize)
      val offset = bytes.length - unscaled.length
      var i = 0
      while ( {
        i < bytes.length
      }) {
        if (i < offset) bytes(i) = fillByte
        else bytes(i) = unscaled(i - offset)
        i += 1
      }
      new GenericData.Fixed(schema, bytes)
    }
  }

  /**
    * Convert a underlying representation of a logical type (such as a
    * ByteBuffer) to a higher level object (such as a BigDecimal).
    *
    * @param datum  The object to be converted.
    * @param schema The schema of datum. Cannot be null if datum is not null.
    * @param type   The { @link org.apache.avro.LogicalType} of datum. Cannot
    *                           be null if datum is not null.
    * @param conversion The tool used to finish the conversion. Cannot
    *                   be null if datum is not null.
    * @return The result object, which is a high level object of the logical
    * type. If a null datum is passed in, a null value will be returned.
    * @throws IllegalArgumentException if a null schema, type or conversion
    *                                  is passed in while datum is not null.
    */
  def convertToLogicalType(
    datum: Any,
    schema: Schema,
    `type`: LogicalType,
    conversion: Conversion[_]
  ): Any = {
    if (datum == null) return null
    if (schema == null || `type` == null || conversion == null) throw new IllegalArgumentException("Parameters cannot be null! Parameter values:" + util.Arrays.deepToString(Array[AnyRef](datum, schema, `type`, conversion)))
    try {
      schema.getType match {
        case RECORD =>
          return conversion.fromRecord(datum.asInstanceOf[IndexedRecord], schema, `type`)
        case ENUM =>
          return conversion.fromEnumSymbol(datum.asInstanceOf[GenericEnumSymbol], schema, `type`)
        case ARRAY =>
          return conversion.fromArray(datum.asInstanceOf[util.Collection[_]], schema, `type`)
        case MAP =>
          return conversion.fromMap(datum.asInstanceOf[util.Map[_, _]], schema, `type`)
        case FIXED =>
          return conversion.fromFixed(datum.asInstanceOf[GenericFixed], schema, `type`)
        case STRING =>
          return conversion.fromCharSequence(datum.asInstanceOf[CharSequence], schema, `type`)
        case BYTES =>
          return conversion.fromBytes(datum.asInstanceOf[ByteBuffer], schema, `type`)
        case INT =>
          return conversion.fromInt(datum.asInstanceOf[Integer], schema, `type`)
        case LONG =>
          return conversion.fromLong(datum.asInstanceOf[Long], schema, `type`)
        case FLOAT =>
          return conversion.fromFloat(datum.asInstanceOf[Float], schema, `type`)
        case DOUBLE =>
          return conversion.fromDouble(datum.asInstanceOf[Double], schema, `type`)
        case BOOLEAN =>
          return conversion.fromBoolean(datum.asInstanceOf[Boolean], schema, `type`)
      }
      datum
    } catch {
      case e: ClassCastException =>
        throw new AvroRuntimeException("Cannot convert " + datum + ":" + datum.getClass.getSimpleName + ": expected generic type", e)
    }
  }

  /**
    * Convert a high level representation of a logical type (such as a BigDecimal)
    * to the its underlying representation object (such as a ByteBuffer)
    *
    * @param datum  The object to be converted.
    * @param schema The schema of datum. Cannot be null if datum is not null.
    * @param type   The { @link org.apache.avro.LogicalType} of datum. Cannot
    *                           be null if datum is not null.
    * @param conversion The tool used to finish the conversion. Cannot
    *                   be null if datum is not null.
    * @return The result object, which is an underlying representation object
    *         of the logical type. If the input param datum is null, a null value will
    *         be returned.
    * @throws IllegalArgumentException if a null schema, type or conversion
    *                                  is passed in while datum is not null.
    */
  def convertToRawType[T](
    datum: Any,
    schema: Schema,
    `type`: LogicalType,
    conversion: Conversion[T]
  ): Any = {
    if (datum == null) return null
    if (schema == null || `type` == null || conversion == null) throw new IllegalArgumentException("Parameters cannot be null! Parameter values:" + util.Arrays.deepToString(Array[AnyRef](datum, schema, `type`, conversion)))
    try {
      val fromClass = conversion.getConvertedType
      schema.getType match {
        case RECORD =>
          return conversion.toRecord(fromClass.cast(datum), schema, `type`)
        case ENUM =>
          return conversion.toEnumSymbol(fromClass.cast(datum), schema, `type`)
        case ARRAY =>
          return conversion.toArray(fromClass.cast(datum), schema, `type`)
        case MAP =>
          return conversion.toMap(fromClass.cast(datum), schema, `type`)
        case FIXED =>
          return conversion.toFixed(fromClass.cast(datum), schema, `type`)
        case STRING =>
          return conversion.toCharSequence(fromClass.cast(datum), schema, `type`)
        case BYTES =>
          return conversion.toBytes(fromClass.cast(datum), schema, `type`)
        case INT =>
          return conversion.toInt(fromClass.cast(datum), schema, `type`)
        case LONG =>
          return conversion.toLong(fromClass.cast(datum), schema, `type`)
        case FLOAT =>
          return conversion.toFloat(fromClass.cast(datum), schema, `type`)
        case DOUBLE =>
          return conversion.toDouble(fromClass.cast(datum), schema, `type`)
        case BOOLEAN =>
          return conversion.toBoolean(fromClass.cast(datum), schema, `type`)
      }
      datum
    } catch {
      case e: ClassCastException =>
        throw new AvroRuntimeException("Cannot convert " + datum + ":" + datum.getClass.getSimpleName + ": expected logical type", e)
    }
  }
}
