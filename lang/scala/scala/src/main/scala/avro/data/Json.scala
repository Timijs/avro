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
package avro.data

import java.io.IOException
import java.io.InputStream
import java.io.StringReader
import java.util

import org.apache.avro.util.internal.JacksonUtils
import org.codehaus.jackson.{JsonFactory, JsonNode, JsonParseException, JsonToken}
import org.codehaus.jackson.map.ObjectMapper
import org.codehaus.jackson.node.JsonNodeFactory
import org.codehaus.jackson.node.LongNode
import org.codehaus.jackson.node.DoubleNode
import org.codehaus.jackson.node.TextNode
import org.codehaus.jackson.node.BooleanNode
import org.codehaus.jackson.node.NullNode
import org.codehaus.jackson.node.ArrayNode
import org.codehaus.jackson.node.ObjectNode
import org.apache.avro.Schema
import org.apache.avro.AvroRuntimeException
import org.apache.avro.io.DatumReader
import org.apache.avro.io.DatumWriter
import org.apache.avro.io.Encoder
import org.apache.avro.io.Decoder
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.ResolvingDecoder

/** Utilities for reading and writing arbitrary Json data in Avro format. */
object Json {
  val FACTORY = new JsonFactory
  val MAPPER = new ObjectMapper(FACTORY)
  /** The schema for Json data. */
  var SCHEMA:Schema = _

  /**
    * {@link DatumWriter} for arbitrary Json data.
    *
    * @deprecated use { @link ObjectWriter}
    */
  @deprecated class Writer extends DatumWriter[JsonNode] {
    override def setSchema(schema: Schema): Unit = if (!(SCHEMA == schema)) throw new RuntimeException("Not the Json schema: " + schema)

    @throws[IOException]
    override def write(
      datum: JsonNode,
      out: Encoder
    ): Unit = Json.write(datum, out)
  }

  /**
    * {@link DatumReader} for arbitrary Json data.
    *
    * @deprecated use { @link ObjectReader}
    */
  @deprecated class Reader extends DatumReader[JsonNode] {
    private var written:Schema = _
    private var resolver: ResolvingDecoder = _

    override def setSchema(schema: Schema): Unit = this.written = if (SCHEMA == written) null
    else schema

    @throws[IOException]
    override def read(
      reuse: JsonNode,
      in: Decoder
    ): JsonNode = {
      if (written == null) { // same schema
        return Json.read(in)
      }
      // use a resolver to adapt alternate version of Json schema
      if (resolver == null) resolver = DecoderFactory.get.resolvingDecoder(written, SCHEMA, null)
      resolver.configure(in)
      val result = Json.read(resolver)
      resolver.drain()
      result
    }
  }

  /** {@link DatumWriter} for arbitrary Json data using the object model described
    * in {@link org.apache.avro.JsonProperties}. */
  class ObjectWriter extends DatumWriter[AnyRef] {
    override def setSchema(schema: Schema): Unit = if (!(SCHEMA == schema)) throw new RuntimeException("Not the Json schema: " + schema)

    @throws[IOException]
    override def write(
      datum: Any,
      out: Encoder
    ): Unit = Json.writeObject(datum, out)
  }

  /** {@link DatumReader} for arbitrary Json data using the object model described
    * in {@link org.apache.avro.JsonProperties}. */
  class ObjectReader extends DatumReader[AnyRef] {
    private var written:Schema = _
    private var resolver: ResolvingDecoder = _

    override def setSchema(schema: Schema): Unit = this.written = if (SCHEMA == written) null
    else schema

    @throws[IOException]
    override def read(
      reuse: Any,
      in: Decoder
    ): Any = {
      if (written == null) return Json.readObject(in)
      if (resolver == null) resolver = DecoderFactory.get.resolvingDecoder(written, SCHEMA, null)
      resolver.configure(in)
      val result = Json.readObject(resolver)
      resolver.drain()
      result
    }
  }

  /**
    * Parses a JSON string and converts it to the object model described in
    * {@link org.apache.avro.JsonProperties}.
    */
  def parseJson(s: String): Any = try JacksonUtils.toObject(MAPPER.readTree(FACTORY.createJsonParser(new StringReader(s))))
  catch {
    case e: JsonParseException =>
      throw new RuntimeException(e)
    case e: IOException =>
      throw new RuntimeException(e)
  }

  /**
    * Converts an instance of the object model described in
    * {@link org.apache.avro.JsonProperties} to a JSON string.
    */
  def toString(datum: Any): String = JacksonUtils.toJsonNode(datum).toString

  /** Note: this enum must be kept aligned with the union in Json.avsc. */
  object JsonType {

    sealed trait JsonTypeEnum { def ordinal: Int }
    case object LONG extends JsonTypeEnum {
      def ordinal: Int = 0
    }
    case object DOUBLE extends JsonTypeEnum {
      def ordinal: Int = 1
    }
    case object STRING extends JsonTypeEnum {
      def ordinal: Int = 2
    }
    case object BOOLEAN extends JsonTypeEnum {
      def ordinal: Int = 3
    }
    case object NULL extends JsonTypeEnum {
      def ordinal: Int = 4
    }
    case object ARRAY extends JsonTypeEnum {
      def ordinal: Int = 5
    }
    case object OBJECT extends JsonTypeEnum {
      def ordinal: Int = 6
    }

  }

  /**
    * Write Json data as Avro data.
    *
    * @deprecated internal method
    */
  @deprecated
  @throws[IOException]
  def write(
    node: JsonNode,
    out: Encoder
  ): Unit = {
    import JsonToken._
    node.asToken match {
      case VALUE_NUMBER_INT =>
        out.writeIndex(JsonType.LONG.ordinal)
        out.writeLong(node.getLongValue)
      case VALUE_NUMBER_FLOAT =>
        out.writeIndex(JsonType.DOUBLE.ordinal)
        out.writeDouble(node.getDoubleValue)
      case VALUE_STRING =>
        out.writeIndex(JsonType.STRING.ordinal)
        out.writeString(node.getTextValue)
      case VALUE_TRUE =>
        out.writeIndex(JsonType.BOOLEAN.ordinal)
        out.writeBoolean(true)
      case VALUE_FALSE =>
        out.writeIndex(JsonType.BOOLEAN.ordinal)
        out.writeBoolean(false)
      case VALUE_NULL =>
        out.writeIndex(JsonType.NULL.ordinal)
        out.writeNull()
      case START_ARRAY =>
        out.writeIndex(JsonType.ARRAY.ordinal)
        out.writeArrayStart()
        out.setItemCount(node.size)
        import scala.collection.JavaConversions._
        for (element <- node) {
          out.startItem()
          write(element, out)
        }
        out.writeArrayEnd()
      case START_OBJECT =>
        out.writeIndex(JsonType.OBJECT.ordinal)
        out.writeMapStart()
        out.setItemCount(node.size)
        val i = node.getFieldNames
        while ( {
          i.hasNext
        }) {
          out.startItem()
          val name = i.next
          out.writeString(name)
          write(node.get(name), out)
        }
        out.writeMapEnd()
      case _ =>
        throw new AvroRuntimeException(node.asToken + " unexpected: " + node)
    }
  }
  /**
    * Read Json data from Avro data.
    *
    * @deprecated internal method
    */
  @deprecated
  @throws[IOException]
  def read(in: Decoder): JsonNode = in.readIndex match {
    case 0 =>
      new LongNode(in.readLong)
    case 1 =>
      new DoubleNode(in.readDouble)
    case 2 =>
      new TextNode(in.readString)
    case 3 =>
      if (in.readBoolean) BooleanNode.TRUE
      else BooleanNode.FALSE
    case 4 =>
      in.readNull()
      NullNode.getInstance
    case 5 => ??? // #todo array coding
    case 6 => ??? // #todo object coding
    case _ =>
      throw new AvroRuntimeException("Unexpected Json node type")
  }

  @throws[IOException]
  private def writeObject(
    datum: Any,
    out: Encoder
  ) = write(JacksonUtils.toJsonNode(datum), out)

  @throws[IOException]
  private def readObject(in: Decoder) = JacksonUtils.toObject(read(in))

  try try {
    val in = classOf[Json].getResourceAsStream("/org/apache/avro/data/Json.avsc")
    try SCHEMA = Schema.parse(in)
    finally in.close()
  } catch {
    case e: IOException =>
      throw new AvroRuntimeException(e)
  }
}

class Json private() // singleton: no public ctor
{
}
