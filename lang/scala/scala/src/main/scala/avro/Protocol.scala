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

import java.io.ByteArrayInputStream
import java.io.File
import java.io.InputStream
import java.io.StringWriter
import java.io.IOException
import java.security.MessageDigest
import java.util
import java.util.Collections

import avro.SchemaType._
import org.apache.avro.Schema.Field
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.JsonParser
import org.codehaus.jackson.JsonGenerator
import org.codehaus.jackson.node.TextNode

object Protocol {
  /** The version of the protocol specification implemented here. */
    val VERSION = 1
  // Support properties for both Protocol and Message objects
  private val MESSAGE_RESERVED = new java.util.HashSet[String]
  private val FIELD_RESERVED = new java.util.HashSet[String]
  /** An error that can be thrown by any message. */
  val SYSTEM_ERROR: Schema = Schema.create(STRING)
  /** Union type for generating system errors. */
  var SYSTEM_ERRORS = null
  private val PROTOCOL_RESERVED = new java.util.HashSet[String]

  /** Read a protocol from a Json file. */
  @throws[IOException]
  def parse(file: File): Protocol = parse(Schema.FACTORY.createJsonParser(file))

  /** Read a protocol from a Json stream. */
  @throws[IOException]
  def parse(stream: InputStream): Protocol = parse(Schema.FACTORY.createJsonParser(stream))

  /** Read a protocol from one or more json strings */
  def parse(
    string: String,
    more: String*
  ): Protocol = {
    val b = new StringBuilder(string)
    for (part <- more) {
      b.append(part)
    }
    parse(b.toString)
  }

  /** Read a protocol from a Json string. */
  def parse(string: String): Protocol = try parse(Schema.FACTORY.createJsonParser(new ByteArrayInputStream(string.getBytes("UTF-8"))))
  catch {
    case e: IOException =>
      throw new AvroRuntimeException(e)
  }

  private def parse(parser: JsonParser) = try {
    val protocol = new Protocol
    protocol.parse(Schema.MAPPER.readTree(parser))
    protocol
  } catch {
    case e: IOException =>
      throw new SchemaParseException(e)
  }

  @throws[Exception]
  def main(args: Array[String]): Unit = System.out.println(Protocol.parse(new File(args(0))))

  try Collections.addAll(MESSAGE_RESERVED, "doc", "response", "request", "errors", "one-way")
  Collections.addAll(FIELD_RESERVED, "name", "type", "doc", "default", "aliases")
  val errors = new java.util.ArrayList[Schema]
  errors.add(SYSTEM_ERROR)
  SYSTEM_ERRORS = Schema.createUnion(errors)
  Collections.addAll(PROTOCOL_RESERVED, "namespace", "protocol", "doc", "messages", "types", "errors")
}

class Protocol private() extends JsonProperties(Protocol.PROTOCOL_RESERVED) {

  /** A protocol message. */
  class Message private(
    var name: String,
    var doc: String,
    val propMap: Map[String, _],
    var request: Schema
  )

  /** Construct a message. */
    extends JsonProperties(Protocol.MESSAGE_RESERVED) {
    if (propMap != null) { // copy props
      import scala.collection.JavaConversions._
      for (prop <- propMap.entrySet) {
        val value = prop.getValue
        this.addProp(prop.getKey, if (value.isInstanceOf[String]) TextNode.valueOf(value.asInstanceOf[String])
        else value.asInstanceOf[JsonNode])
      }
    }

    /** The name of this message. */
    def getName: String = name

    /** The parameters of this message. */
    def getRequest: Schema = request

    /** The returned data. */
    def getResponse: Schema = Schema.create(NULL)

    /** Errors that might be thrown. */
    def getErrors: Schema = Schema.createUnion(new java.util.ArrayList[Schema])

    /** Returns true if this is a one-way message, with no response or errors. */
    def isOneWay = true

    override def toString: String = try {
      val writer = new StringWriter
      val gen = Schema.FACTORY.createJsonGenerator(writer)
      toJson(gen)
      gen.flush()
      writer.toString
    } catch {
      case e: IOException =>
        throw new AvroRuntimeException(e)
    }

    @throws[IOException]
    private[avro] def toJson(gen: JsonGenerator) = {
      gen.writeStartObject()
      if (doc != null) gen.writeStringField("doc", doc)
      writeProps(gen) // write out properties
      gen.writeFieldName("request")
      request.fieldsToJson(types, gen)
      toJson1(gen)
      gen.writeEndObject()
    }

    @throws[IOException]
    private[avro] def toJson1(gen: JsonGenerator) = {
      gen.writeStringField("response", "null")
      gen.writeBooleanField("one-way", true)
    }

    override def equals(o: Any): Boolean = {
      if (o == this) return true
      if (!o.isInstanceOf[Protocol#Message]) return false
      val that = o.asInstanceOf[Protocol#Message]
      this.name == that.name && this.request == that.request && props == that.props
    }

    override def hashCode: Int = name.hashCode + request.hashCode + props.hashCode

    def getDoc: String = doc
  }

  private class TwoWayMessage private(
    override val name: String,
    override val doc: String,
    override val propMap: Map[String, _],
    override val request: Schema,
    var response: Schema,
    var errors: Schema
  ) extends Protocol#Message(name, doc, propMap, request) {
    override def getResponse: Schema = response

    override def getErrors: Schema = errors

    override def isOneWay = false

    override def equals(o: Any): Boolean = {
      if (!super.equals(o)) return false
      if (!o.isInstanceOf[Protocol#TwoWayMessage]) return false
      val that = o.asInstanceOf[Protocol#TwoWayMessage]
      this.response == that.response && this.errors == that.errors
    }

    override def hashCode: Int = super.hashCode + response.hashCode + errors.hashCode

    @throws[IOException]
    override private[avro] def toJson1(gen: JsonGenerator) = {
      gen.writeFieldName("response")
      response.toJson(types, gen)
      val errs = errors.getTypes // elide system error
      if (errs.size > 1) {
        val union = Schema.createUnion(errs.subList(1, errs.size))
        gen.writeFieldName("errors")
        union.toJson(types, gen)
      }
    }
  }

  private var name:String = null
  private var namespace:String = null
  private var doc:String  = null
  private var types = new Schema.Names
  private val messages = new java.util.LinkedHashMap[String, Protocol#Message]
  private var md5:Array[Byte] = _

  def this(
    name: String,
    doc: String,
    namespace: String
  ) {
    this()
    super (Protocol.PROTOCOL_RESERVED)
    this.name = name
    this.doc = doc
    this.namespace = namespace
  }

  def this(
    name: String,
    namespace: String
  ) {
    this(name, null, namespace)
  }

  /** The name of this protocol. */
  def getName: String = name

  /** The namespace of this protocol.  Qualifies its name. */
  def getNamespace: String = namespace

  /** Doc string for this protocol. */
  def getDoc: String = doc

  /** The types of this protocol. */
  def getTypes: java.util.Collection[Schema] = types.values

  /** Returns the named type. */
  def getType(name: String): Schema = types.get(name)

  /** Set the types of this protocol. */
  def setTypes(newTypes: java.util.Collection[Schema]): Unit = {
    types = new Schema.Names
    import scala.collection.JavaConversions._
    for (s <- newTypes) {
      types.add(s)
    }
  }

  /** The messages of this protocol. */
  def getMessages: java.util.Map[String, Protocol#Message] = messages

  /** Create a one-way message. */
  @deprecated def createMessage(
    name: String,
    doc: String,
    request: Schema
  ): Protocol#Message = createMessage(name, doc, new java.util.LinkedHashMap[String, String], request)

  def createMessage[T](
    name: String,
    doc: String,
    propMap: java.util.Map[String, T],
    request: Schema
  ) = new Protocol#Message(name, doc, propMap, request)

  /** Create a two-way message. */
  @deprecated def createMessage(
    name: String,
    doc: String,
    request: Schema,
    response: Schema,
    errors: Schema
  ): Protocol#Message = createMessage(name, doc, new java.util.LinkedHashMap[String, String], request, response, errors)

  def createMessage[T](
    name: String,
    doc: String,
    propMap: java.util.Map[String, T],
    request: Schema,
    response: Schema,
    errors: Schema
  ) = new Protocol#TwoWayMessage(name, doc, propMap, request, response, errors)

  override def equals(o: Any): Boolean = {
    if (o == this) return true
    if (!o.isInstanceOf[Protocol]) return false
    val that = o.asInstanceOf[Protocol]
    this.name == that.name && this.namespace == that.namespace && this.types == that.types && this.messages == that.messages && this.props == that.props
  }

  override def hashCode: Int = name.hashCode + namespace.hashCode + types.hashCode + messages.hashCode + props.hashCode

  /** Render this as <a href="http://json.org/">JSON</a>. */
  override def toString: String = toString(false)

  /** Render this as <a href="http://json.org/">JSON</a>.
    *
    * @param pretty if true, pretty-print JSON.
    */
  def toString(pretty: Boolean): String = try {
    val writer = new StringWriter
    val gen = Schema.FACTORY.createJsonGenerator(writer)
    if (pretty) gen.useDefaultPrettyPrinter
    toJson(gen)
    gen.flush()
    writer.toString
  } catch {
    case e: IOException =>
      throw new AvroRuntimeException(e)
  }

  @throws[IOException]
  private[avro] def toJson(gen: JsonGenerator) = {
    types.space(namespace)
    gen.writeStartObject()
    gen.writeStringField("protocol", name)
    gen.writeStringField("namespace", namespace)
    if (doc != null) gen.writeStringField("doc", doc)
    writeProps(gen)
    gen.writeArrayFieldStart("types")
    val resolved = new Schema.Names(namespace)
    import scala.collection.JavaConversions._
    for (t <- types.values) {
      if (!resolved.contains(t)) t.toJson(resolved, gen)
    }
    gen.writeEndArray()
    gen.writeObjectFieldStart("messages")
    import scala.collection.JavaConversions._
    for (e <- messages.entrySet) {
      gen.writeFieldName(e.getKey)
      e.getValue.toJson(gen)
    }
    gen.writeEndObject()
    gen.writeEndObject()
  }

  /** Return the MD5 hash of the text of this protocol. */
  def getMD5: Array[Byte] = {
    if (md5 == null) try md5 = MessageDigest.getInstance("MD5").digest(this.toString.getBytes("UTF-8"))
    catch {
      case e: Exception =>
        throw new AvroRuntimeException(e)
    }
    md5
  }

  private def parse(json: JsonNode) = {
    parseNamespace(json)
    parseName(json)
    parseTypes(json)
    parseMessages(json)
    parseDoc(json)
    parseProps(json)
  }

  private def parseNamespace(json: JsonNode): Unit = {
    val nameNode = json.get("namespace")
    if (nameNode == null) return // no namespace defined
    this.namespace = nameNode.getTextValue
    types.space(this.namespace)
  }

  private def parseDoc(json: JsonNode) = this.doc = parseDocNode(json)

  private def parseDocNode(json: JsonNode): String = {
    val nameNode = json.get("doc")
    if (nameNode == null) return null // no doc defined
    nameNode.getTextValue
  }

  private def parseName(json: JsonNode) = {
    val nameNode = json.get("protocol")
    if (nameNode == null) throw new SchemaParseException("No protocol name specified: " + json)
    this.name = nameNode.getTextValue
  }

  private def parseTypes(json: JsonNode): Unit = {
    val defs = json.get("types")
    if (defs == null) return // no types defined
    if (!defs.isArray) throw new SchemaParseException("Types not an array: " + defs)
    import scala.collection.JavaConversions._
    for (t <- defs) {
      if (!t.isObject) throw new SchemaParseException("Type not an object: " + t)
      Schema.parse(t, types)
    }
  }

  private def parseProps(json: JsonNode) = {
    val i = json.getFieldNames
    while ( {
      i.hasNext
    }) {
      val p = i.next // add non-reserved as props
      if (!Protocol.PROTOCOL_RESERVED.contains(p)) this.addProp(p, json.get(p))
    }
  }

  private def parseMessages(json: JsonNode): Unit = {
    val defs = json.get("messages")
    if (defs == null) return
    // no messages defined
    val i = defs.getFieldNames
    while ( {
      i.hasNext
    }) {
      val prop = i.next
      this.messages.put(prop, parseMessage(prop, defs.get(prop)))
    }
  }

  private def parseMessage(
    messageName: String,
    json: JsonNode
  ): Protocol#Message = {
    val doc = parseDocNode(json)
    val mProps = new java.util.LinkedHashMap[String, JsonNode]
    val i = json.getFieldNames
    while ( {
      i.hasNext
    }) {
      val p = i.next
      if (!Protocol.MESSAGE_RESERVED.contains(p)) mProps.put(p, json.get(p))
    }
    val requestNode = json.get("request")
    if (requestNode == null || !requestNode.isArray) throw new SchemaParseException("No request specified: " + json)
    val fields = new java.util.ArrayList[Schema.Field]
    import scala.collection.JavaConversions._
    for (field <- requestNode) {
      val fieldNameNode = field.get("name")
      if (fieldNameNode == null) throw new SchemaParseException("No param name: " + field)
      val fieldTypeNode = field.get("type")
      if (fieldTypeNode == null) throw new SchemaParseException("No param type: " + field)
      val name = fieldNameNode.getTextValue
      var fieldDoc:String = null
      val fieldDocNode = field.get("doc")
      if (fieldDocNode != null) fieldDoc = fieldDocNode.getTextValue
      val newField = new Schema.Field(name, Schema.parse(fieldTypeNode, types), fieldDoc, field.get("default"))
      val aliases = Schema.parseAliases(field)
      if (aliases != null) { // add aliases
        import scala.collection.JavaConversions._
        for (alias <- aliases) {
          newField.addAlias(alias)
        }
      }
      val i = field.getFieldNames
      while ( {
        i.hasNext
      }) { // add properties
        val prop = i.next
        if (!Protocol.FIELD_RESERVED.contains(prop)) { // ignore reserved
          newField.addProp(prop, field.get(prop))
        }
      }
      fields.add(newField)
    }
    val request = Schema.createRecord(fields)
    var oneWay = false
    val oneWayNode = json.get("one-way")
    if (oneWayNode != null) {
      if (!oneWayNode.isBoolean) throw new SchemaParseException("one-way must be boolean: " + json)
      oneWay = oneWayNode.getBooleanValue
    }
    val responseNode = json.get("response")
    if (!oneWay && responseNode == null) throw new SchemaParseException("No response specified: " + json)
    val decls = json.get("errors")
    if (oneWay) {
      if (decls != null) throw new SchemaParseException("one-way can't have errors: " + json)
      if (responseNode != null && (Schema.parse(responseNode, types).getType != NULL)) throw new SchemaParseException("One way response must be null: " + json)
      return new Protocol#Message(messageName, doc, mProps, request)
    }
    val response = Schema.parse(responseNode, types)
    val errs = new java.util.ArrayList[Schema]
    errs.add(Protocol.SYSTEM_ERROR) // every method can throw
    if (decls != null) {
      if (!decls.isArray) throw new SchemaParseException("Errors not an array: " + json)
      import scala.collection.JavaConversions._
      for (decl <- decls) {
        val name = decl.getTextValue
        val schema = this.types.get(name)
        if (schema == null) throw new SchemaParseException("Undefined error: " + name)
        if (!schema.isError) throw new SchemaParseException("Not an error: " + name)
        errs.add(schema)
      }
    }
    new Protocol#TwoWayMessage(messageName, doc, mProps, request, response, Schema.createUnion(errs))
  }
}
