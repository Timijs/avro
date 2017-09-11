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

import java.io.File
import java.io.IOException
import java.io.InputStream
import java.io.StringReader
import java.io.StringWriter
import java.util
import java.util.Collections
import java.util.Locale

import avro.Schema.Field.ASCENDING
import org.apache.avro.util.internal.JacksonUtils
import org.codehaus.jackson.JsonFactory
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.JsonParseException
import org.codehaus.jackson.JsonParser
import org.codehaus.jackson.JsonGenerator
import org.codehaus.jackson.map.ObjectMapper
import org.codehaus.jackson.node.DoubleNode
import avro.SchemaType.Type
import avro.SchemaType._
import org.apache.avro.Schema.Field.Order

object Schema {
  private[avro] val FACTORY = new JsonFactory
  private[avro] val MAPPER = new ObjectMapper(FACTORY)
  private val NO_HASHCODE:Int = Integer.MIN_VALUE

  /** Create a schema for a primitive type. */
  def create(`type`: SchemaType.Type): Schema = `type` match {
    case STRING => new Schema.StringSchema
    case BYTES => new Schema.BytesSchema
    case INT => new Schema.IntSchema
    case LONG => new Schema.LongSchema
    case FLOAT => new Schema.FloatSchema
    case DOUBLE => new Schema.DoubleSchema
    case BOOLEAN => new Schema.BooleanSchema
    case NULL => new Schema.NullSchema
    case _ => throw new AvroRuntimeException("Can't create a: " + `type`)
  }

  private val SCHEMA_RESERVED = new java.util.HashSet[String]

  /** Create an anonymous record schema. */
  def createRecord(fields: java.util.List[Schema.Field]): Schema = {
    val result = createRecord(null, null, null, false)
    result.setFields(fields)
    result
  }

  /** Create a named record schema. */
  def createRecord(
    name: String,
    doc: String,
    namespace: String,
    isError: Boolean
  ) = new Schema.RecordSchema(new Schema.Name(name, namespace), doc, isError)

  /** Create a named record schema with fields already set. */
  def createRecord(
    name: String,
    doc: String,
    namespace: String,
    isError: Boolean,
    fields: java.util.List[Schema.Field]
  ) = new Schema.RecordSchema(new Schema.Name(name, namespace), doc, isError, fields)

  /** Create an enum schema. */
  def createEnum(
    name: String,
    doc: String,
    namespace: String,
    values: List[String]
  ) = new Schema.EnumSchema(new Schema.Name(name, namespace), doc, new Schema.LockableArrayList[String](values))

  /** Create an array schema. */
  def createArray(elementType: Schema) = new Schema.ArraySchema(elementType)

  /** Create a map schema. */
  def createMap(valueType: Schema) = new Schema.MapSchema(valueType)

  def createUnion(types: Schema*): Schema = new Schema.UnionSchema(types)

  def createFixed(
    name: String,
    doc: String,
    space: String,
    size: Int
  ) = new Schema.FixedSchema(new Schema.Name(name, space), doc, size)

  private val FIELD_RESERVED = new java.util.HashSet[String]

  /** A field within a record. */
  object Field {

    /** How values of this field should be ordered when sorting records. */
    sealed trait Order {override def toString = this.toString.toLowerCase(Locale.ENGLISH)}
    case object ASCENDING extends Order
    case object DESCENDING extends Order
    case object IGNORE extends Order

  }
  
  @deprecated
  class Field(
    val name: String,
    val schema: Schema,
    val doc: String,
    val defaultValue: JsonNode,
    val order: Field.Order
  )

  /** @deprecated use { @link #Field(String, Schema, String, Object, Order)}*/
  extends JsonProperties(FIELD_RESERVED) {
    this.name = validateName(name)
    this.defaultValue = validateDefault(name, schema, defaultValue)
    final private var name: String =
    null // name of the field.
    private val position: Int =
    -1
    final private var defaultValue: JsonNode =
    null
    private var aliases: util.Set[String] =
    null

    /** @deprecated use { @link #Field(String, Schema, String, Object)}*/
    def this(
      name: String,
      schema: Schema,
      doc: String,
      defaultValue: JsonNode
    ) {
      this(name, schema, doc, defaultValue, Field.Order.ASCENDING)
    }

    /**
      * @param defaultValue the default value for this field specified using the mapping
      *                     in { @link JsonProperties}
      */
    def this(
      name: String,
      schema: Schema,
      doc: String,
      defaultValue: Any,
      order: Field.Order
    ) {
      this(name, schema, doc, JacksonUtils.toJsonNode(defaultValue), order)
    }

    def this(
      name: String,
      schema: Schema,
      doc: String,
      defaultValue: Any
    ) {
      this(name, schema, doc, defaultValue, Field.Order.ASCENDING)
    }

    def name = name

    /** The position of this field within the record. */
    def pos = position

    /** This field's {@link Schema}. */
    def schema = schema

    /** Field's documentation within the record, if set. May return null. */
    def doc = doc

    /** @deprecated use { @link #defaultVal() }*/
    @deprecated def defaultValue = defaultValue

    /**
      * @return the default value for this field specified using the mapping
      *         in { @link JsonProperties}
      */
    def defaultVal = JacksonUtils.toObject(defaultValue, schema)

    def order = order

    @deprecated def props = getProps

    def addAlias(alias: String) = {
      if (aliases == null) this.aliases = new util.LinkedHashSet[String]
      aliases.add(alias)
    }

    /** Return the defined aliases as an unmodifieable Set. */
    def aliases: util.Set[String] = {
      if (aliases == null) return Collections.emptySet
      Collections.unmodifiableSet(aliases)
    }

    override def equals(other: Any)

    =
    {
      if (other eq this) return true
      if (!other.isInstanceOf[Schema.Field]) return false
      val that = other.asInstanceOf[Schema.Field]
      return (name == that.name) && (schema == that.schema) && defaultValueEquals(that.defaultValue) && (order eq that.order) && props == that.props
    }

    override def hashCode = {
      return name.hashCode + schema.computeHash
    }

    private def defaultValueEquals(thatDefaultValue: JsonNode)

    =
    {
      if (defaultValue == null) return thatDefaultValue == null
      if (thatDefaultValue == null) return false
      if (Double.isNaN(defaultValue.getDoubleValue)) return Double.isNaN(thatDefaultValue.getDoubleValue)
      return defaultValue == thatDefaultValue
    }

    override def toString

    =
    {
      return name + " type:" + schema.t + " pos:" + position
    }
  }

  private[avro] class Name(
    var name: String,
    var space: String
  ) {
    val lastDot: Int = name.lastIndexOf('.')
    if (lastDot < 0) { // unqualified name
      this.name = validateName(name)
    }
    else { // qualified name
      space = name.substring(0, lastDot) // get space from name
      this.name = validateName(name.substring(lastDot + 1, name.length))
    }
    if ("" == space) space = null
    this.full = if (this.space == null) this.name
    else s"${this.space}.${this.name}"
    final private var full:String = _

    override def equals(o: Any): Boolean = {
      if (o == this) return true
      if (!o.isInstanceOf[Schema.Name]) return false
      val that = o.asInstanceOf[Schema.Name]
      if (full == null) that.full == null
      else full == that.full
    }

    override def hashCode: Int = if (full == null) 0
    else full.hashCode

    override def toString: String = full

    @throws[IOException]
    def writeName(
      names: Schema.Names,
      gen: JsonGenerator
    ): Unit = {
      if (name != null) gen.writeStringField("name", name)
      if (space != null) if (!(space == names.space)) gen.writeStringField("namespace", space)
      else if (names.space != null) { // null within non-null
        gen.writeStringField("namespace", "")
      }
    }

    def getQualified(defaultSpace: String): String = if (space == null || space == defaultSpace) name
    else full
  }

  abstract private class NamedSchema(
    override val t: Type,
    val name: Schema.Name,
    val doc: String
  ) extends Schema(t) {
    if (PRIMITIVES.containsKey(name.full)) throw new AvroTypeException("Schemas may not be named after primitives: " + name.full)
    private[avro] var aliases = new java.util.LinkedHashSet[Schema.Name]

    override def getName: String = name.name

    override def getDoc: String = doc

    override def getNamespace: String = name.space

    override def getFullName: String = name.full

    override def addAlias(alias: String): Unit = addAlias(alias, null)

    override def addAlias(
      name: String,
      space: String
    ): Unit = {
      if (aliases == null) this.aliases = new java.util.LinkedHashSet[Schema.Name]
      if (space == null) space = this.name.space
      aliases.add(new Schema.Name(name, space))
    }

    override def getAliases: java.util.Set[String] = {
      val result = new java.util.LinkedHashSet[String]
      if (aliases != null) {
        import scala.collection.JavaConversions._
        for (alias <- aliases) {
          result.add(alias.full)
        }
      }
      result
    }

    @throws[IOException]
    def writeNameRef(
      names: Schema.Names,
      gen: JsonGenerator
    ): Boolean = {
      if (this == names.get(name)) {
        gen.writeString(name.getQualified(names.space))
        return true
      }
      else if (name.name != null) names.put(name, this)
      false
    }

    @throws[IOException]
    def writeName(
      names: Schema.Names,
      gen: JsonGenerator
    ): Unit = name.writeName(names, gen)

    def equalNames(that: Schema.NamedSchema): Boolean = this.name == that.name

    override private[avro] def computeHash = super.computeHash + name.hashCode

    @throws[IOException]
    def aliasesToJson(gen: JsonGenerator): Unit = {
      if (aliases == null || aliases.size == 0) return
      gen.writeFieldName("aliases")
      gen.writeStartArray()
      import scala.collection.JavaConversions._
      for (alias <- aliases) {
        gen.writeString(alias.getQualified(name.space))
      }
      gen.writeEndArray()
    }
  }

  private class SeenPair private(
    var s1: Any,
    var s2: Any
  ) {
    override def equals(o: Any): Boolean = {
      if (!o.isInstanceOf[Schema.SeenPair]) return false
      (this.s1 == o.asInstanceOf[Schema.SeenPair].s1) &&
      (this.s2 == o.asInstanceOf[Schema.SeenPair].s2)
    }

    override def hashCode: Int = System.identityHashCode(s1) + System.identityHashCode(s2)
  }

  private val SEEN_EQUALS = new ThreadLocal[java.util.Set[_]]() {
    override protected def initialValue = new java.util.HashSet[_]
  }
  private val SEEN_HASHCODE = new ThreadLocal[java.util.Map[_, _]]() {
    override protected def initialValue = new java.util.IdentityHashMap[_, _]
  }

  @SuppressWarnings(value = Array("unchecked")) private class RecordSchema extends Schema.NamedSchema {
    private var fields = null
    private var fieldMap = null
    final private var isError = false

    def this(
      name: Schema.Name,
      doc: String,
      isError: Boolean
    ) {
      this()
      super(RECORD, name, doc)
      this.isError = isError
    }

    def this(
      name: Schema.Name,
      doc: String,
      isError: Boolean,
      fields: java.util.List[Schema.Field]
    ) {
      this()
      super(RECORD, name, doc)
      this.isError = isError
      setFields(fields)
    }

    override def getField(fieldname: String): Schema.Field = {
      if (fieldMap == null) throw new AvroRuntimeException("Schema fields not set yet")
      fieldMap.get(fieldname)
    }

    override def getFields: java.util.List[Schema.Field] = {
      if (fields == null) throw new AvroRuntimeException("Schema fields not set yet")
      fields
    }

    override def setFields(fields: java.util.List[Schema.Field]): Unit = {
      if (this.fields != null) throw new AvroRuntimeException("Fields are already set")
      var i = 0
      fieldMap = new java.util.HashMap[String, Schema.Field]
      val ff = new Schema.LockableArrayList[_]
      import scala.collection.JavaConversions._
      for (f <- fields) {
        if (f.position != -1) throw new AvroRuntimeException("Field already used: " + f)
        f.position = {
          i += 1; i - 1
        }
        val existingField = fieldMap.put(f.name, f)
        if (existingField != null) throw new AvroRuntimeException(String.format("Duplicate field %s in record %s: %s and %s.", f.name, name, f, existingField))
        ff.add(f)
      }
      this.fields = ff.lock
      this.hashCode = NO_HASHCODE
    }

    override def equals(o: Any): Boolean = {
      if (o == this) return true
      if (!o.isInstanceOf[Schema.RecordSchema]) return false
      val that = o.asInstanceOf[Schema.RecordSchema]
      if (!equalCachedHash(that)) return false
      if (!equalNames(that)) return false
      if (!(props == that.props)) return false
      val seen = SEEN_EQUALS.get
      val here = new Schema.SeenPair(this, o)
      if (seen.contains(here)) return true
      // prevent stack overflow
      val first = seen.isEmpty
      try {
        seen.add(here)
        fields == o.asInstanceOf[Schema.RecordSchema].fields
      } finally if (first) seen.clear()
    }

    override private[avro] def computeHash: Int = {
      val seen = SEEN_HASHCODE.get
      if (seen.containsKey(this)) return 0
      val first = seen.isEmpty
      try {
        seen.put(this, this)
        super.computeHash + fields.hashCode
      } finally if (first) seen.clear()
    }

    @throws[IOException]
    override private[avro] def toJson(
      names: Schema.Names,
      gen: JsonGenerator
    ): Unit = {
      if (writeNameRef(names, gen)) return
      val savedSpace = names.space // save namespace
      gen.writeStartObject()
      gen.writeStringField("type", if (isError) "error"
      else "record")
      writeName(names, gen)
      names.space = name.space // set default namespace
      if (getDoc != null) gen.writeStringField("doc", getDoc)
      if (fields != null) {
        gen.writeFieldName("fields")
        fieldsToJson(names, gen)
      }
      writeProps(gen)
      aliasesToJson(gen)
      gen.writeEndObject()
      names.space = savedSpace // restore namespace
    }

    @throws[IOException]
    override private[avro] def fieldsToJson(
      names: Schema.Names,
      gen: JsonGenerator
    ) = {
      gen.writeStartArray()
      import scala.collection.JavaConversions._
      for (f <- fields) {
        gen.writeStartObject()
        gen.writeStringField("name", f.name)
        gen.writeFieldName("type")
        f.schema.toJson(names, gen)
        if (f.doc != null) gen.writeStringField("doc", f.doc)
        if (f.defaultValue != null) {
          gen.writeFieldName("default")
          gen.writeTree(f.defaultValue)
        }
        if (f.order ne Field.Order.ASCENDING) gen.writeStringField("order", f.order.name)
        if (f.aliases != null && f.aliases.size != 0) {
          gen.writeFieldName("aliases")
          gen.writeStartArray()
          import scala.collection.JavaConversions._
          for (alias <- f.aliases) {
            gen.writeString(alias)
          }
          gen.writeEndArray()
        }
        f.writeProps(gen)
        gen.writeEndObject()
      }
      gen.writeEndArray()
    }
  }

  private class EnumSchema(
    override val name: Schema.Name,
    override val doc: String,
    val symbols: Schema.LockableArrayList[String]
  ) extends Schema.NamedSchema(ENUM, name, doc) {
    this.symbols = symbols.lock
    this.ordinals = new java.lang.HashMap[String, Integer]
    var i = 0

    import scala.collection.JavaConversions._

    for (symbol <- symbols) {
      if (ordinals.put(validateName(symbol), {
        i += 1; i - 1
      }) != null) throw new SchemaParseException("Duplicate enum symbol: " + symbol)
    }
    final private var symbols = null
    final private var ordinals = null

    override def getEnumSymbols: java.util.List[String] = symbols

    override def hasEnumSymbol(symbol: String): Boolean = ordinals.containsKey(symbol)

    override def getEnumOrdinal(symbol: String): Int = ordinals.get(symbol)

    override def equals(o: Any): Boolean = {
      if (o == this) return true
      if (!o.isInstanceOf[Schema.EnumSchema]) return false
      val that = o.asInstanceOf[Schema.EnumSchema]
      equalCachedHash(that) && equalNames(that) && symbols == that.symbols && props == that.props
    }

    override private[avro] def computeHash = super.computeHash + symbols.hashCode

    @throws[IOException]
    override private[avro] def toJson(
      names: Schema.Names,
      gen: JsonGenerator
    ): Unit = {
      if (writeNameRef(names, gen)) return
      gen.writeStartObject()
      gen.writeStringField("type", "enum")
      writeName(names, gen)
      if (getDoc != null) gen.writeStringField("doc", getDoc)
      gen.writeArrayFieldStart("symbols")
      import scala.collection.JavaConversions._
      for (symbol <- symbols) {
        gen.writeString(symbol)
      }
      gen.writeEndArray()
      writeProps(gen)
      aliasesToJson(gen)
      gen.writeEndObject()
    }
  }

  private class ArraySchema(val elementType: Schema) extends Schema(ARRAY) {
    override def getElementType: Schema = elementType

    override def equals(o: Any): Boolean = {
      if (o == this) return true
      if (!o.isInstanceOf[Schema.ArraySchema]) return false
      val that = o.asInstanceOf[Schema.ArraySchema]
      equalCachedHash(that) && elementType == that.elementType && props == that.props
    }

    override private[avro] def computeHash = super.computeHash + elementType.computeHash

    @throws[IOException]
    override private[avro] def toJson(
      names: Schema.Names,
      gen: JsonGenerator
    ) = {
      gen.writeStartObject()
      gen.writeStringField("type", "array")
      gen.writeFieldName("items")
      elementType.toJson(names, gen)
      writeProps(gen)
      gen.writeEndObject()
    }
  }

  private class MapSchema(val valueType: Schema) extends Schema(MAP) {
    override def getValueType: Schema = valueType

    override def equals(o: Any): Boolean = {
      if (o == this) return true
      if (!o.isInstanceOf[Schema.MapSchema]) return false
      val that = o.asInstanceOf[Schema.MapSchema]
      equalCachedHash(that) && valueType == that.valueType && props == that.props
    }

    override private[avro] def computeHash = super.computeHash + valueType.computeHash

    @throws[IOException]
    override private[avro] def toJson(
      names: Schema.Names,
      gen: JsonGenerator
    ) = {
      gen.writeStartObject()
      gen.writeStringField("type", "map")
      gen.writeFieldName("values")
      valueType.toJson(names, gen)
      writeProps(gen)
      gen.writeEndObject()
    }
  }

  private class UnionSchema(val types: Seq[Schema]) extends Schema(UNION) {
    var index = 0

    import scala.collection.JavaConversions._

    for (t <- types) {
      if (t.getType == UNION) throw new AvroRuntimeException("Nested union: " + this)
      val name = t.getFullName
      if (name == null) throw new AvroRuntimeException("Nameless in union:" + this)
      if (indexByName.put(name, {
        index += 1; index - 1
      }) != null) throw new AvroRuntimeException("Duplicate in union:" + name)
    }
    final private val indexByName = new java.util.HashMap[String, Integer]

    override def getTypes: java.util.List[Schema] = types

    override def getIndexNamed(name: String): Integer = indexByName.get(name)

    override def equals(o: Any): Boolean = {
      if (o == this) return true
      if (!o.isInstanceOf[Schema.UnionSchema]) return false
      val that = o.asInstanceOf[Schema.UnionSchema]
      equalCachedHash(that) && types == that.types && props == that.props
    }

    override private[avro] def computeHash = {
      var hash = super.computeHash
      import scala.collection.JavaConversions._
      for (`t` <- types) {
        hash += t.computeHash
      }
      hash
    }

    override def addProp(
      name: String,
      value: String
    ) = throw new AvroRuntimeException("Can't set properties on a union: " + this)

    @throws[IOException]
    override private[avro] def toJson(
      names: Schema.Names,
      gen: JsonGenerator
    ) = {
      gen.writeStartArray()
      import scala.collection.JavaConversions._
      for (`t` <- types) {
        t.toJson(names, gen)
      }
      gen.writeEndArray()
    }
  }

  private class FixedSchema(
    override val name: Schema.Name,
    override val doc: String,
    val size: Int
  ) extends Schema.NamedSchema(FIXED, name, doc) {
    if (size < 0) throw new IllegalArgumentException("Invalid fixed size: " + size)

    override def getFixedSize: Int = size

    override def equals(o: Any): Boolean = {
      if (o == this) return true
      if (!o.isInstanceOf[Schema.FixedSchema]) return false
      val that = o.asInstanceOf[Schema.FixedSchema]
      equalCachedHash(that) && equalNames(that) && size == that.size && props == that.props
    }

    override private[avro] def computeHash = super.computeHash + size

    @throws[IOException]
    override private[avro] def toJson(
      names: Schema.Names,
      gen: JsonGenerator
    ): Unit = {
      if (writeNameRef(names, gen)) return
      gen.writeStartObject()
      gen.writeStringField("type", "fixed")
      writeName(names, gen)
      if (getDoc != null) gen.writeStringField("doc", getDoc)
      gen.writeNumberField("size", size)
      writeProps(gen)
      aliasesToJson(gen)
      gen.writeEndObject()
    }
  }

  private class StringSchema()  extends Schema(STRING)
  private class BytesSchema()   extends Schema(BYTES)
  private class IntSchema()     extends Schema(INT)
  private class LongSchema()    extends Schema(LONG)
  private class FloatSchema()   extends Schema(FLOAT)
  private class DoubleSchema()  extends Schema(DOUBLE)
  private class BooleanSchema() extends Schema(BOOLEAN)
  private class NullSchema()    extends Schema(NULL)

  /** A parser for JSON-format schemas.  Each named schema parsed with a parser
    * is added to the names known to the parser so that subsequently parsed
    * schemas may refer to it by name. */
  class Parser {
    private val names = new Schema.Names
    private var validate = true
    private var validateDefaults = true

    /** Adds the provided types to the set of defined, named types known to
      * this parser. */
    def addTypes(types: java.util.Map[String, Schema]): Schema.Parser = {
      import scala.collection.JavaConversions._
      for (s <- types.values) {
        names.add(s)
      }
      this
    }

    /** Returns the set of defined, named types known to this parser. */
    def getTypes: java.util.Map[String, Schema] = {
      val result = new java.util.LinkedHashMap[String, Schema]
      import scala.collection.JavaConversions._
      for (s <- names.values) {
        result.put(s.getFullName, s)
      }
      result
    }

    /** Enable or disable name validation. */
    def setValidate(validate: Boolean): Schema.Parser = {
      this.validate = validate
      this
    }

    /** True iff names are validated.  True by default. */
    def getValidate: Boolean = this.validate

    /** Enable or disable default value validation. */
    def setValidateDefaults(validateDefaults: Boolean): Schema.Parser = {
      this.validateDefaults = validateDefaults
      this
    }

    /** True iff default values are validated.  False by default. */
    def getValidateDefaults: Boolean = this.validateDefaults

    /** Parse a schema from the provided file.
      * If named, the schema is added to the names known to this parser. */
    @throws[IOException]
    def parse(file: File): Schema = parse(FACTORY.createJsonParser(file))

    /** Parse a schema from the provided stream.
      * If named, the schema is added to the names known to this parser.
      * The input stream stays open after the parsing. */
    @throws[IOException]
    def parse(in: InputStream): Schema = parse(FACTORY.createJsonParser(in).disable(JsonParser.Feature.AUTO_CLOSE_SOURCE))

    /** Read a schema from one or more json strings */
    def parse(
      s: String,
      more: String*
    ): Schema = {
      val b = new StringBuilder(s)
      for (part <- more) {
        b.append(part)
      }
      parse(b.toString)
    }

    /** Parse a schema from the provided string.
      * If named, the schema is added to the names known to this parser. */
    def parse(s: String): Schema = try parse(FACTORY.createJsonParser(new StringReader(s)))
    catch {
      case e: IOException =>
        throw new SchemaParseException(e)
    }

    @throws[IOException]
    private def parse(parser: JsonParser) = {
      val saved = validateNames.get
      val savedValidateDefaults = VALIDATE_DEFAULTS.get
      try {
        validateNames.set(validate)
        VALIDATE_DEFAULTS.set(validateDefaults)
        Schema.parse(MAPPER.readTree(parser), names)
      } catch {
        case e: JsonParseException =>
          throw new SchemaParseException(e)
      } finally {
        parser.close()
        validateNames.set(saved)
        VALIDATE_DEFAULTS.set(savedValidateDefaults)
      }
    }
  }

  /**
    * Constructs a Schema object from JSON schema file <tt>file</tt>.
    * The contents of <tt>file</tt> is expected to be in UTF-8 format.
    *
    * @param file The file to read the schema from.
    * @return The freshly built Schema.
    * @throws IOException        if there was trouble reading the contents
    * @throws JsonParseException if the contents are invalid
    * @deprecated use { @link Parser} instead.
    */
  @throws[IOException]
  def parse(file: File): Schema = new Schema.Parser().parse(file)

  /**
    * Constructs a Schema object from JSON schema stream <tt>in</tt>.
    * The contents of <tt>in</tt> is expected to be in UTF-8 format.
    *
    * @param in The input stream to read the schema from.
    * @return The freshly built Schema.
    * @throws IOException        if there was trouble reading the contents
    * @throws JsonParseException if the contents are invalid
    * @deprecated use { @link Parser} instead.
    */
  @throws[IOException]
  def parse(in: InputStream): Schema = new Schema.Parser().parse(in)

  /** Construct a schema from <a href="http://json.org/">JSON</a> text.
    *
    * @deprecated use { @link Parser} instead.
    */
  def parse(jsonSchema: String): Schema = new Schema.Parser().parse(jsonSchema)

  /** Construct a schema from <a href="http://json.org/">JSON</a> text.
    *
    * @param validate true if names should be validated, false if not.
    * @deprecated use { @link Parser} instead.
    */
  def parse(
    jsonSchema: String,
    validate: Boolean
  ): Schema = new Schema.Parser().setValidate(validate).parse(jsonSchema)

  private[avro] val PRIMITIVES = new java.util.HashMap[String, SchemaType.Type]

  private[avro] class Names() extends java.util.LinkedHashMap[Schema.Name, Schema] {
    var space:String = _ // default namespace
    def this(space2: String) {
      this()
      this.space = space2
    }

    def space(space: String): Unit = this.space = space

    override def get(o: Any): Schema = {
      var name:Name = _
      if (o.isInstanceOf[String]) {
        val primitive = PRIMITIVES.get(o.asInstanceOf[String])
        if (primitive != null) return Schema.create(primitive)
        name = new Schema.Name(o.asInstanceOf[String], space)
        if (!containsKey(name)) { // if not in default
          name = new Schema.Name(o.asInstanceOf[String], "") // try anonymous
        }
      }
      else name = o.asInstanceOf[Schema.Name]
      super.get(name)
    }

    def contains(schema: Schema): Boolean = get(schema.asInstanceOf[Schema.NamedSchema].name) != null

    def add(schema: Schema): Unit = put(schema.asInstanceOf[Schema.NamedSchema].name, schema)

    override def put(
      name: Schema.Name,
      schema: Schema
    ): Schema = {
      if (containsKey(name)) throw new SchemaParseException("Can't redefine: " + name)
      super.put(name, schema)
    }
  }

  private val validateNames = new ThreadLocal[Boolean]() {
    override protected def initialValue = true
  }

  private def validateName(name: String): String = {
    if (!validateNames.get) return name
    // not validating names
    val length = name.length
    if (length == 0) throw new SchemaParseException("Empty name")
    val first = name.charAt(0)
    if (!(Character.isLetter(first) || first == '_')) throw new SchemaParseException("Illegal initial character: " + name)
    var i = 1
    while ( {
      i < length
    }) {
      val c = name.charAt(i)
      if (!(Character.isLetterOrDigit(c) || c == '_')) throw new SchemaParseException("Illegal character in: " + name)
      {
        i += 1; i - 1
      }
    }
    name
  }

  private val VALIDATE_DEFAULTS = new ThreadLocal[Boolean]() {
    override protected def initialValue = false
  }

  private def validateDefault(
    fieldName: String,
    schema: Schema,
    defaultValue: JsonNode
  ) = {
    if (VALIDATE_DEFAULTS.get && (defaultValue != null) && !isValidDefault(schema, defaultValue)) { // invalid default
      val message = "Invalid default for field " + fieldName + ": " + defaultValue + " not a " + schema
      throw new AvroTypeException(message) // throw exception
    }
    defaultValue
  }

  private def isValidDefault(
    schema: Schema,
    defaultValue: JsonNode
  ): Boolean = {
    if (defaultValue == null) return false
    schema.getType match {
      case STRING =>
      case BYTES =>
      case ENUM =>
      case FIXED =>
        defaultValue.isTextual
      case INT =>
      case LONG =>
      case FLOAT =>
      case DOUBLE =>
        defaultValue.isNumber
      case BOOLEAN =>
        defaultValue.isBoolean
      case NULL =>
        defaultValue.isNull
      case ARRAY =>
        if (!defaultValue.isArray) return false
        import scala.collection.JavaConversions._
        for (element <- defaultValue) {
          if (!isValidDefault(schema.getElementType, element)) return false
        }
        true
      case MAP =>
        if (!defaultValue.isObject) return false
        import scala.collection.JavaConversions._
        for (value <- defaultValue) {
          if (!isValidDefault(schema.getValueType, value)) return false
        }
        true
      case UNION => // union default: first branch
        isValidDefault(schema.getTypes.get(0), defaultValue)
      case RECORD =>
        if (!defaultValue.isObject) return false
        import scala.collection.JavaConversions._
        for (field <- schema.getFields) {
          if (!isValidDefault(field.schema, if (defaultValue.has(field.name)) {
            defaultValue.get(field.name)
          }
          else {
            field.defaultValue
          })) return false
        }
        true
      case _ =>
        false
    }
  }

  /** @see #parse(String) */
  private[avro] def parse(
    schema: JsonNode,
    names: Schema.Names
  ) = if (schema.isTextual) { // name
    val result = names.get(schema.getTextValue)
    if (result == null) throw new SchemaParseException("Undefined name: " + schema)
    result
  }
  else if (schema.isObject) {
    var result = null
    val `type` = getRequiredText(schema, "type", "No type")
    var name = null
    val savedSpace = names.space
    var doc = null
    if (`type` == "record" || `type` == "error" || `type` == "enum" || `type` == "fixed") {
      var space = getOptionalText(schema, "namespace")
      doc = getOptionalText(schema, "doc")
      if (space == null) space = names.space
      name = new Schema.Name(getRequiredText(schema, "name", "No name in schema"), space)
      if (name.space != null) names.space(name.space)
    }
    if (PRIMITIVES.containsKey(`type`)) { // primitive
      result = create(PRIMITIVES.get(`type`))
    }
    else if (`type` == "record" || `type` == "error") { // record
      val fields = new java.util.ArrayList[Schema.Field]
      result = new Schema.RecordSchema(name, doc, `type` == "error")
      if (name != null) names.add(result)
      val fieldsNode = schema.get("fields")
      if (fieldsNode == null || !fieldsNode.isArray) throw new SchemaParseException("Record has no fields: " + schema)
      import scala.collection.JavaConversions._
      for (field <- fieldsNode) {
        val fieldName = getRequiredText(field, "name", "No field name")
        val fieldDoc = getOptionalText(field, "doc")
        val fieldTypeNode = field.get("type")
        if (fieldTypeNode == null) throw new SchemaParseException("No field type: " + field)
        if (fieldTypeNode.isTextual && names.get(fieldTypeNode.getTextValue) == null) throw new SchemaParseException(fieldTypeNode + " is not a defined name." + " The type of the \"" + fieldName + "\" field must be" + " a defined name or a {\"type\": ...} expression.")
        val fieldSchema = parse(fieldTypeNode, names)
        var order = ASCENDING
        val orderNode = field.get("order")
        if (orderNode != null) order = Order.valueOf(orderNode.getTextValue.toUpperCase(Locale.ENGLISH))
        var defaultValue = field.get("default")
        if ((defaultValue != null && FLOAT == fieldSchema.getType) ||
            DOUBLE == fieldSchema.getType && defaultValue.isTextual)
          defaultValue = new DoubleNode(java.lang.Double.valueOf(defaultValue.getTextValue))
        val f = new Schema.Field(fieldName, fieldSchema, fieldDoc, defaultValue, order)
        val i = field.getFieldNames
        while ( {
          i.hasNext
        }) { // add field props
          val prop = i.next
          if (!FIELD_RESERVED.contains(prop)) f.addProp(prop, field.get(prop))
        }
        f.aliases = parseAliases(field)
        fields.add(f)
      }
      result.setFields(fields)
    }
    else if (`type` == "enum") { // enum
      val symbolsNode = schema.get("symbols")
      if (symbolsNode == null || !symbolsNode.isArray) throw new SchemaParseException("Enum has no symbols: " + schema)
      val symbols = new Schema.LockableArrayList[String](symbolsNode.size)
      import scala.collection.JavaConversions._
      for (n <- symbolsNode) {
        symbols.add(n.getTextValue)
      }
      result = new Schema.EnumSchema(name, doc, symbols)
      if (name != null) names.add(result)
    }
    else if (`type` == "array") { // array
      val itemsNode = schema.get("items")
      if (itemsNode == null) throw new SchemaParseException("Array has no items type: " + schema)
      result = new Schema.ArraySchema(parse(itemsNode, names))
    }
    else if (`type` == "map") { // map
      val valuesNode = schema.get("values")
      if (valuesNode == null) throw new SchemaParseException("Map has no values type: " + schema)
      result = new Schema.MapSchema(parse(valuesNode, names))
    }
    else if (`type` == "fixed") { // fixed
      val sizeNode = schema.get("size")
      if (sizeNode == null || !sizeNode.isInt) throw new SchemaParseException("Invalid or no size: " + schema)
      result = new Schema.FixedSchema(name, doc, sizeNode.getIntValue)
      if (name != null) names.add(result)
    }
    else throw new SchemaParseException("Type not supported: " + `type`)
    val i = schema.getFieldNames
    while ( {
      i.hasNext
    }) { // add properties
      val prop = i.next
      if (!SCHEMA_RESERVED.contains(prop)) { // ignore reserved
        result.addProp(prop, schema.get(prop))
      }
    }
    // parse logical type if present
    result.logicalType = LogicalTypes.fromSchemaIgnoreInvalid(result)
    names.space(savedSpace) // restore space
    if (result.isInstanceOf[Schema.NamedSchema]) {
      val aliases = parseAliases(schema)
      if (aliases != null) { // add aliases
        import scala.collection.JavaConversions._
        for (alias <- aliases) {
          result.addAlias(alias)
        }
      }
    }
    result
  }
  else if (schema.isArray) { // union
    val types = new Schema.LockableArrayList[Schema](schema.size)
    import scala.collection.JavaConversions._
    for (typeNode <- schema) {
      types.add(parse(typeNode, names))
    }
    new Schema.UnionSchema(types)
  }
  else throw new SchemaParseException("Schema not yet supported: " + schema)

  private[avro] def parseAliases(node: JsonNode): java.util.Set[String] = {
    val aliasesNode = node.get("aliases")
    if (aliasesNode == null) return null
    if (!aliasesNode.isArray) throw new SchemaParseException("aliases not an array: " + node)
    val aliases = new java.util.LinkedHashSet[String]
    import scala.collection.JavaConversions._
    for (aliasNode <- aliasesNode) {
      if (!aliasNode.isTextual) throw new SchemaParseException("alias not a string: " + aliasNode)
      aliases.add(aliasNode.getTextValue)
    }
    aliases
  }

  /** Extracts text value associated to key from the container JsonNode,
    * and throws {@link SchemaParseException} if it doesn't exist.
    *
    * @param container Container where to find key.
    * @param key       Key to look for in container.
    * @param error     String to prepend to the SchemaParseException.
    */
  private def getRequiredText(
    container: JsonNode,
    key: String,
    error: String
  ) = {
    val out = getOptionalText(container, key)
    if (null == out) throw new SchemaParseException(error + ": " + container)
    out
  }

  /** Extracts text value associated to key from the container JsonNode. */
  private def getOptionalText(
    container: JsonNode,
    key: String
  ) = {
    val jsonNode = container.get(key)
    if (jsonNode != null) jsonNode.getTextValue
    else null
  }

  /**
    * Parses a string as Json.
    *
    * @deprecated use { @link org.apache.avro.data.Json#parseJson(String)}
    */
  @deprecated def parseJson(s: String): JsonNode = try MAPPER.readTree(FACTORY.createJsonParser(new StringReader(s)))
  catch {
    case e: JsonParseException =>
      throw new RuntimeException(e)
    case e: IOException =>
      throw new RuntimeException(e)
  }

  /** Rewrite a writer's schema using the aliases from a reader's schema.  This
    * permits reading records, enums and fixed schemas whose names have changed,
    * and records whose field names have changed.  The returned schema always
    * contains the same data elements in the same order, but with possibly
    * different names. */
  def applyAliases(
    writer: Schema,
    reader: Schema
  ): Schema = {
    if (writer eq reader) return writer
    // same schema
    // create indexes of names
    val seen = new java.util.IdentityHashMap[Schema, Schema](1)
    val aliases = new java.util.HashMap[Schema.Name, Schema.Name](1)
    val fieldAliases = new java.util.HashMap[Schema.Name, java.util.Map[String, String]](1)
    getAliases(reader, seen, aliases, fieldAliases)
    if (aliases.size == 0 && fieldAliases.size == 0) return writer // no aliases
    seen.clear()
    applyAliases(writer, seen, aliases, fieldAliases)
  }

  private def applyAliases(
    s: Schema,
    seen: java.util.Map[Schema, Schema],
    aliases: java.util.Map[Schema.Name, Schema.Name],
    fieldAliases: java.util.Map[Schema.Name, java.util.Map[String, String]]
  ): Schema = {
    var name = if (s.isInstanceOf[Schema.NamedSchema]) s.asInstanceOf[Schema.NamedSchema].name
    else null
    var result = s
    s.getType match {
      case RECORD =>
        if (seen.containsKey(s)) return seen.get(s) // break loops
        if (aliases.containsKey(name)) name = aliases.get(name)
        result = Schema.createRecord(name.full, s.getDoc, null, s.isError)
        seen.put(s, result)
        val newFields = new java.util.ArrayList[Schema.Field]
        import scala.collection.JavaConversions._
        for (f <- s.getFields) {
          val fSchema = applyAliases(f.schema, seen, aliases, fieldAliases)
          val fName = getFieldAlias(name, f.name, fieldAliases)
          val newF = new Schema.Field(fName, fSchema, f.doc, f.defaultValue, f.order)
          newF.props.putAll(f.props) // copy props
          newFields.add(newF)
        }
        result.setFields(newFields)
        break //todo: break is not supported
      case ENUM =>
        if (aliases.containsKey(name)) result = Schema.createEnum(aliases.get(name).full, s.getDoc, null, s.getEnumSymbols)
        break //todo: break is not supported
      case ARRAY =>
        val e = applyAliases(s.getElementType, seen, aliases, fieldAliases)
        if (e ne s.getElementType) result = Schema.createArray(e)
        break //todo: break is not supported
      case MAP =>
        val v = applyAliases(s.getValueType, seen, aliases, fieldAliases)
        if (v ne s.getValueType) result = Schema.createMap(v)
        break //todo: break is not supported
      case UNION =>
        val types = new java.util.ArrayList[Schema]
        import scala.collection.JavaConversions._
        for (branch <- s.getTypes) {
          types.add(applyAliases(branch, seen, aliases, fieldAliases))
        }
        result = Schema.createUnion(types)
        break //todo: break is not supported
      case FIXED =>
        if (aliases.containsKey(name)) result = Schema.createFixed(aliases.get(name).full, s.getDoc, null, s.getFixedSize)
        break //todo: break is not supported
    }
    if (result ne s) result.props.putAll(s.props)
    result
  }

  private def getAliases(
    schema: Schema,
    seen: java.util.Map[Schema, Schema],
    aliases: java.util.Map[Schema.Name, Schema.Name],
    fieldAliases: java.util.Map[Schema.Name, java.util.Map[String, String]]
  ): Unit = {
    if (schema.isInstanceOf[Schema.NamedSchema]) {
      val namedSchema = schema.asInstanceOf[Schema.NamedSchema]
      if (namedSchema.aliases != null) {
        import scala.collection.JavaConversions._
        for (alias <- namedSchema.aliases) {
          aliases.put(alias, namedSchema.name)
        }
      }
    }
    schema.getType match {
      case RECORD =>
        if (seen.containsKey(schema)) return
        seen.put(schema, schema)
        val record = schema.asInstanceOf[Schema.RecordSchema]
        import scala.collection.JavaConversions._
        for (field <- schema.getFields) {
          if (field.aliases != null) {
            import scala.collection.JavaConversions._
            for (fieldAlias <- field.aliases) {
              var recordAliases = fieldAliases.get(record.name)
              if (recordAliases == null) fieldAliases.put(record.name, recordAliases = new util.HashMap[String, String])
              recordAliases.put(fieldAlias, field.name)
            }
          }
          getAliases(field.schema, seen, aliases, fieldAliases)
        }
        if (record.aliases != null && fieldAliases.containsKey(record.name)) {
          import scala.collection.JavaConversions._
          for (recordAlias <- record.aliases) {
            fieldAliases.put(recordAlias, fieldAliases.get(record.name))
          }
        }
        break //todo: break is not supported
      case ARRAY =>
        getAliases(schema.getElementType, seen, aliases, fieldAliases)
        break //todo: break is not supported
      case MAP =>
        getAliases(schema.getValueType, seen, aliases, fieldAliases)
        break //todo: break is not supported
      case UNION =>
        import scala.collection.JavaConversions._
        for (s <- schema.getTypes) {
          getAliases(s, seen, aliases, fieldAliases)
        }
        break //todo: break is not supported
    }
  }

  private def getFieldAlias(
    record: Schema.Name,
    field: String,
    fieldAliases: java.util.Map[Schema.Name, java.util.Map[String, String]]
  ): String = {
    val recordAliases = fieldAliases.get(record)
    if (recordAliases == null) return field
    val alias = recordAliases.get(field)
    if (alias == null) return field
    alias
  }

  /**
    * No change is permitted on LockableArrayList once lock() has been
    * called on it.
    *
    * @param < E>
    */
  /*
     * This class keeps a boolean variable <tt>locked</tt> which is set
     * to <tt>true</tt> in the lock() method. It's legal to call
     * lock() any number of times. Any lock() other than the first one
     * is a no-op.
     *
     * This class throws <tt>IllegalStateException</tt> if a mutating
     * operation is performed after being locked. Since modifications through
     * iterator also use the list's mutating operations, this effectively
     * blocks all modifications.
     */ @SerialVersionUID(1L)
  private[avro] class LockableArrayList[E]() extends util.ArrayList[E] {
    private var locked = false

    def this(size: Int) {
      this()
      super (size)
    }

    def this(types: util.List[E]) {
      this()
      super (types)
    }

    def this(types: E*) {
      this()
      super (types.length)
      Collections.addAll(this, types)
    }

    def lock: util.List[E] = {
      locked = true
      this
    }

    private def ensureUnlocked() = if (locked) throw new IllegalStateException

    override def add(e: E): Boolean = {
      ensureUnlocked()
      super.add(e)
    }

    override def remove(o: Any): Boolean = {
      ensureUnlocked()
      super.remove(o)
    }

    override def remove(index: Int): E = {
      ensureUnlocked()
      super.remove(index)
    }

    override def addAll(c: util.Collection[_ <: E]): Boolean = {
      ensureUnlocked()
      super.addAll(c)
    }

    override def addAll(
      index: Int,
      c: util.Collection[_ <: E]
    ): Boolean = {
      ensureUnlocked()
      super.addAll(index, c)
    }

    override def removeAll(c: util.Collection[_]): Boolean = {
      ensureUnlocked()
      super.removeAll(c)
    }

    override def retainAll(c: util.Collection[_]): Boolean = {
      ensureUnlocked()
      super.retainAll(c)
    }

    override def clear(): Unit = {
      ensureUnlocked()
      super.clear()
    }
  }

  try FACTORY.enable(JsonParser.Feature.ALLOW_COMMENTS)
  FACTORY.setCodec(MAPPER)
  Collections.addAll(SCHEMA_RESERVED, "doc", "fields", "items", "name", "namespace", "size", "symbols", "values", "type", "aliases")
  Collections.addAll(FIELD_RESERVED, "default", "doc", "name", "order", "type", "aliases")
  PRIMITIVES.put("string", STRING)
  PRIMITIVES.put("bytes", BYTES)
  PRIMITIVES.put("int", INT)
  PRIMITIVES.put("long", LONG)
  PRIMITIVES.put("float", FLOAT)
  PRIMITIVES.put("double", DOUBLE)
  PRIMITIVES.put("boolean", BOOLEAN)
  PRIMITIVES.put("null", NULL)
}

abstract class Schema private[avro](
  val t: Type
) extends JsonProperties(Schema.SCHEMA_RESERVED) {
  private var logicalType = null
  override private[avro] var hashCode = Schema.NO_HASHCODE

  override def addProp(
    name: String,
    value: JsonNode
  ): Unit = {
    super.addProp(name, value)
  }

  override def addProp(
    name: String,
    value: Any
  ): Unit = {
    super.addProp(name, value)
  }

  def getLogicalType: LogicalType = logicalType

  private[avro] def setLogicalType(logicalType: LogicalType) = this.logicalType = logicalType

  /** Return the type of this schema. */
  def getType: SchemaType.Type = t

  /**
    * If this is a record, returns the Field with the
    * given name <tt>fieldName</tt>. If there is no field by that name, a
    * <tt>null</tt> is returned.
    */
  def getField(fieldname: String) = throw new AvroRuntimeException("Not a record: " + this)

  /**
    * If this is a record, returns the fields in it. The returned
    * list is in the order of their positions.
    */
  def getFields = throw new AvroRuntimeException("Not a record: " + this)

  /**
    * If this is a record, set its fields. The fields can be set
    * only once in a schema.
    */
  def setFields(fields: java.util.List[Schema.Field]) = throw new AvroRuntimeException("Not a record: " + this)

  /** If this is an enum, return its symbols. */
  def getEnumSymbols = throw new AvroRuntimeException("Not an enum: " + this)

  /** If this is an enum, return a symbol's ordinal value. */
  def getEnumOrdinal(symbol: String) = throw new AvroRuntimeException("Not an enum: " + this)

  /** If this is an enum, returns true if it contains given symbol. */
  def hasEnumSymbol(symbol: String) = throw new AvroRuntimeException("Not an enum: " + this)

  /** If this is a record, enum or fixed, returns its name, otherwise the name
    * of the primitive type. */
  def getName: String = t.name

  /** If this is a record, enum, or fixed, returns its docstring,
    * if available.  Otherwise, returns null. */
  def getDoc = null

  /** If this is a record, enum or fixed, returns its namespace, if any. */
  def getNamespace = throw new AvroRuntimeException("Not a named type: " + this)

  /** If this is a record, enum or fixed, returns its namespace-qualified name,
    * otherwise returns the name of the primitive type. */
  def getFullName: String = getName

  /** If this is a record, enum or fixed, add an alias. */
  def addAlias(alias: String) = throw new AvroRuntimeException("Not a named type: " + this)

  def addAlias(
    alias: String,
    space: String
  ) = throw new AvroRuntimeException("Not a named type: " + this)

  /** If this is a record, enum or fixed, return its aliases, if any. */
  def getAliases = throw new AvroRuntimeException("Not a named type: " + this)

  /** Returns true if this record is an error type. */
  def isError = throw new AvroRuntimeException("Not a record: " + this)

  /** If this is an array, returns its element type. */
  def getElementType = throw new AvroRuntimeException("Not an array: " + this)

  /** If this is a map, returns its value type. */
  def getValueType = throw new AvroRuntimeException("Not a map: " + this)

  /** If this is a union, returns its types. */
  def getTypes = throw new AvroRuntimeException("Not a union: " + this)

  /** If this is a union, return the branch with the provided full name. */
  def getIndexNamed(name: String) = throw new AvroRuntimeException("Not a union: " + this)

  /** If this is fixed, returns its size. */
  def getFixedSize = throw new AvroRuntimeException("Not fixed: " + this)

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
    toJson(new Schema.Names, gen)
    gen.flush()
    writer.toString
  } catch {
    case e: IOException =>
      throw new AvroRuntimeException(e)
  }

  @throws[IOException]
  private[avro] def toJson(
    names: Schema.Names,
    gen: JsonGenerator
  ) = if (props.size == 0) { // no props defined
    gen.writeString(getName) // just write name
  }
  else {
    gen.writeStartObject()
    gen.writeStringField("type", getName)
    writeProps(gen)
    gen.writeEndObject()
  }

  @throws[IOException]
  private[avro] def fieldsToJson(
    names: Schema.Names,
    gen: JsonGenerator
  ) = throw new AvroRuntimeException("Not a record: " + this)

  override def equals(o: Any): Boolean = {
    if (o eq this) return true
    if (!o.isInstanceOf[Schema]) return false
    val that = o.asInstanceOf[Schema]
    if (!(this.t eq that.t)) return false
    equalCachedHash(that) && props == that.props
  }

  override final def hashCode: Int = {
    if (hashCode == Schema.NO_HASHCODE) hashCode = computeHash
    hashCode
  }

  private[avro] def computeHash = getType.hashCode + props.hashCode

  final private[avro] def equalCachedHash(other: Schema) = (hashCode == other.hashCode) || (hashCode == Schema.NO_HASHCODE) || (other.hashCode == Schema.NO_HASHCODE)
}
