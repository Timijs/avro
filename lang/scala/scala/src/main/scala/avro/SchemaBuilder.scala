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

import java.io.IOException
import java.nio.ByteBuffer
import java.util

import avro.Schema.Field.{ASCENDING, DESCENDING, IGNORE, Order}
import avro.SchemaType._
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.io.JsonStringEncoder
import org.codehaus.jackson.map.ObjectMapper
import org.codehaus.jackson.node.TextNode

object SchemaBuilder{

  def builder = new TypeBuilder[Schema](new SchemaCompletion, new NameContext)

  def builder(namespace: String) = new TypeBuilder[Schema](new SchemaCompletion, new NameContext().namespace(namespace))

  def record(name: String): RecordBuilder[Schema] = builder.record(name)

  def enumeration(name: String): EnumBuilder[Schema] = builder.enumeration(name)

  def fixed(name: String): FixedBuilder[Schema] = builder.fixed(name)

  def array: ArrayBuilder[Schema] = builder.array

  def map: MapBuilder[Schema] = builder.map

  def unionOf: BaseTypeBuilder[UnionAccumulator[Schema]] = builder.unionOf

  def nullable: BaseTypeBuilder[Schema] = builder.nullable

  abstract class PropBuilder[S <: PropBuilder[S]] { self =>
    private var props:java.util.HashMap[String, JsonNode] = _

    final def prop(
      name: String,
      `val`: String
    ) = prop(name, TextNode.valueOf(`val`))

    // for internal use by the Parser
    final private[avro] def prop(
      name: String,
      `val`: JsonNode
    ) = {
      if (!hasProps) props = new java.util.HashMap[String, JsonNode]
      props.put(name, `val`)
      self
    }

    protected def hasProps = props != null

    final private[avro] def addPropsTo[T <: JsonProperties](jsonable: T) = {
      if (hasProps) {
        import scala.collection.JavaConversions._
        for (prop <- props.entrySet) {
          jsonable.addProp(prop.getKey, prop.getValue)
        }
      }
      jsonable
    }
  }

  abstract class NamedBuilder[S <: NamedBuilder[S]](
    val names: NameContext,
    val name: String
  ) extends PropBuilder[S] {
    checkRequired(name, "Type must have a name")
    protected var doc:String = _
    private var aliases:Seq[String] = _

    final private[avro] def addAliasesTo(schema: Schema) = {
      if (null != aliases) for (alias <- aliases) {
        schema.addAlias(alias)
      }
      schema
    }

    final private[avro] def addAliasesTo(field: Schema.Field) = {
      if (null != aliases) for (alias <- aliases) {
        field.addAlias(alias)
      }
      field
    }
  }

  abstract class NamespacedBuilder[R, S <: NamespacedBuilder[R, S]] protected(
    val context: Completion[R],
    override val names: NameContext,
    override val name: String
  ) extends NamedBuilder[S](names, name) {
    private var namespace:String = _

    final private[avro] def space: String = {
      if (null == namespace) return names.namespace
      namespace
    }

    final private[avro] def completeSchema(schema: Schema) = {
      addPropsTo(schema)
      addAliasesTo(schema)
      names.put(schema)
      schema
    }

    final private[avro] def context = context
  }

  abstract private class PrimitiveBuilder[R, P <: PrimitiveBuilder[R, P]](
    val context: Completion[R],
    val names: NameContext,
    val `type`: Type
  ) extends PropBuilder[P] {
    final private var immutable:Schema = _
    this.immutable = names.getFullname(`type`.getName)

    protected def end:R = {
      var schema = immutable
      if (super.hasProps) {
        schema = Schema.create(immutable.getType)
        addPropsTo(schema)
      }
      context.complete(schema)
    }
  }

  final class BooleanBuilder[R](
    override val context: Completion[R],
    override val names: NameContext
  ) extends PrimitiveBuilder[R, BooleanBuilder[R]](context, names, BOOLEAN) {

    /** complete building this type, return control to context **/
    def endBoolean: R = super.end
  }

  final class IntBuilder[R](
    override val context: Completion[R],
    override val names: NameContext
  ) extends PrimitiveBuilder[R, IntBuilder[R]](context, names, INT) {

    def endInt: R = super.end
  }

  final class LongBuilder[R](
    override val context: Completion[R],
    override val names: NameContext
  ) extends PrimitiveBuilder[R, LongBuilder[R]](context, names, LONG) {

    def endLong: R = super.end
  }

  final class FloatBuilder[R](
    override val context: Completion[R],
    override val names: NameContext
  ) extends PrimitiveBuilder[R, FloatBuilder[R]](context, names, FLOAT) {

    def endFloat: R = super.end
  }

  final class DoubleBuilder[R](
    override val context: Completion[R],
    override val names: NameContext
  ) extends PrimitiveBuilder[R, DoubleBuilder[R]](context, names, DOUBLE) {

    def endDouble: R = super.end
  }

  final class StringBldr[R](
    override val context: Completion[R],
    override val names: NameContext
  ) extends PrimitiveBuilder[R, StringBldr[R]](context, names, STRING) {

    def endString: R = super.end
  }

  final class BytesBuilder[R](
    override val context: Completion[R],
    override val names: NameContext
  ) extends PrimitiveBuilder[R, BytesBuilder[R]](context, names, BYTES) {

    def endBytes: R = super.end
  }

  final class NullBuilder[R](
    override val context: Completion[R],
    override val names: NameContext
  ) extends PrimitiveBuilder[R, NullBuilder[R]](context, names, NULL) {

    def endNull: R = super.end
  }

  final class FixedBuilder[R](
    override val context: Completion[R],
    override val names: NameContext,
    override val name: String
  ) extends NamespacedBuilder[R, FixedBuilder[R]](context, names, name) {

    /** Configure this fixed type's size, and end its configuration. **/
    def size(size: Int): R = {
      val schema = Schema.createFixed(name, super.doc, space, size)
      completeSchema(schema)
      context.complete(schema)
    }
  }

  final class EnumBuilder[R](
    override val context: Completion[R],
    override val names: NameContext,
    override val name: String
  ) extends NamespacedBuilder[R, EnumBuilder[R]](context, names, name) {

    /** Configure this enum type's symbols, and end its configuration. **/
    def symbols(symbols: String*): R = {
      val schema = Schema.createEnum(name, doc, space, java.util.Arrays.asList(symbols))
      completeSchema(schema)
      context.complete(schema)
    }
  }

  final class MapBuilder[R](
    val context: Completion[R],
    val names: NameContext
  ) extends PropBuilder[MapBuilder[R]] {

    def values = new TypeBuilder[R](new MapCompletion[R](this, context), names)
    def values(valueSchema: Schema): R = new MapCompletion[R](this, context).complete(valueSchema)
  }

  final class ArrayBuilder[R](
    val context: Completion[R],
    val names: NameContext
  ) extends PropBuilder[ArrayBuilder[R]] {
    def items = new TypeBuilder[R](new ArrayCompletion[R](this, context), names)

    def items(itemsSchema: Schema): R = new ArrayCompletion[R](this, context).complete(itemsSchema)
  }

  private object NameContext {
    private val PRIMITIVES = new java.util.HashSet[String]
    try PRIMITIVES.add("null")
    PRIMITIVES.add("boolean")
    PRIMITIVES.add("int")
    PRIMITIVES.add("long")
    PRIMITIVES.add("float")
    PRIMITIVES.add("double")
    PRIMITIVES.add("bytes")
    PRIMITIVES.add("string")
  }

  class NameContext{
    this.schemas = new java.util.HashMap[String, Schema]
    this.namespace = null
    schemas.put("null", Schema.create(NULL))
    schemas.put("boolean", Schema.create(BOOLEAN))
    schemas.put("int", Schema.create(INT))
    schemas.put("long", Schema.create(LONG))
    schemas.put("float", Schema.create(FLOAT))
    schemas.put("double", Schema.create(DOUBLE))
    schemas.put("bytes", Schema.create(BYTES))
    schemas.put("string", Schema.create(STRING))
    final private var schemas:java.util.HashMap[String, Schema] = _
    final var namespace:String = _

    def this(
      schemas: java.util.HashMap[String, Schema],
      namespace: String
    ) {
      this()
      this.schemas = schemas
      this.namespace = if ("" == namespace) null
      else namespace
    }

    private def namespace(namespace: String) = new NameContext(schemas, namespace)

    def get(
      name: String,
      namespace: String
    ) = getFullname(resolveName(name, namespace))

    def getFullname(fullName: String) = {
      val schema = schemas.get(fullName)
      if (schema == null) throw new SchemaParseException("Undefined name: " + fullName)
      schema
    }

    private def put(schema: Schema) = {
      val fullName = schema.getFullName
      if (schemas.containsKey(fullName)) throw new SchemaParseException("Can't redefine: " + fullName)
      schemas.put(fullName, schema)
    }

    private def resolveName(
      name: String,
      space: String
    ): String = {
      if (NameContext.PRIMITIVES.contains(name) && space == null) return name
      val lastDot = name.lastIndexOf('.')
      if (lastDot < 0) { // short name
        if (space == null) space = namespace
        if (space != null && !("" == space)) return space + "." + name
      }
      name
    }
  }

  class BaseTypeBuilder[R](
    val context: Completion[R],
    val names: NameContext
  ) {
    final def `type`(schema: Schema): R = context.complete(schema)

    final def `type`(name: String): R = `type`(name, null)

    final def `type`(
      name: String,
      namespace: String
    ): R = `type`(names.get(name, namespace))

    final def booleanType: R = booleanBuilder.endBoolean
    final def booleanBuilder: BooleanBuilder[R] = new BooleanBuilder(context, names)
    final def intType: R = intBuilder.endInt
    final def intBuilder: IntBuilder[R] = new IntBuilder(context, names)
    final def longType: R = longBuilder.endLong
    final def longBuilder: LongBuilder[R] = new LongBuilder(context, names)
    final def floatType: R = floatBuilder.endFloat
    final def floatBuilder: FloatBuilder[R] = new FloatBuilder(context, names)
    final def doubleType: R = doubleBuilder.endDouble
    final def doubleBuilder: DoubleBuilder[R] = new DoubleBuilder(context, names)
    final def stringType: R = stringBuilder.endString

    final def stringBuilder: StringBldr[R] = new StringBldr(context, names)

    final def bytesType: R = bytesBuilder.endBytes

    final def bytesBuilder: BytesBuilder[R] = new BytesBuilder(context, names)

    final def nullType: R = nullBuilder.endNull

    /**
      * Build a null type that can set custom properties. If custom properties
      * are not needed it is simpler to use {@link #nullType()}.
      */
    final def nullBuilder: NullBuilder[R] = new NullBuilder(context, names)

    /** Build an Avro map type  Example usage:
      * <pre>
      * map().values().intType()
      * </pre>
      * Equivalent to Avro JSON Schema:
      * <pre>
      * {"type":"map", "values":"int"}
      * </pre>
      * */
    final def map: MapBuilder[R] = new MapBuilder(context, names)

    /** Build an Avro array type  Example usage:
      * <pre>
      * array().items().longType()
      * </pre>
      * Equivalent to Avro JSON Schema:
      * <pre>
      * {"type":"array", "values":"long"}
      * </pre>
      * */
    final def array: ArrayBuilder[R] = new ArrayBuilder(context, names)
    final def fixed(name: String): FixedBuilder[R] = new FixedBuilder(context, names, name)
    final def enumeration(name: String): EnumBuilder[R] = new EnumBuilder(context, names, name)
    final def record(name: String): RecordBuilder[R] = RecordBuilder.create(context, names, name)
    protected def unionOf: BaseTypeBuilder[UnionAccumulator[R]] = UnionBuilder.create(context, names)
    protected def nullable = new BaseTypeBuilder[R](new NullableCompletion[R](context), names)
  }

  final class TypeBuilder[R](
    override val context: Completion[R],
    override val names: NameContext
  ) extends BaseTypeBuilder[R](context, names) {
    override def unionOf: BaseTypeBuilder[UnionAccumulator[R]] = super.unionOf

    override def nullable: BaseTypeBuilder[R] = super.nullable
  }

  /** A special builder for unions.  Unions cannot nest unions directly **/
  private object UnionBuilder {
    def create[R](
      context: Completion[R],
      names: NameContext
    ) = new UnionBuilder[R](context, names)
  }

  final private class UnionBuilder[R] private(
    override val context: Completion[R],
    override val names: NameContext,
    val schemas: List[Schema]
  ) extends BaseTypeBuilder[UnionAccumulator[R]](new UnionCompletion[R](context, names, schemas), names) {
    def this(
      context: Completion[R],
      names: NameContext
    ) {
      this(context, names, new java.util.ArrayList[Schema])
    }
  }

  class BaseFieldTypeBuilder[R](
    val bldr: FieldBuilder[R],
    val wrapper: CompletionWrapper
  ) {
    this.names = bldr.names
    final protected var names:NameContext = _
    final def booleanType: BooleanDefault[R] = booleanBuilder.endBoolean
    final def booleanBuilder: BooleanBuilder[BooleanDefault[R]] = new BooleanBuilder(wrap(new BooleanDefault[R](bldr)), names)
    final def intType: IntDefault[R] = intBuilder.endInt
    final def intBuilder: IntBuilder[IntDefault[R]] = new IntBuilder(wrap(new IntDefault[R](bldr)), names)
    final def longType: LongDefault[R] = longBuilder.endLong
    final def longBuilder: LongBuilder[LongDefault[R]] =
      new LongBuilder(wrap(new LongDefault[R](bldr)), names)
    final def floatType: FloatDefault[R] = floatBuilder.endFloat
    final def floatBuilder: FloatBuilder[FloatDefault[R]] =
      new FloatBuilder(wrap(new FloatDefault[R](bldr)), names)
    final def doubleType: DoubleDefault[R] = doubleBuilder.endDouble
    final def doubleBuilder: DoubleBuilder[DoubleDefault[R]] = new DoubleBuilder(wrap(new DoubleDefault[R](bldr)), names)
    final def stringType: StringDefault[R] = stringBuilder.endString
    final def stringBuilder: StringBldr[StringDefault[R]] = new StringBldr(wrap(new StringDefault[R](bldr)), names)
    final def bytesType: BytesDefault[R] = bytesBuilder.endBytes
    final def bytesBuilder: BytesBuilder[BytesDefault[R]] = new BytesBuilder(wrap(new BytesDefault[R](bldr)), names)
    final def nullType: NullDefault[R] = nullBuilder.endNull
    final def nullBuilder: NullBuilder[NullDefault[R]] = new NullBuilder(wrap(new NullDefault[R](bldr)), names)

    final def map: MapBuilder[MapDefault[R]] = new MapBuilder(wrap(new MapDefault[R](bldr)), names)

    final def array: ArrayBuilder[ArrayDefault[R]] = new ArrayBuilder(wrap(new ArrayDefault[R](bldr)), names)

    final def fixed(name: String): FixedBuilder[FixedDefault[R]] = new FixedBuilder(wrap(new FixedDefault[R](bldr)), names, name)

    final def enumeration(name: String): EnumBuilder[EnumDefault[R]] = new EnumBuilder(wrap(new EnumDefault[R](bldr)), names, name)

    final def record(name: String): RecordBuilder[RecordDefault[R]] = RecordBuilder.create(wrap(new RecordDefault[R](bldr)), names, name)

    private def wrap[C](completion: Completion[C]): Completion[C] = {
      if (wrapper != null) return wrapper.wrap(completion)
      completion
    }
  }

  /** FieldTypeBuilder adds {@link #unionOf()}, {@link #nullable()}, and {@link #optional()}
    * to BaseFieldTypeBuilder. **/
  final class FieldTypeBuilder[R](
    override val bldr: FieldBuilder[R]
  ) extends BaseFieldTypeBuilder[R](bldr, null) {
    def unionOf = new UnionFieldTypeBuilder[R](bldr)

    def nullable = new BaseFieldTypeBuilder[R](bldr, new NullableCompletionWrapper)
    def optional = new BaseTypeBuilder[FieldAssembler[R]](new OptionalCompletion[R](bldr), names)
  }

  final class UnionFieldTypeBuilder[R] private(val bldr: FieldBuilder[R]) {
    final private var names: NameContext = _
    this.names = bldr.names

    def booleanType: UnionAccumulator[BooleanDefault[R]] = booleanBuilder.endBoolean
    def booleanBuilder: BooleanBuilder[UnionAccumulator[BooleanDefault[R]]] = new BooleanBuilder(completion(new BooleanDefault[R](bldr)), names)
    def intType: UnionAccumulator[IntDefault[R]] = intBuilder.endInt
    def intBuilder: IntBuilder[UnionAccumulator[IntDefault[R]]] = new IntBuilder(completion(new IntDefault[R](bldr)), names)
    def longType: UnionAccumulator[LongDefault[R]] = longBuilder.endLong
    def longBuilder: LongBuilder[UnionAccumulator[LongDefault[R]]] =
      new LongBuilder(completion(new LongDefault[R](bldr)), names)
    def floatType: UnionAccumulator[FloatDefault[R]] =
      floatBuilder.endFloat
    def floatBuilder: FloatBuilder[UnionAccumulator[FloatDefault[R]]] =
      new FloatBuilder(completion(new FloatDefault[R](bldr)), names)
    def doubleType: UnionAccumulator[DoubleDefault[R]] =
      doubleBuilder.endDouble
    def doubleBuilder: DoubleBuilder[UnionAccumulator[DoubleDefault[R]]] =
      new DoubleBuilder(completion(new DoubleDefault[R](bldr)), names)
    def stringType: UnionAccumulator[StringDefault[R]] =
      stringBuilder.endString
    def stringBuilder: StringBldr[UnionAccumulator[StringDefault[R]]] =
      new StringBldr(completion(new StringDefault[R](bldr)), names)
    def bytesType: UnionAccumulator[BytesDefault[R]] =
      bytesBuilder.endBytes
    def bytesBuilder: BytesBuilder[UnionAccumulator[BytesDefault[R]]] =
      new BytesBuilder(completion(new BytesDefault[R](bldr)), names)
    def nullType: UnionAccumulator[NullDefault[R]] =
      nullBuilder.endNull
    def nullBuilder: NullBuilder[UnionAccumulator[NullDefault[R]]] =
      new NullBuilder(completion(new NullDefault[R](bldr)), names)
    def map: MapBuilder[UnionAccumulator[MapDefault[R]]] =
      new MapBuilder(completion(new MapDefault[R](bldr)), names)
    def array: ArrayBuilder[UnionAccumulator[ArrayDefault[R]]] =
      new ArrayBuilder(completion(new ArrayDefault[R](bldr)), names)
    def fixed(name: String): FixedBuilder[UnionAccumulator[FixedDefault[R]]] =
      new FixedBuilder(completion(new FixedDefault[R](bldr)), names, name)

    def enumeration(name: String): EnumBuilder[UnionAccumulator[EnumDefault[R]]] = new EnumBuilder(completion(new EnumDefault[R](bldr)), names, name)

    def record(name: String): RecordBuilder[UnionAccumulator[RecordDefault[R]]] = RecordBuilder.create(completion(new RecordDefault[R](bldr)), names, name)

    private def completion[C](context: Completion[C]) = new UnionCompletion[C](context, names, new java.util.ArrayList[Schema])
  }

  object RecordBuilder {
    def create[R](
      context: Completion[R],
      names: NameContext,
      name: String
    ) = new RecordBuilder[R](context, names, name)
  }

  final class RecordBuilder[R](
    override val context: Completion[R],
    override val names: NameContext,
    override val name: String
  ) extends NamespacedBuilder[R, RecordBuilder[R]](context, names, name) {

    def fields: FieldAssembler[R] = {
      val record = Schema.createRecord(name, doc, space, false)
      // place the record in the name context, fields yet to be set.
      completeSchema(record)
      new FieldAssembler[R](context, names.namespace(record.getNamespace), record)
    }
  }

  final class FieldAssembler[R](
    val context: Completion[R],
    val names: NameContext,
    val record: Schema
  ) {
    final private val fields = new java.util.ArrayList[Schema.Field]

    def name(fieldName: String) = new FieldBuilder[R](this, names, fieldName)
    def requiredBoolean(fieldName: String): FieldAssembler[R] = name(fieldName).`type`.booleanType.noDefault
    def optionalBoolean(fieldName: String): FieldAssembler[R] = name(fieldName).`type`.optional.booleanType
    def nullableBoolean(
      fieldName: String,
      defaultVal: Boolean
    ): FieldAssembler[R] = name(fieldName).`type`.nullable.booleanType.booleanDefault(defaultVal)
    def requiredInt(fieldName: String): FieldAssembler[R] = name(fieldName).`type`.intType.noDefault
    def optionalInt(fieldName: String): FieldAssembler[R] = name(fieldName).`type`.optional.intType
    def nullableInt(
      fieldName: String,
      defaultVal: Int
    ): FieldAssembler[R] = name(fieldName).`type`.nullable.intType.intDefault(defaultVal)
    def requiredLong(fieldName: String): FieldAssembler[R] = name(fieldName).`type`.longType.noDefault
    def optionalLong(fieldName: String): FieldAssembler[R] = name(fieldName).`type`.optional.longType
    def nullableLong(
      fieldName: String,
      defaultVal: Long
    ): FieldAssembler[R] = name(fieldName).`type`.nullable.longType.longDefault(defaultVal)
    def requiredFloat(fieldName: String): FieldAssembler[R] = name(fieldName).`type`.floatType.noDefault
    def optionalFloat(fieldName: String): FieldAssembler[R] = name(fieldName).`type`.optional.floatType
    def nullableFloat(
      fieldName: String,
      defaultVal: Float
    ): FieldAssembler[R] = name(fieldName).`type`.nullable.floatType.floatDefault(defaultVal)
    def requiredDouble(fieldName: String): FieldAssembler[R] = name(fieldName).`type`.doubleType.noDefault
    def optionalDouble(fieldName: String): FieldAssembler[R] = name(fieldName).`type`.optional.doubleType
    def nullableDouble(
      fieldName: String,
      defaultVal: Double
    ): FieldAssembler[R] = name(fieldName).`type`.nullable.doubleType.doubleDefault(defaultVal)
    def requiredString(fieldName: String): FieldAssembler[R] = name(fieldName).`type`.stringType.noDefault
    def optionalString(fieldName: String): FieldAssembler[R] = name(fieldName).`type`.optional.stringType
    def nullableString(
      fieldName: String,
      defaultVal: String
    ): FieldAssembler[R] = name(fieldName).`type`.nullable.stringType.stringDefault(defaultVal)
    def requiredBytes(fieldName: String): FieldAssembler[R] = name(fieldName).`type`.bytesType.noDefault
    def optionalBytes(fieldName: String): FieldAssembler[R] = name(fieldName).`type`.optional.bytesType
    def nullableBytes(
      fieldName: String,
      defaultVal: Array[Byte]
    ): FieldAssembler[R] = name(fieldName).`type`.nullable.bytesType.bytesDefault(defaultVal)
    def endRecord: R = {
      record.setFields(fields)
      context.complete(record)
    }

    private def addField(field: Schema.Field) = {
      fields.add(field)
      this
    }
  }

  final class FieldBuilder[R](
    val fields: FieldAssembler[R],
    override val names: NameContext,
    override val name: String
  ) extends NamedBuilder[FieldBuilder[R]](names, name) {
    private var order:Order = ASCENDING

    def orderAscending: FieldBuilder[R] = {
      order = ASCENDING
      self
    }

    def orderDescending: FieldBuilder[R] = {
      order = DESCENDING
      self
    }

    def orderIgnore: FieldBuilder[R] = {
      order = IGNORE
      self
    }

    def `type` = new FieldTypeBuilder[R](this)
    def `type`(`type`: Schema) = new GenericDefault[R](this, `type`)
    def `type`(name: String): GenericDefault[R] = `type`(name, null)
    def `type`(
      name: String,
      namespace: String
    ): GenericDefault[R] = {
      val schema = names.get(name, namespace)
      `type`(schema)
    }

    def completeField(
      schema: Schema,
      defaultVal: Any
    ):FieldBuilder[R] = {
      val defaultNode = toJsonNode(defaultVal)
      completeField(schema, defaultNode)
    }

    def completeField(schema: Schema) = completeField(schema, null)

    private def completeField(
      schema: Schema,
      defaultVal: JsonNode
    ) = {
      val field = new Schema.Field(name, schema, doc, defaultVal, order)
      addPropsTo(field)
      addAliasesTo(field)
      fields.addField(field)
    }
  }

  abstract class FieldDefault[R, S <: FieldDefault[R, S]](val field: FieldBuilder[R]) extends Completion[S] { self =>
    private var schema: Schema = _

    final def noDefault: FieldAssembler[R] = field.completeField(schema)

    def usingDefault(defaultVal: Any):FieldBuilder[R] = field.completeField(schema, defaultVal)

    override final private[avro] def complete(schema: Schema) = {
      this.schema = schema
      self
    }
  }

  /** Choose whether to use a default value for the field or not. **/
  class BooleanDefault[R] private(override val field: FieldBuilder[R]) extends FieldDefault[R, BooleanDefault[R]](field) {
    /** Completes this field with the default value provided **/
    final def booleanDefault(defaultVal: Boolean): FieldAssembler[R] = super.usingDefault(defaultVal)
  }

  class IntDefault[R] private(override val field: FieldBuilder[R]) extends FieldDefault[R, IntDefault[R]](field) {
    final def intDefault(defaultVal: Int): FieldAssembler[R] = super.usingDefault(defaultVal)
  }

  class LongDefault[R] private(override val field: FieldBuilder[R]) extends FieldDefault[R, LongDefault[R]](field) {
    final def longDefault(defaultVal: Long): FieldAssembler[R] = super.usingDefault(defaultVal)
  }

  class FloatDefault[R] private(override val field: FieldBuilder[R]) extends FieldDefault[R, FloatDefault[R]](field) {
    final def floatDefault(defaultVal: Float): FieldAssembler[R] = super.usingDefault(defaultVal)
  }

  class DoubleDefault[R] private(override val field: FieldBuilder[R]) extends FieldDefault[R, DoubleDefault[R]](field) {
    final def doubleDefault(defaultVal: Double): FieldAssembler[R] = super.usingDefault(defaultVal)

  }

  class StringDefault[R] private(override val field: FieldBuilder[R]) extends FieldDefault[R, StringDefault[R]](field) {
    final def stringDefault(defaultVal: String): FieldAssembler[R] = super.usingDefault(defaultVal)
  }

  class BytesDefault[R] private(override val field: FieldBuilder[R]) extends FieldDefault[R, BytesDefault[R]](field) {
    final def bytesDefault(defaultVal: Array[Byte]): FieldAssembler[R] = super.usingDefault(ByteBuffer.wrap(defaultVal))

    final def bytesDefault(defaultVal: ByteBuffer): FieldAssembler[R] = super.usingDefault(defaultVal)

    final def bytesDefault(defaultVal: String): FieldAssembler[R] = super.usingDefault(defaultVal)

  }

  class NullDefault[R] private(override val field: FieldBuilder[R]) extends FieldDefault[R, NullDefault[R]](field) {
    final def nullDefault: FieldAssembler[R] = super.usingDefault(null)

  }

  class MapDefault[R] private(override val field: FieldBuilder[R]) extends FieldDefault[R, MapDefault[R]](field) {
    final def mapDefault[K, V](defaultVal: java.util.Map[K, V]): FieldAssembler[R] = super.usingDefault(defaultVal)

  }

  class ArrayDefault[R] private(override val field: FieldBuilder[R]) extends FieldDefault[R, ArrayDefault[R]](field) {
    final def arrayDefault[V](defaultVal: java.util.List[V]): FieldAssembler[R] = super.usingDefault(defaultVal)
  }

  class FixedDefault[R] private(override val field: FieldBuilder[R]) extends FieldDefault[R, FixedDefault[R]](field) {
    final def fixedDefault(defaultVal: Array[Byte]): FieldAssembler[R] = super.usingDefault(ByteBuffer.wrap(defaultVal))

    final def fixedDefault(defaultVal: ByteBuffer): FieldAssembler[R] = super.usingDefault(defaultVal)

    final def fixedDefault(defaultVal: String): FieldAssembler[R] = super.usingDefault(defaultVal)
  }

  class EnumDefault[R](override val field: FieldBuilder[R]) extends FieldDefault[R, EnumDefault[R]](field) {
    final def enumDefault(defaultVal: String): FieldAssembler[R] = super.usingDefault(defaultVal)
  }

  class RecordDefault[R](
    override val field: FieldBuilder[R]
  ) extends FieldDefault[R, RecordDefault[R]](field) {
    final def recordDefault(defaultVal: GenericRecord): FieldAssembler[R] = super.usingDefault(defaultVal)

  }

  final class GenericDefault[R] private(
    val field: FieldBuilder[R],
    val schema: Schema
  ) {
    def noDefault: FieldAssembler[R] = field.completeField(schema)

    def withDefault(defaultVal: Any): FieldAssembler[R] = field.completeField(schema, defaultVal)
  }

  trait Completion[R] {
    def complete(schema: Schema):R
  }

  private class SchemaCompletion extends Completion[Schema] {
    def complete(schema: Schema): Schema = schema
  }

  private val NULL_SCHEMA = Schema.create(NULL)

  private class NullableCompletion[R] private(val context: Completion[R]) extends Completion[R] {
    def complete(schema: Schema): R = { // wrap the schema as a union of the schema and null
      val nullable = Schema.createUnion(schema, NULL_SCHEMA)
      context.complete(nullable)
    }
  }

  private class OptionalCompletion[R](val bldr: FieldBuilder[R]) extends Completion[FieldAssembler[R]] {
    def complete(schema: Schema): FieldAssembler[R] = { // wrap the schema as a union of null and the schema
      val optional = Schema.createUnion(NULL_SCHEMA, schema)
      bldr.completeField(optional, null.asInstanceOf[Any])
    }
  }

  abstract private class CompletionWrapper {
    def wrap[R](completion: Completion[R])
  }

  final private class NullableCompletionWrapper extends CompletionWrapper {
    override private[avro] def wrap[R](completion: Completion[R]) = new NullableCompletion[R](completion)
  }

  abstract private class NestedCompletion[R](
    val assembler: PropBuilder[_ <: PropBuilder[_]],
    val context: Completion[R]
  ) extends Completion[R] {
    override final protected def complete(schema: Schema): R = {
      val outer = outerSchema(schema)
      assembler.addPropsTo(outer)
      context.complete(outer)
    }

    protected def outerSchema(inner: Schema): Schema
  }

  private class MapCompletion[R](
    override val assembler: MapBuilder[R],
    override val context: Completion[R]
  ) extends NestedCompletion[R](assembler, context) {
    override protected def outerSchema(inner: Schema): Schema = Schema.createMap(inner)
  }

  private class ArrayCompletion[R](
    override val assembler: ArrayBuilder[R],
    override val context: Completion[R]
  ) extends NestedCompletion[R](assembler, context) {
    override protected def outerSchema(inner: Schema): Schema = Schema.createArray(inner)
  }

  private class UnionCompletion[R] private(
    val context: Completion[R],
    val names: NameContext,
    val schemas: List[Schema]
  ) extends Completion[UnionAccumulator[R]] {
    override protected def complete(schema: Schema): UnionAccumulator[R] = {
      val updated = new java.util.ArrayList[Schema](this.schemas)
      updated.add(schema)
      new UnionAccumulator[R](context, names, updated)
    }
  }

  final class UnionAccumulator[R] private(
    val context: Completion[R],
    val names: NameContext,
    val schemas: List[Schema]
  ) {
    /** Add an additional type to this union **/
    def and = new UnionBuilder[R](context, names, schemas)

    /** Complete this union **/
    def endUnion: R = {
      val schema = Schema.createUnion(schemas)
      context.complete(schema)
    }
  }

  private def checkRequired(
    reference: Any,
    errorMessage: String
  ) = if (reference == null) throw new NullPointerException(errorMessage)

  // create default value JsonNodes from objects
  private def toJsonNode(o: Any) = try {
    var s = null
    if (o.isInstanceOf[ByteBuffer]) { // special case since GenericData.toString() is incorrect for bytes
      // note that this does not handle the case of a default value with nested bytes
      val bytes = o.asInstanceOf[ByteBuffer]
      bytes.mark
      val data = new Array[Byte](bytes.remaining)
      bytes.get(data)
      bytes.reset // put the buffer back the way we got it
      s = new String(data, "ISO-8859-1")
      val quoted = JsonStringEncoder.getInstance.quoteAsString(s)
      s = "\"" + new String(quoted) + "\""
    }
    else if (o.isInstanceOf[Array[Byte]]) {
      s = new String(o.asInstanceOf[Array[Byte]], "ISO-8859-1")
      val quoted = JsonStringEncoder.getInstance.quoteAsString(s)
      s = '\"' + new String(quoted) + '\"'
    }
    else s = GenericData.get.toString(o)
    new ObjectMapper().readTree(s)
  } catch {
    case e: IOException =>
      throw new xception(e)
  }
}
