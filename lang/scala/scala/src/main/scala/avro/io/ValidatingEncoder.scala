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
package avro.io

import java.io.IOException
import java.nio.ByteBuffer
import org.apache.avro.AvroTypeException
import org.apache.avro.Schema
import org.apache.avro.io.parsing.ValidatingGrammarGenerator
import org.apache.avro.io.parsing.Parser
import org.apache.avro.io.parsing.Symbol
import org.apache.avro.util.Utf8

/**
  * An implementation of {@link Encoder} that wraps another Encoder and
  * ensures that the sequence of operations conforms to the provided schema.
  * <p/>
  * Use {@link EncoderFactory#validatingEncoder(Schema, Encoder)} to construct
  * and configure.
  * <p/>
  * ValidatingEncoder is not thread-safe.
  *
  * @see Encoder
  * @see EncoderFactory
  */
@throws[IOException]
class ValidatingEncoder(val root: Symbol, var out: Encoder) extends ParsingEncoder with Parser.ActionHandler {
  this.parser = new Parser (root, this)
  final protected var parser: Parser = null
  def this (schema: Schema, in: Encoder) {
  this (new ValidatingGrammarGenerator ().generate (schema), in)
}
  @throws[IOException]
  override def flush (): Unit = {
  out.flush ()
}

  /**
    * Reconfigures this ValidatingEncoder to wrap the encoder provided.
    *
    * @param encoder
    * The Encoder to wrap for validation.
    * @return
    * This ValidatingEncoder.
    */
  def configure (encoder: Encoder): ValidatingEncoder = {
  this.parser.reset ()
  this.out = encoder
  return this
}
  @throws[IOException]
  override def writeNull (): Unit = {
  parser.advance (Symbol.NULL)
  out.writeNull ()
}
  @throws[IOException]
  override def writeBoolean (b: Boolean): Unit = {
  parser.advance (Symbol.BOOLEAN)
  out.writeBoolean (b)
}
  @throws[IOException]
  override def writeInt (n: Int): Unit = {
  parser.advance (Symbol.INT)
  out.writeInt (n)
}
  @throws[IOException]
  override def writeLong (n: Long): Unit = {
  parser.advance (Symbol.LONG)
  out.writeLong (n)
}
  @throws[IOException]
  override def writeFloat (f: Float): Unit = {
  parser.advance (Symbol.FLOAT)
  out.writeFloat (f)
}
  @throws[IOException]
  override def writeDouble (d: Double): Unit = {
  parser.advance (Symbol.DOUBLE)
  out.writeDouble (d)
}
  @throws[IOException]
  override def writeString (utf8: Utf8): Unit = {
  parser.advance (Symbol.STRING)
  out.writeString (utf8)
}
  @throws[IOException]
  override def writeString (str: String): Unit = {
  parser.advance (Symbol.STRING)
  out.writeString (str)
}
  @throws[IOException]
  override def writeString (charSequence: CharSequence): Unit = {
  parser.advance (Symbol.STRING)
  out.writeString (charSequence)
}
  @throws[IOException]
  override def writeBytes (bytes: ByteBuffer): Unit = {
  parser.advance (Symbol.BYTES)
  out.writeBytes (bytes)
}
  @throws[IOException]
  override def writeBytes (bytes: Array[Byte], start: Int, len: Int): Unit = {
  parser.advance (Symbol.BYTES)
  out.writeBytes (bytes, start, len)
}
  @throws[IOException]
  override def writeFixed (bytes: Array[Byte], start: Int, len: Int): Unit = {
  parser.advance (Symbol.FIXED)
  val top: Symbol.IntCheckAction = parser.popSymbol.asInstanceOf[Symbol.IntCheckAction]
  if (len != top.size) {
  throw new AvroTypeException ("Incorrect length for fixed binary: expected " + top.size + " but received " + len + " bytes.")
}
  out.writeFixed (bytes, start, len)
}
  @throws[IOException]
  override def writeEnum (e: Int): Unit = {
  parser.advance (Symbol.ENUM)
  val top: Symbol.IntCheckAction = parser.popSymbol.asInstanceOf[Symbol.IntCheckAction]
  if (e < 0 || e >= top.size) {
  throw new AvroTypeException ("Enumeration out of range: max is " + top.size + " but received " + e)
}
  out.writeEnum (e)
}
  @throws[IOException]
  override def writeArrayStart (): Unit = {
  push ()
  parser.advance (Symbol.ARRAY_START)
  out.writeArrayStart ()
}
  @throws[IOException]
  override def writeArrayEnd (): Unit = {
  parser.advance (Symbol.ARRAY_END)
  out.writeArrayEnd ()
  pop ()
}
  @throws[IOException]
  override def writeMapStart (): Unit = {
  push ()
  parser.advance (Symbol.MAP_START)
  out.writeMapStart ()
}
  @throws[IOException]
  override def writeMapEnd (): Unit = {
  parser.advance (Symbol.MAP_END)
  out.writeMapEnd ()
  pop ()
}
  @throws[IOException]
  override def setItemCount (itemCount: Long): Unit = {
  super.setItemCount (itemCount)
  out.setItemCount (itemCount)
}
  @throws[IOException]
  override def startItem (): Unit = {
  super.startItem ()
  out.startItem ()
}
  @throws[IOException]
  override def writeIndex (unionIndex: Int): Unit = {
  parser.advance (Symbol.UNION)
  val top: Symbol.Alternative = parser.popSymbol.asInstanceOf[Symbol.Alternative]
  parser.pushSymbol (top.getSymbol (unionIndex) )
  out.writeIndex (unionIndex)
}
  @throws[IOException]
  override def doAction (input: Symbol, top: Symbol): Symbol = {
  return null
}
}
