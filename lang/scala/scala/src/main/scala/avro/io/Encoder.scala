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

import java.io.Flushable
import java.io.IOException
import java.nio.ByteBuffer

import avro.AvroTypeException
import org.apache.avro.util.Utf8

/**
  * Low-level support for serializing Avro values.
  * <p/>
  * This class has two types of methods.  One type of methods support
  * the writing of leaf values (for example, {@link #writeLong} and
  * {@link #writeString}).  These methods have analogs in {@link
  * Decoder}.
  * <p/>
  * The other type of methods support the writing of maps and arrays.
  * These methods are {@link #writeArrayStart}, {@link
  * #startItem}, and {@link #writeArrayEnd} (and similar methods for
  * maps).  Some implementations of {@link Encoder} handle the
  * buffering required to break large maps and arrays into blocks,
  * which is necessary for applications that want to do streaming.
  * (See {@link #writeArrayStart} for details on these methods.)
  * <p/>
  * {@link EncoderFactory} contains Encoder construction and configuration
  * facilities.
  *
  * @see EncoderFactory
  * @see Decoder
  */
abstract class Encoder extends Flushable {
  /**
    * "Writes" a null value.  (Doesn't actually write anything, but
    * advances the state of the parser if this class is stateful.)
    *
    * @throws AvroTypeException If this is a stateful writer and a
    *                           null is not expected
    */
    @throws[IOException]
    def writeNull(): Unit

  /**
    * Write a boolean value.
    *
    * @throws AvroTypeException If this is a stateful writer and a
    *                           boolean is not expected
    */
  @throws[IOException]
  def writeBoolean(b: Boolean): Unit

  /**
    * Writes a 32-bit integer.
    *
    * @throws AvroTypeException If this is a stateful writer and an
    *                           integer is not expected
    */
  @throws[IOException]
  def writeInt(n: Int): Unit

  /**
    * Write a 64-bit integer.
    *
    * @throws AvroTypeException If this is a stateful writer and a
    *                           long is not expected
    */
  @throws[IOException]
  def writeLong(n: Long): Unit

  /** Write a float.
    *
    * @throws IOException
    * @throws AvroTypeException If this is a stateful writer and a
    *                           float is not expected
    */
  @throws[IOException]
  def writeFloat(f: Float): Unit

  /**
    * Write a double.
    *
    * @throws AvroTypeException If this is a stateful writer and a
    *                           double is not expected
    */
  @throws[IOException]
  def writeDouble(d: Double): Unit

  /**
    * Write a Unicode character string.
    *
    * @throws AvroTypeException If this is a stateful writer and a
    *                           char-string is not expected
    */
  @throws[IOException]
  def writeString(utf8: Utf8): Unit

  /**
    * Write a Unicode character string.  The default implementation converts
    * the String to a {@link org.apache.avro.util.Utf8}.  Some Encoder
    * implementations may want to do something different as a performance optimization.
    *
    * @throws AvroTypeException If this is a stateful writer and a
    *                           char-string is not expected
    */
  @throws[IOException]
  def writeString(str: String): Unit = writeString(new Utf8(str))

  /**
    * Write a Unicode character string.  If the CharSequence is an
    * {@link org.apache.avro.util.Utf8} it writes this directly, otherwise
    * the CharSequence is converted to a String via toString() and written.
    *
    * @throws AvroTypeException If this is a stateful writer and a
    *                           char-string is not expected
    */
  @throws[IOException]
  def writeString(charSequence: CharSequence): Unit = if (charSequence.isInstanceOf[Utf8]) writeString(charSequence.asInstanceOf[Utf8])
  else writeString(charSequence.toString)

  /**
    * Write a byte string.
    *
    * @throws AvroTypeException If this is a stateful writer and a
    *                           byte-string is not expected
    */
  @throws[IOException]
  def writeBytes(bytes: ByteBuffer): Unit

  /**
    * Write a byte string.
    *
    * @throws AvroTypeException If this is a stateful writer and a
    *                           byte-string is not expected
    */
  @throws[IOException]
  def writeBytes(
    bytes: Array[Byte],
    start: Int,
    len: Int
  ): Unit

  /**
    * Writes a byte string.
    * Equivalent to <tt>writeBytes(bytes, 0, bytes.length)</tt>
    *
    * @throws IOException
    * @throws AvroTypeException If this is a stateful writer and a
    *                           byte-string is not expected
    */
  @throws[IOException]
  def writeBytes(bytes: Array[Byte]): Unit = writeBytes(bytes, 0, bytes.length)

  /**
    * Writes a fixed size binary object.
    *
    * @param bytes The contents to write
    * @param start The position within <tt>bytes</tt> where the contents
    *              start.
    * @param len   The number of bytes to write.
    * @throws AvroTypeException If this is a stateful writer and a
    *                           byte-string is not expected
    * @throws IOException
    */
  @throws[IOException]
  def writeFixed(
    bytes: Array[Byte],
    start: Int,
    len: Int
  ): Unit

  /**
    * A shorthand for <tt>writeFixed(bytes, 0, bytes.length)</tt>
    *
    * @param bytes
    */
  @throws[IOException]
  def writeFixed(bytes: Array[Byte]): Unit = writeFixed(bytes, 0, bytes.length)

  /** Writes a fixed from a ByteBuffer. */
  @throws[IOException]
  def writeFixed(bytes: ByteBuffer): Unit = {
    val pos = bytes.position
    val len = bytes.limit - pos
    if (bytes.hasArray) writeFixed(bytes.array, bytes.arrayOffset + pos, len)
    else {
      val b = new Array[Byte](len)
      bytes.duplicate.get(b, 0, len)
      writeFixed(b, 0, len)
    }
  }

  /**
    * Writes an enumeration.
    *
    * @param e
    * @throws AvroTypeException If this is a stateful writer and an enumeration
    *                           is not expected or the <tt>e</tt> is out of range.
    * @throws IOException
    */
  @throws[IOException]
  def writeEnum(e: Int): Unit

  /** Call this method to start writing an array.
    *
    * When starting to serialize an array, call {@link
    * #writeArrayStart}. Then, before writing any data for any item
    * call {@link #setItemCount} followed by a sequence of
    * {@link #startItem()} and the item itself. The number of
    * {@link #startItem()} should match the number specified in
    * {@link #setItemCount}.
    * When actually writing the data of the item, you can call any {@link
    * Encoder} method (e.g., {@link #writeLong}).  When all items
    * of the array have been written, call {@link #writeArrayEnd}.
    *
    * As an example, let's say you want to write an array of records,
    * the record consisting of an Long field and a Boolean field.
    * Your code would look something like this:
    * <pre>
    *  out.writeArrayStart();
    *  out.setItemCount(list.size());
    * for (Record r : list) {
    *    out.startItem();
    *    out.writeLong(r.longField);
    *    out.writeBoolean(r.boolField);
    * }
    *  out.writeArrayEnd();
    * </pre>
    *
    * @throws AvroTypeException If this is a stateful writer and an
    *                           array is not expected
    */
  @throws[IOException]
  def writeArrayStart(): Unit

  /**
    * Call this method before writing a batch of items in an array or a map.
    * Then for each item, call {@link #startItem()} followed by any of the
    * other write methods of {@link Encoder}. The number of calls
    * to {@link #startItem()} must be equal to the count specified
    * in {@link #setItemCount}. Once a batch is completed you
    * can start another batch with {@link #setItemCount}.
    *
    * @param itemCount The number of { @link #startItem()} calls to follow.
    * @throws IOException
    */
  @throws[IOException]
  def setItemCount(itemCount: Long): Unit

  /**
    * Start a new item of an array or map.
    * See {@link #writeArrayStart} for usage information.
    *
    * @throws AvroTypeException If called outside of an array or map context
    */
  @throws[IOException]
  def startItem(): Unit

  /**
    * Call this method to finish writing an array.
    * See {@link #writeArrayStart} for usage information.
    *
    * @throws AvroTypeException If items written does not match count
    *                           provided to { @link #writeArrayStart}
    * @throws AvroTypeException If not currently inside an array
    */
  @throws[IOException]
  def writeArrayEnd(): Unit

  /**
    * Call this to start a new map.  See
    * {@link #writeArrayStart} for details on usage.
    *
    * As an example of usage, let's say you want to write a map of
    * records, the record consisting of an Long field and a Boolean
    * field.  Your code would look something like this:
    * <pre>
    * out.writeMapStart();
    * out.setItemCount(list.size());
    * for (Map.Entry<String,Record> entry : map.entrySet()) {
    *   out.startItem();
    *   out.writeString(entry.getKey());
    *   out.writeLong(entry.getValue().longField);
    *   out.writeBoolean(entry.getValue().boolField);
    * }
    * out.writeMapEnd();
    * </pre>
    *
    * @throws AvroTypeException If this is a stateful writer and a
    *                           map is not expected
    */
  @throws[IOException]
  def writeMapStart(): Unit

  /**
    * Call this method to terminate the inner-most, currently-opened
    * map.  See {@link #writeArrayStart} for more details.
    *
    * @throws AvroTypeException If items written does not match count
    *                           provided to { @link #writeMapStart}
    * @throws AvroTypeException If not currently inside a map
    */
  @throws[IOException]
  def writeMapEnd(): Unit

  /** Call this method to write the tag of a union.
    *
    * As an example of usage, let's say you want to write a union,
    * whose second branch is a record consisting of an Long field and
    * a Boolean field.  Your code would look something like this:
    * <pre>
    * out.writeIndex(1);
    * out.writeLong(record.longField);
    * out.writeBoolean(record.boolField);
    * </pre>
    *
    * @throws AvroTypeException If this is a stateful writer and a
    *                           map is not expected
    */
  @throws[IOException]
  def writeIndex(unionIndex: Int): Unit
}
