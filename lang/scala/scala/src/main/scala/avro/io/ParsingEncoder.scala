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
import java.util
import org.apache.avro.AvroTypeException

/** Base class for <a href="parsing/package-summary.html">parser</a>-based
  * {@link Encoder}s. */
abstract class ParsingEncoder extends Encoder {
  /**
    * Tracks the number of items that remain to be written in
    * the collections (array or map).
    */
    private var counts = new Array[Long](10)
  protected var pos: Int = -1

  @throws[IOException]
  override def setItemCount(itemCount: Long): Unit = {
    if (counts(pos) != 0) throw new AvroTypeException("Incorrect number of items written. " + counts(pos) + " more required.")
    counts(pos) = itemCount
  }

  @throws[IOException]
  override def startItem(): Unit = counts(pos) -= 1

  /** Push a new collection on to the stack. */
  final protected def push(): Unit = {
    if ( {
      pos += 1; pos
    } == counts.length) counts = util.Arrays.copyOf(counts, pos + 10)
    counts(pos) = 0
  }

  final protected def pop(): Unit = {
    if (counts(pos) != 0) throw new AvroTypeException("Incorrect number of items written. " + counts(pos) + " more required.")
    pos -= 1
  }

  final protected def depth: Int = pos
}
