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
package avro.specific

import java.io.Externalizable
import java.io.ObjectOutput
import java.io.ObjectInput
import java.io.IOException

import avro.{AvroRemoteException, Schema}

/** Base class for specific exceptions. */
abstract class SpecificExceptionBase extends AvroRemoteException with SpecificRecord with Externalizable {
  def this(value: Throwable) {
    this()
    new AvroRemoteException(value)
  }

  def this(value: Any) {
    this()
    new AvroRemoteException(value)
  }

  def this(value: Any, cause: Throwable) {
    this()
    new AvroRemoteException(value, cause)
  }

  def getSchema: Schema

  def get(field: Int): Any

  def put(field: Int, value: Any): Unit

  override def equals(that: Any): Boolean = {
    if (that == this) return true // identical object
    if (!that.isInstanceOf[SpecificExceptionBase]) return false // not a record
    if (this.getClass ne that.getClass) return false // not same schema
    SpecificData.get.compare(this, that, this.getSchema) == 0
  }

  override def hashCode: Int = SpecificData.get.hashCode(this, this.getSchema)

  @throws[IOException]
  override def writeExternal(out: ObjectOutput): Unit

  @throws[IOException]
  override def readExternal(in: ObjectInput): Unit
}
