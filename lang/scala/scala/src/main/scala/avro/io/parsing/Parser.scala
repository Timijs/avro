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
package avro.io.parsing

import java.io.IOException
import java.util

import avro.AvroTypeException

/**
  * Parser is the class that maintains the stack for parsing. This class
  * is used by encoders, which are not required to skip.
  */
object Parser {

  /**
    * The parser knows how to handle the terminal and non-terminal
    * symbols. But it needs help from outside to handle implicit
    * and explicit actions. The clients implement this interface to
    * provide this help.
    */
  trait ActionHandler {
    /**
      * Handle the action symbol <tt>top</tt> when the <tt>input</tt> is
      * sought to be taken off the stack.
      *
      * @param input The input symbol from the caller of advance
      * @param top   The symbol at the top the stack.
      * @return <tt>null</tt> if advance() is to continue processing the
      * stack. If not <tt>null</tt> the return value will be returned
      *         by advance().
      * @throws IOException
      */
      @throws[IOException]
      def doAction(input: Symbol, top: Symbol): Symbol
  }

}
@throws[IOException]
class Parser(val root: Symbol, val symbolHandler: Parser.ActionHandler) {
  import Symbol.Kind._
  protected var stack:Array[Symbol] = new Array[Symbol](5) // Start small to make sure expansion code works
  this.stack(0) = root
  protected var pos:Int = 1

  private def expandStack() =
    stack = util.Arrays.copyOf(
      stack,
      stack.length + Math.max(stack.length, 1024))

  /**
    * Recursively replaces the symbol at the top of the stack with its
    * production, until the top is a terminal. Then checks if the
    * top symbol matches the terminal symbol suppled <tt>terminal</tt>.
    *
    * @param input The symbol to match against the terminal at the
    *              top of the stack.
    * @return The terminal symbol at the top of the stack unless an
    *         implicit action resulted in another symbol, in which case that
    *         symbol is returned.
    */
  @throws[IOException]
  def advance(input: Symbol)(top:Symbol):Symbol = {
      val top:Symbol = stack(pos=pos-1)
      if (top == input)
        return top // A common case

      top.kind match {
        case IMPLICIT_ACTION => symbolHandler.doAction(input, top)
        case TERMINAL => throw new AvroTypeException(s"Attempt to process a $input when a $top was expected.")
        case t @ Symbol.Repeater if t.end == input => input
        case _ => pushProduction(top); advance(input)(top)
      }
  }

  /**
    * Performs any implicit actions at the top the stack, expanding any
    * production (other than the root) that may be encountered.
    * This method will fail if there are any repeaters on the stack.
    *
    * @throws IOException
    */
  @throws[IOException]
  final def processImplicitActions(): Unit = {
    var noBreak = true
    while ( {
      noBreak && pos > 1
    }) {
      val top = stack(pos - 1)
      if (top.kind == IMPLICIT_ACTION) {
        pos -= 1
        symbolHandler.doAction(null, top)
      }
      else if (top.kind != TERMINAL) {
        pos -= 1
        pushProduction(top)
      }
      else noBreak = false
    }
  }

  /**
    * Performs any "trailing" implicit actions at the top the stack.
    */
  @throws[IOException]
  final def processTrailingImplicitActions(): Unit = {

    var noBreak = true
    while (pos >= 1 && noBreak) {
      val top = stack(pos - 1)
      if ((top.kind != IMPLICIT_ACTION) && top.asInstanceOf[Symbol.ImplicitAction].isTrailing) {
        pos -= 1
        symbolHandler.doAction(null, top)
      }
      else noBreak = false
    }
  }

  /**
    * Pushes the production for the given symbol <tt>sym</tt>.
    * If <tt>sym</tt> is a repeater and <tt>input</tt> is either
    * {@link Symbol#ARRAY_END} or {@link Symbol#MAP_END} pushes nothing.
    *
    * @param sym
    */
  final def pushProduction(sym: Symbol): Unit = {
    val p = sym.production
    while (pos + p.length > stack.length) expandStack()
    System.arraycopy(p, 0, stack, pos, p.length)
    pos += p.length
  }

  def popSymbol: Symbol = stack({
    pos -= 1; pos
  })

  def topSymbol: Symbol = stack(pos - 1)

  def pushSymbol(sym: Symbol): Unit = {
    if (pos == stack.length) expandStack()
    stack({
      pos += 1; pos - 1
    }) = sym
  }

  /**
    * Returns the depth of the stack.
    */
  def depth: Int = pos

  def reset(): Unit = pos = 1
}
