package avro

import java.util.Locale

object SchemaType {
  sealed trait Type {
    def getName = this.toString.toLowerCase(Locale.ENGLISH)
    def name = this.toString.toLowerCase(Locale.ENGLISH)
  }

  case object RECORD extends Type
  case object ENUM extends Type
  case object ARRAY extends Type
  case object MAP extends Type
  case object UNION extends Type
  case object FIXED extends Type
  case object STRING extends Type
  case object BYTES extends Type
  case object INT extends Type
  case object LONG extends Type
  case object FLOAT extends Type
  case object DOUBLE extends Type
  case object BOOLEAN extends Type
  case object NULL extends Type
}
