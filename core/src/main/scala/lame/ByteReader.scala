/*
 * Copyright (C) 2009-2019 Lightbend Inc. <https://www.lightbend.com>
 */

package lame
import akka.util.ByteString
import scala.util.control.NoStackTrace

object ByteReader {
  val NeedMoreData = new Exception with NoStackTrace
}

class ByteReader(input: ByteString) {
  import ByteReader.NeedMoreData

  private[this] var off = 0

  def hasRemaining: Boolean = off < input.size
  def remainingSize: Int = input.size - off

  def currentOffset: Int = off

  def remainingData: ByteString = input.drop(off)
  def fromStartToHere: ByteString = input.take(off)

  def take(n: Int): ByteString =
    if (off + n <= input.length) {
      val o = off
      off = o + n
      input.slice(o, off)
    } else throw NeedMoreData
  def takeAll(): ByteString = {
    val ret = remainingData
    off = input.size
    ret
  }

  def readByte(): Int =
    if (off < input.length) {
      val x = input(off)
      off += 1
      x & 0xFF
    } else throw NeedMoreData
  def readShortLE(): Int = readByte() | (readByte() << 8)
  def readIntLE(): Int = readShortLE() | (readShortLE() << 16)
  def readLongLE(): Long =
    (readIntLE() & 0XFFFFFFFFL) | ((readIntLE() & 0XFFFFFFFFL) << 32)

  def readShortBE(): Int = (readByte() << 8) | readByte()
  def readIntBE(): Int = (readShortBE() << 16) | readShortBE()
  def readLongBE(): Long =
    ((readIntBE() & 0XFFFFFFFFL) << 32) | (readIntBE() & 0XFFFFFFFFL)

  def skip(numBytes: Int): Unit =
    if (off + numBytes <= input.length) off += numBytes
    else throw NeedMoreData
  def skipZeroTerminatedString(): Unit = while (readByte() != 0) {}
}
