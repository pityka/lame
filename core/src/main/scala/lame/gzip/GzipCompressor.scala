/*
 * Copyright (C) 2009-2019 Lightbend Inc. <https://www.lightbend.com>
 *
 * Changes to the Akka file: eliminate the DeflateCompressor abstract class
 */

package lame.gzip

import java.util.zip.{CRC32, Deflater}

import akka.util.ByteString

class GzipCompressor(
    compressionLevel: Int,
    customDeflater: Option[() => Deflater]
) extends Compressor {

  import DeflateCompressor._

  private lazy val deflater = customDeflater match {
    case Some(factory) => factory()
    case _             => new Deflater(compressionLevel, true)
  }

  def needsInput = deflater.needsInput

  private val checkSum = new CRC32 // CRC32 of uncompressed data
  private var headerSent = false
  private var bytesRead = 0L

  def compressAndFlush(input: ByteString): ByteString = {
    val buffer = newTempBuffer(input.size)

    compressWithBuffer(input, buffer) ++ flushWithBuffer(buffer)
  }

  def compressAndFinish(input: ByteString): ByteString = {
    val buffer = newTempBuffer(input.size)

    compressWithBuffer(input, buffer) ++ finishWithBuffer(buffer)
  }

  def compress(input: ByteString): ByteString =
    compressWithBuffer(input, newTempBuffer())

  def flush(): ByteString = flushWithBuffer(newTempBuffer())
  def finish(): ByteString = finishWithBuffer(newTempBuffer())

  def close(): Unit = deflater.end()

  private def compressWithBuffer(
      input: ByteString,
      buffer: Array[Byte]
  ): ByteString = {

    updateCrc(input)

    header() ++ {
      require(deflater.needsInput())
      deflater.setInput(input.toArray)
      drainDeflater(deflater, buffer)
    }
  }

  private def flushWithBuffer(buffer: Array[Byte]): ByteString =
    header() ++ {
      val written =
        deflater.deflate(buffer, 0, buffer.length, Deflater.SYNC_FLUSH)
      ByteString.fromArray(buffer, 0, written)
    }

  private def finishWithBuffer(buffer: Array[Byte]): ByteString =
    header() ++ {
      deflater.finish()
      val res = drainDeflater(deflater, buffer)
      deflater.end()
      res
    } ++ trailer()

  private def updateCrc(input: ByteString): Unit = {
    checkSum.update(input.toArray)
    bytesRead += input.length
  }

  private def header(): ByteString =
    if (!headerSent) {
      headerSent = true
      GzipCompressor.Header
    } else ByteString.empty

  private def trailer(): ByteString = {
    def int32(i: Int): ByteString = ByteString(i, i >> 8, i >> 16, i >> 24)
    val crc = checkSum.getValue.toInt
    val tot =
      bytesRead.toInt // truncated to 32bit as specified in https://tools.ietf.org/html/rfc1952#section-2
    val trailer = int32(crc) ++ int32(tot)

    trailer
  }

  private def newTempBuffer(size: Int = 65536): Array[Byte] = {
    // The default size is somewhat arbitrary, we'd like to guess a better value but Deflater/zlib
    // is buffering in an unpredictable manner.
    // `compress` will only return any data if the buffered compressed data has some size in
    // the region of 10000-50000 bytes.
    // `flush` and `finish` will return any size depending on the previous input.
    // This value will hopefully provide a good compromise between memory churn and
    // excessive fragmentation of ByteStrings.
    // We also make sure that buffer size stays within a reasonable range, to avoid
    // draining deflator with too small buffer.
    new Array[Byte](math.max(size, MinBufferSize))
  }
}

object GzipCompressor {
  // RFC 1952: http://tools.ietf.org/html/rfc1952 section 2.2
  val Header = ByteString(0x1f, // ID1
    0x8b, // ID2
    8, // CM = Deflate
    0, // FLG
    0, // MTIME 1
    0, // MTIME 2
    0, // MTIME 3
    0, // MTIME 4
    0, // XFL
    0 // OS
  )
}
