package lame
import java.util.zip.Deflater

object Gzip {
  import lame.gzip._
  def apply(level: Int = 1, customDeflater: Option[() => Deflater] = None) =
    CompressionUtils.compressorFlow(() =>
      new GzipCompressor(level, customDeflater)
    )
}
