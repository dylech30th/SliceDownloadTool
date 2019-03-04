import org.apache.http.client.methods.CloseableHttpResponse
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import java.io.File
import java.io.RandomAccessFile
import java.net.HttpURLConnection
import java.net.URL
import java.nio.charset.StandardCharsets
import java.util.concurrent.*

class UnsupportedUrlException(message: String?) : Exception(message)

data class Range(val start: Long, val end: Long)

class SliceDownload private constructor(private val url: String, private val location: String, private var maxSlice: Int, private var header: MutableMap<String, String>? = null) {
    private val sniffValidator = "bytes \\d-\\d/(?<TotalByteLength>.(\\d+))".toRegex()
    private var totalSize: Long? = null
    private var sliceSize: Long? = null
    private var lowerBound: Long = 2 shl 17
    private var sliced: Array<Range?>

    init {
        if (maxSlice >= 16) throw IllegalArgumentException("maxSlice cannot over 16")
        sliced = sliced()
    }

    private fun sliced(): Array<Range?> {
        totalSize = validateSniffer()
        sliceSize = Math.ceil(totalSize!! / maxSlice.toDouble()).toLong()
        if (sliceSize!! < lowerBound) {
            maxSlice = Math.ceil(totalSize!! / lowerBound.toDouble()).toInt()
            sliceSize = lowerBound
        }

        val array = arrayOfNulls<Range>(maxSlice - 1)
        var allocated: Long = 0
        allocated += sliceSize!!
        for (i in 1 until maxSlice) {
            sliceSize!!.apply {
                allocated += this
                array[i - 1] = when(i) {
                    1 -> Range(0, allocated)
                    maxSlice -> Range(allocated + 1, Long.MIN_VALUE)
                    else -> Range(allocated - this + 1, allocated)
                }
            }
        }
        return array
    }

    private fun validateSniffer(): Long {
        val sniffHeader: MutableMap<String, String> =
            if (header == null) mapOf("Range" to sniffRange) as MutableMap<String, String> else HashMap(header).apply {
                this["Range"] = sniffRange
            }
        val response = request(url, sniffHeader).allHeaders
        val total = response.find { it.name == "Content-Range" }?.value ?: throw UnsupportedUrlException("request url does not support get part of resource")
        return sniffValidator.matchEntire(total)!!.groups["TotalByteLength"]!!.value.apply { println(this) }.toLong()
    }


    private fun request(url: String, header: Map<String, String>? = null): CloseableHttpResponse {
        val get = HttpGet(url)
        header?.forEach { k, v -> get.addHeader(k, v) }
        val client = HttpClients.createDefault()
        EntityUtils.toString(client.execute(get).entity, StandardCharsets.UTF_8)
        return client.execute(get)
    }

    private fun allocateTask(range: Range, location: String, onProgressChangedCallback: ((Long) -> Unit)? = null, onCompleteCallback: (() -> Unit)? = null, onExceptionCallback: ((Exception) -> Unit)? = null): () -> Unit {
        val header = if (header == null) mapOf("Range" to "bytes=${range.start}-${if(range.end == Long.MIN_VALUE) "" else range.end.toString()}") else
            HashMap(header).apply {
                this["Range"] = "bytes=${range.start}-${range.end}"
            }
        return {
            downloadTask(url, location, header, range.start, onProgressChangedCallback, onCompleteCallback, onExceptionCallback)
        }
    }

    private fun downloadTask(url: String, location: String, header: Map<String, String>, startPos: Long, onProgressChangedCallback: ((Long) -> Unit)? = null, onCompleteCallback: (() -> Unit)? = null, onExceptionCallback: ((Exception) -> Unit)? = null) {
        File(location.substring(0, location.lastIndexOf('\\'))).apply {
            if (!exists()) mkdirs()
        }
        var byteRead: Int
        try {
            val httpURLConnection = (URL(url).openConnection() as HttpURLConnection).apply {
                header.forEach { this.setRequestProperty(it.key, it.value) }
            }
            httpURLConnection.inputStream.use {
                if (File(location).length() == httpURLConnection.contentLengthLong) return
                RandomAccessFile(File(location), "rwd").use { randomAccessStream ->
                    randomAccessStream.seek(startPos)
                    val buffer = ByteArray(1024); var k: Long = 0
                    while (it.read(buffer).apply { byteRead = this } != -1) {
                        onProgressChangedCallback?.invoke((k++ / httpURLConnection.contentLengthLong.toDouble() * 100000.toDouble()).toLong())
                        randomAccessStream.write(buffer, 0, byteRead)
                    }
                }
            }
        } catch (e: Exception) {
            onExceptionCallback?.invoke(e)
        }
        onCompleteCallback?.invoke()
    }

    private fun execute(onProgressChangedCallback: ((Long) -> Unit)? = null, onCompleteCallback: (() -> Unit)? = null, onExceptionCallback: ((Exception) -> Unit)? = null) {
        val taskProcessPool = Executors.newFixedThreadPool(maxSlice)
        val stopWatch = CountDownLatch(maxSlice)
        for (task in sliced) {
            taskProcessPool.execute {
                allocateTask(task!!, location, onProgressChangedCallback, onCompleteCallback, onExceptionCallback).invoke()
                stopWatch.countDown()
            }
        }
        try {
            stopWatch.await()
        } catch (ignored: InterruptedException) {}
    }

    companion object {
        fun run(url: String, location: String, header: MutableMap<String, String>? = null, maxSlice: Int? = null, onProgressChangedCallback: ((Long) -> Unit)?, onCompleteCallback: (() -> Unit)?, onExceptionCallback: ((Exception) -> Unit)?) {
            val instance = SliceDownload(url, location, maxSlice ?: 2 shl 3, header)
            instance.execute(onProgressChangedCallback, onCompleteCallback, onExceptionCallback)
        }

        private const val sniffRange = "bytes=0-0"
    }
}

fun main() {
    SliceDownload.run(
        "http://cn-hbxy-cmcc-v-09.acgvideo.com/upgcxcode/50/89/78788950/78788950-1-80.flv?expires=1551722100&platform=pc&ssig=v1onh2Mu6nnloD_E0H-PNw&oi=664113900&trid=7d98edb64e7d45b881b5673f730056a8&nfb=maPYqpoel5MI3qOUX6YpRA==&nfc=1",
        "D:\\MultiThreadDownloadTest\\video.flv",
        mutableMapOf(
            "Accept" to "*/*",
            "Accept-Encoding" to "gzip, deflate, br",
            "Accept-Language" to "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
            "User-Agent" to "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36",
            "Referer" to "https://www.bilibili.com/video/av44986949",
            "Origin" to "https://www.bilibili.com",
            "Connection" to "keep-alive",
            "Host" to "cn-hbxy-cmcc-v-09.acgvideo.com"
        ),
        10,
        {
            println("${Thread.currentThread().name}: $it%")
        },
        {
            println("complete")
        },
        {
            it.printStackTrace()
        }
    )
}
