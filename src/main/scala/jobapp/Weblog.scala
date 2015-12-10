package jobapp

import com.opencsv.{CSVParser, CSVParserBuilder}
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.joda.time.format.PeriodFormatterBuilder
import org.joda.time.{DateTime, Duration}
import org.rogach.scallop.ScallopConf

import scala.collection.mutable.ListBuffer

class Conf(args: Array[String]) extends ScallopConf(args) {
  val partitions = opt[Int](required = false, noshort = true, default = Some(8))
  val input = opt[String](required = true, noshort = true)
  // max diff between two requests in one session in seconds
  val maxGapInSessions = opt[Int](required = false, noshort = true, default = Some(300))
}

class CsvParserHolder(createParser: () => CSVParser) extends Serializable {
  lazy val parser = createParser()
}

object CsvParserHolder {
  def apply(): CsvParserHolder = {
    def creator() = {
      new CSVParserBuilder()
        .withQuoteChar('"').withSeparator(' ')
        .withEscapeChar('\\')
        .withStrictQuotes(false)
        .build()

    }
    new CsvParserHolder(creator)
  }
}

case class LogEntry (id:String, url:String, timestamp:DateTime, data:Array[String])


object Weblog extends Logging {


  /**
   * building:
   *   in the root dir run:
   *   mvn clean package
   * running from IntelliJ:
   *   org.apache.spark.deploy.SparkSubmit \
   *     --master local[4] \
   *     --driver-memory 1G \
   *     --executor-memory 1G \
   *     --driver-cores 1 \
   *     --executor-cores 1 \
   *     --num-executors 2 \
   *     --class jobapp.Weblog \
   *     target/weblog-1.0-SNAPSHOT-jar-with-dependencies.jar \
   *     --input data/2015_07_22_mktplace_shop_web_log_sample.log.gz
   * Command-line is the same if running as spark-submit:
   *   spark-submit --master local[4] \
   *      ...
   * Example output (with max-gap-in-sessions = 300):
   *  INFO Weblog: Analyzed log file data/2015_07_22_mktplace_shop_web_log_sample.log.gz
   *  INFO Weblog: Separated sessions with max gap = 300
   *  INFO Weblog: Unique clients: 90544 with 118137 sessions
   *  INFO Weblog: Average session time: 58.385s
   *  INFO Weblog: Most engaged client is from: 164.100.96.254, stayed 18m54.83s Requested 76 unique resources
   *  INFO Weblog: Max unique URLs per session: 5496
   */
  final def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    val sparkConf = new SparkConf()
      .setAppName("Weblog")
      .set("spark.default.parallelism", "%d".format(conf.partitions()))
    val sparkContext = new SparkContext(sparkConf)

    val maxGap = conf.maxGapInSessions() * 1000 // we have milliseconds in timestamp

    val csvParserHolder = sparkContext.broadcast(CsvParserHolder())

    val clientData = sparkContext.textFile(conf.input())
      .map {
        case line =>
          val Array(timestamp, clientIp, requestUrl, userAgent) = parse(line)

            val clientId = clientIp // combine with user_agent?
            (clientId, (clientIp, requestUrl, new DateTime(timestamp), userAgent))
      }.groupByKey().map {
        case (clientId, data) =>
          val clientSessions = split[(String, String, DateTime, String)](data,
            d => d._3.getMillis, maxGap
          )
          (clientId, clientSessions)
      }.cache()

    val sessions = clientData.flatMap {
      case (user, userSessions) =>
        userSessions.map(session => (user, session))
    }.cache()

    val stats = sessions.map {
      case (clientId, visits) =>
        val first = visits.head._3
        val duration = new Duration(visits.head._3, visits.last._3)
        val nUniqueUrls = visits.map {
          case (_, requestUrl, _, _) => requestUrl
        }.distinct.size
        (clientId, duration, nUniqueUrls)
    }.cache()

    val nClients = clientData.count()
    val nSessions = sessions.count()

    val averageSession = new Duration(
      stats.map {
        case (_, duration, _) => duration.getMillis
      }.mean().toLong
    )

    val mostEngaged = stats.max()(new Ordering[(String, Duration, Int)] {
      override def compare(x: (String, Duration, Int), y: (String, Duration, Int)): Int = {
        x._2.compareTo(y._2)
      }
    })

    // req 3. Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.
    // not clear for all the sessions or max/min/avg ?
    // calculating max for now
    val maxUniqueUrls = stats.map {
      case (_, _, nUniqueUrls) => nUniqueUrls
    }.max()

    val durationFormatter =
      new PeriodFormatterBuilder().appendHours().appendSeparator("h")
        .appendMinutes().appendSeparator("m")
        .appendSeconds().appendSeparator(".")
        .appendMillis().appendLiteral("s")
        .toFormatter

    clientData.unpersist()
    sessions.unpersist()
    stats.unpersist()

    sparkContext.stop()
    logInfo("Analyzed log file " + conf.input())
    logInfo("Separated sessions with max gap = " + conf.maxGapInSessions())
    logInfo("Unique clients: " + nClients + " with " + nSessions + " sessions ")
    logInfo("Average session time: " + durationFormatter.print(averageSession.toPeriod))
    logInfo("Most engaged client is from: " + mostEngaged._1 +
            ", stayed " + durationFormatter.print(mostEngaged._2.toPeriod) +
            " Requested " + mostEngaged._3 + " unique resources")
    logInfo("Max unique URLs per session: " + maxUniqueUrls)
  }

  def parse(line:String):Array[String] = {
    // You're gonna tell me I can't parse a context free grammar with regex, aren't you?
    val elbLog = """([^ ]+) ([^ ]+) ([^ :]+):\d+ ([^ ]+) ([0-9.-]+) ([0-9.-]+) ([0-9.-]+) (\d+) (\d+) (\d+) (\d+) "[A-Z]+ ([^ ]+) [^ ]+" "([^"]*)" ([^ "]+) ([^ "]+)""".r
    // timestamp elb
    // client:port backend:port
    // request_processing_time backend_processing_time response_processing_time
    // elb_status_code backend_status_code
    // received_bytes sent_bytes
    // "request"
    // "user_agent"
    // ssl_cipher ssl_protocol

    // 2015-07-22T16:10:38.028609Z marketpalce-shop
    // 106.51.132.54:4841 10.0.4.227:80
    // 0.000022 0.000989 0.00002
    // 400 400
    // 0 166
    // "GET https://paytm.com:443/'"\'\");|]*{%0d%0a<%00>/about/ HTTP/1.1"
    // "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)"
    // DHE-RSA-AES128-SHA TLSv1
    line match {
      case elbLog(timestamp, elb,
                  clientIp, serverAddress,
                  reqTime, backTime, respTime,
                  eldCode, backCode,
                  recvBytes, sentBytes,
                  request,
                  userAgent,
                  sslCipher, sslProtocol) =>
        Array(timestamp, clientIp, request, userAgent)
      case _ =>
        throw new IllegalStateException("Something went wrong, cannot parse \"" + line + "\"")
    }
  }

  /**
   * Sorted chunks of items, the difference between each sequential item in
   * each chunk is not more than maxDiff.
   * split({5, 7, 1, 2, 3}, x => x, 2} =
   *   {{1, 2, 3}, {5, 7}}
   * @param items items to split
   * @param keyer key generator, a T => Long function
   * @param maxDiff maximum diff to keep items in the same bucket
   * @tparam V
   * @return Sequence of buckets
   */
  def split[V](items:Iterable[V], keyer:V=>Long, maxDiff:Long) : Seq[Seq[V]] = {
    if(items.isEmpty) {
      throw new UnsupportedOperationException("empty.split")
    }
    val sorted = items.map(item => (keyer(item), item)).toList.sortBy({_._1})

    var tail = sorted
    var result = new ListBuffer[List[V]]
    while(tail.nonEmpty) {
      var last = tail.head._1
      val (chunk, rest) = tail.span {
        case (k, v) =>
          val tmp = last
          last = k
          (k - tmp) <= maxDiff
      }
      result += chunk.map{case (k, v) => v}
      tail = rest
    }
    result
  }
}
// 2015-07-22T09:00:28.019143Z marketpalce-shop 123.242.248.130:54635 10.0.6.158:80 0.000022 0.026109 0.00002 200 200 0 699 "GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2
