package jobapp

import com.opencsv.{CSVParser, CSVParserBuilder, CSVReader}
import org.apache.spark.{SparkContext, SparkConf}
import org.joda.time.DateTime
import org.rogach.scallop.ScallopConf

class Conf(args: Array[String]) extends ScallopConf(args) {
  val partitions = opt[Int](required = false, noshort = true, default = Some(8))
  val input = opt[String](required = true, noshort = true)
  // max diff between two requests in one session
  val maxGapInSession = opt[Int](required = false, noshort = true, default = Some(120))
}

class CsvParserHolder(createParser: () => CSVParser) extends Serializable {
  lazy val parser = createParser()
}

object CsvParserHolder {
  def apply(): CsvParserHolder = {
    def creator() = {
      val csvParser = new CSVParserBuilder().withQuoteChar('"').withSeparator(' ').build()
      csvParser
    }
    new CsvParserHolder(creator)
  }
}

object Weblog {

  final def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    val sparkConf = new SparkConf()
      .setAppName("Weblog")
      .set("spark.default.parallelism", "%d".format(conf.partitions()))
    val sparkContext = new SparkContext(sparkConf)

    val input = sparkContext.textFile(conf.input())

    val csvParserHolder = sparkContext.broadcast(CsvParserHolder())

    input.map {
      case line =>
        val sp = csvParserHolder.value.parser.parseLine(line)
        val timestamp = new DateTime(sp(0))
        val clientIp = sp(2).split(':')(0)
        val request = sp(11)
        val (requestMethod, requestUrl, protocol) = request.split(' ')
        val clientId = clientIp
        (clientId, new {val timestamp = timestamp; val clientIp = clientIp; val requestUrl = requestUrl; val raw = sp})
    }.groupByKey().map {
      case (clientId, data) =>
        val sorted = data.toSeq.sortBy({_.timestamp})
        
    }
  }
}
// 2015-07-22T09:00:28.019143Z marketpalce-shop 123.242.248.130:54635 10.0.6.158:80 0.000022 0.026109 0.00002 200 200 0 699 "GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2
