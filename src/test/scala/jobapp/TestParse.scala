package jobapp

import org.scalatest.FunSuite

/**
  * Created by khachik on 2015-12-10.
  */
class TestParse extends FunSuite {
  test("Url with quotes") {
    val line = """2015-07-22T16:10:38.028609Z marketpalce-shop 106.51.132.54:4841 10.0.4.227:80 0.000022 0.000989 0.00002 400 400 0 166 "GET https://paytm.com:443/'"\'\");|]*{%0d%0a<%00>/about/ HTTP/1.1" "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)" DHE-RSA-AES128-SHA TLSv1"""
    val Array(timestamp, clientIp, requestUrl, userAgent) = Weblog.parse(line)
    assertResult("2015-07-22T16:10:38.028609Z")(timestamp)
    assertResult("106.51.132.54")(clientIp)
    assertResult("""https://paytm.com:443/'"\'\");|]*{%0d%0a<%00>/about/""")(requestUrl)
    assertResult("Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)")(userAgent)
  }

  test("Error response") {
    val line = """2015-07-22T09:00:35.890581Z marketpalce-shop 106.66.99.116:37913 - -1 -1 -1 504 0 0 0 "GET https://paytm.com:443/shop/orderdetail/1116223940?channel=web&version=2 HTTP/1.1" "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/14.0.794.0 Safari/535.1" ECDHE-RSA-AES128-SHA TLSv1"""
    val Array(timestamp, clientIp, requestUrl, userAgent) = Weblog.parse(line)
    assertResult("2015-07-22T09:00:35.890581Z")(timestamp)
    assertResult("106.66.99.116")(clientIp)
    assertResult("""https://paytm.com:443/shop/orderdetail/1116223940?channel=web&version=2""")(requestUrl)
    assertResult("Mozilla/5.0 (Windows NT 6.1) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/14.0.794.0 Safari/535.1")(userAgent)
  }


  test("Empty User-agent") {
    val line = """2015-07-22T17:45:17.549417Z marketpalce-shop 106.202.23.193:47132 10.0.4.150:80 0.000021 0.001118 0.000032 200 200 0 1150 "GET https://paytm.com:443/favicon.ico HTTP/1.1" "" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2"""
    val Array(timestamp, clientIp, requestUrl, userAgent) = Weblog.parse(line)
    assertResult("2015-07-22T17:45:17.549417Z")(timestamp)
    assertResult("106.202.23.193")(clientIp)
    assertResult("""https://paytm.com:443/favicon.ico""")(requestUrl)
    assertResult("")(userAgent)
  }
 }
