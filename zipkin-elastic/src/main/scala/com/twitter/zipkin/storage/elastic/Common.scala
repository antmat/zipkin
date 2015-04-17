package com.twitter.zipkin.storage.elastic

import java.text.SimpleDateFormat
import java.util.{TimeZone, Calendar, Date, HashMap}

import com.sksamuel.elastic4s.ElasticClient
import com.twitter.logging.Logger
import com.twitter.util.{Future, Promise}
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.search.SearchHit

import scala.concurrent.{ExecutionContext, Future => ScalaFuture, Promise => ScalaPromise}
import scala.util.{Failure, Success}

class Common {
  
  val ec: ExecutionContext = ExecutionContext.global
  val client: ElasticClient = ElasticClient.remote(
    ImmutableSettings.builder().put("cluster.name", "elasticsearch_antmat").build(),
    "127.0.0.1",
    9301
  )
  val log = Logger.get(getClass.getName)

  implicit class ScalaFutureOps[A](sf: ScalaFuture[A]) {
    def asTwitter(implicit ec: ExecutionContext): Future[A] = {
      val tp = new Promise[A]

      sf.onComplete {
        case Success(v) => tp.setValue(v)
        case Failure(t) => tp.setException(t)
      }

      tp
    }
  }

  def get_index(): String = {
    val today = Calendar.getInstance().getTime()
    val format = new SimpleDateFormat("yyyy.MM.dd")
//    log.debug("FORMAT" + "logstash-" + format.format(today))
    "logstash-" + format.format(today)
  }

  def ts_convert(sh: SearchHit): Long = {
    val r_ts = sh.sourceAsMap().get("fields").asInstanceOf[HashMap[String, Object]].get("real_timestamp").asInstanceOf[Long]
    if(r_ts != 0) {
      r_ts
    }
    else {
      val simpleDateFormat = new SimpleDateFormat("y-M-d'T'H:m:s.S")
      simpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
      var ts = sh.sourceAsMap().get("timestamp").asInstanceOf[String];
      val us = ts.substring(ts.size - 3)
      ts = ts.substring(0, ts.size - 3)
      simpleDateFormat.parse(ts).getTime * 1000 + us.toLong
    }
  }

  def ts_format(ts: Long): String = {
    val simpleDateFormat = new SimpleDateFormat("y-M-d'T'H:m:s.S")
    simpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
    simpleDateFormat.format(new Date(ts/1000))
  }

}