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

class Common (
               cluster_name: String,
               index_format: String,
               host: String,
               port: Int
               ){
  
  val ec: ExecutionContext = ExecutionContext.global
  val client: ElasticClient = ElasticClient.remote(
    ImmutableSettings.builder().put("cluster.name", cluster_name).build(),
    host,
    port
  )

  val log = Logger.get(getClass.getName)

  val format = new SimpleDateFormat(index_format)

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

//    log.debug("FORMAT" + "logstash-" + format.format(today))
    format.format(today)
  }

  def get_index(time_ms:Long): String = {
    val d = new Date().setTime(time_ms)
    //    log.debug("FORMAT" + "logstash-" + format.format(today))
    format.format(d)
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