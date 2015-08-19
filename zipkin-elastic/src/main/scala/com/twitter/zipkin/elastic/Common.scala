package com.twitter.zipkin.elastic

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, HashMap, TimeZone}

import com.sksamuel.elastic4s.{ElasticsearchClientUri, ElasticClient}
import com.twitter.logging._
import com.twitter.util.{Future, Promise}
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.search.SearchHit

import scala.concurrent.{ExecutionContext, Future => ScalaFuture, Promise => ScalaPromise}
import scala.util.{Failure, Success}

object ts_traits {
  def default_us_extractor = (ts:String) => {
    ts.substring(ts.size - 3).toLong
  }
  def default_ts_filter = (ts:String) => {
    ts.substring(0, ts.size - 3)
  }
}

class Common (
               cluster_name: String,
               index_format: String,
               es_connection_string: String,
               val timestamp_field: String = "timestamp",
               val trace_id_field: String = "trace_id",
               val span_id_field: String = "span_id",
               val parent_id_field: String = "parent_id",
               val span_name_field: String = "span_name",
               val service_name_field: String = "service_name",
               val real_timestamp_field: String = "real_timestamp",
               val message_field: String = "message",
               val ts_format_string: String = "y-M-d'T'H:m:s.S",
               val timestamp_us_extractor: (String) => Long = ts_traits.default_us_extractor,
               val timestamp_filter: (String) => String = ts_traits.default_ts_filter
               ){

  val ec: ExecutionContext = ExecutionContext.global
  val client: ElasticClient = ElasticClient.remote(
    ImmutableSettings.builder().put("cluster.name", cluster_name).build(),
    ElasticsearchClientUri(es_connection_string)
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

    log.debug("FORMAT:" + format.format(today))
    format.format(today)
  }

  def get_index(time_ms:Long): String = {
    log.debug("FORMAT ms" + time_ms.toString)
    val d = new Date()
    d.setTime(time_ms/1000)
    log.debug("FORMAT RES:" + format.format(d))
    format.format(d)
  }

  def ts_convert(sh: SearchHit): Long = {
    val r_ts = sh.sourceAsMap().get("fields").asInstanceOf[HashMap[String, Object]].get(real_timestamp_field).asInstanceOf[Long]
    if(r_ts != 0) {
      r_ts
    }
    else {
      val simpleDateFormat = new SimpleDateFormat(ts_format_string)
      simpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
      var ts = sh.sourceAsMap().get(timestamp_field).asInstanceOf[String];
      val us = timestamp_us_extractor(ts)
      ts = timestamp_filter(ts)
      simpleDateFormat.parse(ts).getTime * 1000 + us.toLong
    }
  }

  def ts_format(ts: Long): String = {
    val simpleDateFormat = new SimpleDateFormat(ts_format_string)
    simpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
    simpleDateFormat.format(new Date(ts/1000))
  }

}