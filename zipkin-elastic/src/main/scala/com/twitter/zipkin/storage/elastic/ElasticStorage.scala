/*
 * Copyright 2012 Tumblr Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.zipkin.storage.elastic

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, HashMap}

import com.sksamuel.elastic4s.ElasticDsl._
import com.twitter.util.{Promise, Future, Duration}
import com.twitter.zipkin.common.{Endpoint, Annotation, Span}
import com.twitter.zipkin.elastic.Common
import com.twitter.zipkin.storage.{IndexedTraceId, Storage}

import scala.util.{Failure, Success}

trait ElasticStorage extends Storage {

  val elastic: Common

  override def close() = {
    elastic.client.close()
  }


  override def storeSpan(span: Span): Future[Unit] = {
    throw new NotImplementedError
    elastic.log.debug("storeSpan")
    elastic.ScalaFutureOps(elastic.client.execute(
    { index into "" -> "" fields {"" -> ""}}
    ) ).asTwitter(elastic.ec) map { _ =>}
  }

  override def setTimeToLive(traceId: Long, ttl: Duration): Future[Unit] = {
    throw new NotImplementedError
    elastic.log.debug("setTimeToLive")
    elastic.ScalaFutureOps(elastic.client.execute(
    { index into "" -> "" fields {"" -> ""}}
    ) ).asTwitter(elastic.ec) map { _ =>}
  }

  override def getTimeToLive(traceId: Long): Future[Duration] = {
    throw new NotImplementedError
    elastic.log.debug("getTimeToLive")
    elastic.ScalaFutureOps(elastic.client.execute(
    { index into "" -> "" fields {"" -> ""}}
    ) ).asTwitter(elastic.ec) map { _ => Duration.fromSeconds(42)}
  }


  override def getSpansByTraceId(traceId: Long) : Future[Seq[Span]] = {
    elastic.log.debug("getSpansByTraceId")
    fetchTraceById(traceId) map (_.get)
  }


  override def getSpansByTraceIds(traceIds: Seq[Long]): Future[Seq[Seq[Span]]] = {
    elastic.log.debug("getSpansByTraceIds")
    Future.collect(traceIds map (traceId => fetchTraceById(traceId))) map (_ flatten)
  }

  private[this] def fetchTraceById(traceId: Long): Future[Option[Seq[Span]]] = {
    elastic.log.debug("fetchTraceById: "+ traceId)
    elastic.ScalaFutureOps(elastic.client.execute(
      { search in elastic.get_index() query elastic.trace_id_field+":\""+elastic.id_generator(traceId)+"\"" limit(10000)}
    ) ).asTwitter(elastic.ec) map {
      sr => {
        Some(sr.getHits().hits().map(
          sh => {
            val b = Seq.newBuilder[Span]
            val map = sh.sourceAsMap().get("fields").asInstanceOf[HashMap[String, Object]]
            val p_id = map.get(elastic.parent_id_field)
            var parent_id: Option[Long] = None
            if (p_id != null) {
              val p_id_val = elastic.id_parser(p_id.asInstanceOf[String])
              if (p_id_val != 0) {
                parent_id = Some(p_id_val)
              }
            }
            val ann_builder = List.newBuilder[Annotation]
            var service_name = map.get(elastic.service_name_field).asInstanceOf[String];
            if (service_name == null) {
              service_name = "<unknown>"
            }
            ann_builder += new Annotation(
              elastic.ts_convert(sh),
              sh.sourceAsMap().get(elastic.message_field).asInstanceOf[String],
              Some(Endpoint(0, 0, service_name)),
              None
            )
            var span_name = map.get(elastic.span_name_field).asInstanceOf[String]
            if (span_name == null || span_name.isEmpty()) {
              span_name = "<unknown>"
            }
            //elastic.log.debug("SH:"+ sh.sourceAsMap().toString);
            //elastic.log.debug("SNAME:" + map.get(elastic.service_name_field).asInstanceOf[String]);
            val span = Span(
              elastic.id_parser(map.get(elastic.trace_id_field).asInstanceOf[String]),
              span_name,
              elastic.id_parser(map.get(elastic.span_id_field).asInstanceOf[String]),
              parent_id,
              ann_builder.result(),
              Seq.empty
            )
            //elastic.log.debug("SH:" + sh.sourceAsMap().toString);
            elastic.log.debug("(" + traceId + ")SPAN:" + span.toString)
            span
          }
        ).toSeq)
      }
    }
  }

  override def getDataTimeToLive: Int = {
    throw new NotImplementedError
    elastic.log.debug("getDataTimeToLive")
    42
  }

  override def tracesExist(traceIds: Seq[Long]): Future[Set[Long]] = {
    throw new NotImplementedError
    elastic.log.debug("tracesExist")
    Future.value(Set.empty)
  }

}