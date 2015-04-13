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

import com.sksamuel.elastic4s.{SearchType, Executable, ElasticClient}
import com.sksamuel.elastic4s.ElasticDsl._
import com.twitter.logging.Logger

import com.twitter.util._
import com.twitter.zipkin.common.{AnnotationType, BinaryAnnotation, Span}
import com.twitter.zipkin.storage.{TraceIdDuration, IndexedTraceId, Index}
import java.nio.ByteBuffer
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.aggregations.bucket.terms.{StringTerms, Terms}
import org.elasticsearch.search.aggregations.metrics.min.Min

import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}

import scala.concurrent.{Future => ScalaFuture, Promise => ScalaPromise, ExecutionContext}
import scala.util.{Success, Failure}
import scala.collection.GenTraversableOnce
import scala.collection.JavaConverters._

import org.elasticsearch.common.settings

trait ElasticIndex extends Index {

  val elastic = new Common

  override def close() = {
    elastic.client.close()
  }

  override def getTraceIdsByName(serviceName: String, span: Option[String],
                                 endTs: Long, limit: Int): Future[Seq[IndexedTraceId]] = {
    elastic.log.debug("getTraceIdsByName: " + serviceName + ", span: " + span)

    elastic.ScalaFutureOps(elastic.client.execute(
    {
      search in elastic.get_index() query "service_name:"+serviceName aggs(
        agg terms "trace_id" field "trace_id"
        aggs(
          agg min "ts" field "@timestamp"
          )
        ) searchType SearchType.Count
    }
    ) ).asTwitter(elastic.ec) map {
      sr => {
        val ag = sr.getAggregations().get("trace_id").asInstanceOf[Terms]
        ag.getBuckets.asScala.map (
          b => {
            val t_id = IndexedTraceId(b.getKeyAsNumber.longValue(), b.getAggregations().get("ts").asInstanceOf[Min].value().toLong)
            elastic.log.debug("TraceID:" + t_id.timestamp + "," + t_id.traceId)
            t_id
          }
        )
      }
    }
  }
  override def getTraceIdsByAnnotation(serviceName: String, annotation: String, value: Option[ByteBuffer],
                                       endTs: Long, limit: Int): Future[Seq[IndexedTraceId]] = {
    elastic.log.debug("getTraceIdsByAnnotation")
    elastic.ScalaFutureOps(elastic.client.execute(
      { search in "" query "" aggs(agg terms "trace_id")}
    ) ).asTwitter(elastic.ec) map {
      sr =>
        sr.getHits().hits().map(
          sh =>
            IndexedTraceId(sh.field("trace_id").getValue[Long](), elastic.ts_convert(sh))
        )
    }
  }

  override def getTracesDuration(traceIds: Seq[Long]): Future[Seq[TraceIdDuration]] = {
    elastic.log.debug("getTracesDuration")
    elastic.ScalaFutureOps(elastic.client.execute(
      { search in elastic.get_index() query "" aggs(agg terms "trace_id")}
    ) ).asTwitter(elastic.ec) map {
      sr =>
        sr.getHits().hits().map(
          sh =>
            TraceIdDuration(
              sh.field("trace_id").getValue[Long](),
              sh.field("duration").getValue[Long](),
              elastic.ts_convert(sh)
            )
        )
    }
  }

  override def getServiceNames: Future[Set[String]] = {
    elastic.log.debug("getServiceNames")
    elastic.ScalaFutureOps(elastic.client.execute(
    {
      search in elastic.get_index() query "service_name:*" aggs(
          agg terms "service_name" field "service_name"
        ) searchType SearchType.Count
    }
    ) ).asTwitter(elastic.ec) map {
      sr => {
//        log.debug("SR:" + sr.toString)
        val ag = sr.getAggregations().get("service_name").asInstanceOf[StringTerms]
        ag.getBuckets.asScala.map (
          b => {
//            log.debug("B:" + b.getKey)
            b.getKey
          }
        ).toSet
      }
    }
  }

  override def getSpanNames(service: String): Future[Set[String]] = {
    elastic.log.debug("getSpanNames")
    elastic.ScalaFutureOps(elastic.client.execute(
    {
      search in elastic.get_index() query "span_name:*" aggs(
        agg terms "span_name" field "span_name"
        ) searchType SearchType.Count
    }
    ) ).asTwitter(elastic.ec) map {
      sr => {
//        log.debug("SR:" + sr.toString)
        val ag = sr.getAggregations().get("span_name").asInstanceOf[StringTerms]
        ag.getBuckets.asScala.map (
          b => {
//            log.debug("B:" + b.getKey)
            b.getKey
          }
        ).toSet
      }
    }
  }

  override def indexTraceIdByServiceAndName(span: Span) : Future[Unit] = {
    elastic.log.debug("indexTraceIdByServiceAndName")
    elastic.ScalaFutureOps(elastic.client.execute(
    { search in "" query "" aggs(agg terms "trace_id")}
    ) ).asTwitter(elastic.ec) map {
      sr =>
        sr.getHits().hits().map(
          sh =>
            IndexedTraceId(sh.field("trace_id").getValue[Long](), elastic.ts_convert(sh))
        )
    }
  }

  override def indexSpanByAnnotations(span: Span) : Future[Unit] = {
    elastic.log.debug("indexSpanByAnnotations")
    elastic.ScalaFutureOps(elastic.client.execute(
    { search in "" query "" aggs(agg terms "trace_id")}
    ) ).asTwitter(elastic.ec) map {
      sr =>
        sr.getHits().hits().map(
          sh =>
            IndexedTraceId(sh.field("trace_id").getValue[Long](), elastic.ts_convert(sh))
        )
    }
  }

  override def indexServiceName(span: Span): Future[Unit] = {
    elastic.log.debug("indexServiceName")
    elastic.ScalaFutureOps(elastic.client.execute(
    { search in "" query "" aggs(agg terms "trace_id")}
    ) ).asTwitter(elastic.ec) map {
      sr =>
        sr.getHits().hits().map(
          sh =>
            IndexedTraceId(sh.field("trace_id").getValue[Long](), elastic.ts_convert(sh))
        )
    }
  }

  override def indexSpanNameByService(span: Span): Future[Unit] = {
    elastic.log.debug("indexSpanNameByService")
    elastic.ScalaFutureOps(elastic.client.execute(
    { search in "" query "" aggs(agg terms "trace_id")}
    ) ).asTwitter(elastic.ec) map {
      sr =>
        sr.getHits().hits().map(
          sh =>
            IndexedTraceId(sh.field("trace_id").getValue[Long](), elastic.ts_convert(sh))
        )
    }
  }

  override def indexSpanDuration(span: Span): Future[Unit] = {
    elastic.log.debug("indexSpanDuration")
    elastic.ScalaFutureOps(elastic.client.execute(
    { search in "" query "" aggs(agg terms "trace_id")}
    ) ).asTwitter(elastic.ec) map {
      sr =>
        sr.getHits().hits().map(
          sh =>
            IndexedTraceId(sh.field("trace_id").getValue[Long](), elastic.ts_convert(sh))
        )
    }
  }

}
