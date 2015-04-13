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
package com.twitter.zipkin.elastic


import com.twitter.conversions.time._
import com.twitter.util.Duration
import com.twitter.util.Await
import com.twitter.util.Future
import com.twitter.zipkin.builder.Builder
import com.twitter.zipkin.storage.Index
import com.twitter.zipkin.storage.elastic.ElasticIndex
import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticDsl._

case class IndexBuilder() extends Builder[Index] { self =>

  def apply() = {
    val client = ElasticClient.local
    Await.result(Future.value(new ElasticIndex {}), 10.seconds)
  }
}
