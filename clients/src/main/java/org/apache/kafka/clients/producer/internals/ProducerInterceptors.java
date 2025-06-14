/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;


import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.internals.Plugin;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.RecordBatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.List;

/**
 * A container that holds the list {@link org.apache.kafka.clients.producer.ProducerInterceptor}
 * and wraps calls to the chain of custom interceptors.
 */
public class ProducerInterceptors<K, V> implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(ProducerInterceptors.class);
    private final List<Plugin<ProducerInterceptor<K, V>>> interceptorPlugins;

    public ProducerInterceptors(List<ProducerInterceptor<K, V>> interceptors, Metrics metrics) {
        this.interceptorPlugins = Plugin.wrapInstances(interceptors, metrics, ProducerConfig.INTERCEPTOR_CLASSES_CONFIG);
    }

    /**
     * This is called when client sends the record to KafkaProducer, before key and value gets serialized.
     * The method calls {@link ProducerInterceptor#onSend(ProducerRecord)} method. ProducerRecord
     * returned from the first interceptor's onSend() is passed to the second interceptor onSend(), and so on in the
     * interceptor chain. The record returned from the last interceptor is returned from this method.
     *
     * This method does not throw exceptions. Exceptions thrown by any of interceptor methods are caught and ignored.
     * If an interceptor in the middle of the chain, that normally modifies the record, throws an exception,
     * the next interceptor in the chain will be called with a record returned by the previous interceptor that did not
     * throw an exception.
     * 当 Client 端在 key 和 value 序列化之前将记录发送到 KafkaProducer 时，将调用此函数。
     * 该方法调用 {@link ProducerInterceptoronSend（ProducerRecord）} 方法。
     * 从第一个拦截器的 onSend（） 返回的 ProducerRecord 被传递给第二个拦截器 onSend（），
     * 依此类推，在拦截器链中。从最后一个拦截器返回的记录从此方法返回。
     * @param record the record from client
     * @return producer record to send to topic/partition
     */
    public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {
        ProducerRecord<K, V> interceptRecord = record;
        for (Plugin<ProducerInterceptor<K, V>> interceptorPlugin : this.interceptorPlugins) {
            try {
                interceptRecord = interceptorPlugin.get().onSend(interceptRecord);
            } catch (Exception e) {
                // do not propagate interceptor exception, log and continue calling other interceptors
                // be careful not to throw exception from here
                if (record != null)
                    log.warn("Error executing interceptor onSend callback for topic: {}, partition: {}", record.topic(), record.partition(), e);
                else
                    log.warn("Error executing interceptor onSend callback", e);
            }
        }
        return interceptRecord;
    }

    /**
     * This method is called when the record sent to the server has been acknowledged, or when sending the record fails before
     * it gets sent to the server. This method calls {@link ProducerInterceptor#onAcknowledgement(RecordMetadata, Exception, Headers)}
     * method for each interceptor.
     *
     * This method does not throw exceptions. Exceptions thrown by any of interceptor methods are caught and ignored.
     *
     * @param metadata The metadata for the record that was sent (i.e. the partition and offset).
     *                 If an error occurred, metadata will only contain valid topic and maybe partition.
     * @param exception The exception thrown during processing of this record. Null if no error occurred.
     * @param headers The headers for the record that was sent
     */
    public void onAcknowledgement(RecordMetadata metadata, Exception exception, Headers headers) {
        for (Plugin<ProducerInterceptor<K, V>> interceptorPlugin : this.interceptorPlugins) {
            try {
                interceptorPlugin.get().onAcknowledgement(metadata, exception, headers);
            } catch (Exception e) {
                // do not propagate interceptor exceptions, just log
                log.warn("Error executing interceptor onAcknowledgement callback", e);
            }
        }
    }

    /**
     * This method is called when sending the record fails in {@link ProducerInterceptor#onSend
     * (ProducerRecord)} method. This method calls {@link ProducerInterceptor#onAcknowledgement(RecordMetadata, Exception, Headers)}
     * method for each interceptor
     *
     * @param record The record from client
     * @param interceptTopicPartition  The topic/partition for the record if an error occurred
     *        after partition gets assigned; the topic part of interceptTopicPartition is the same as in record.
     * @param exception The exception thrown during processing of this record.
     */
    public void onSendError(ProducerRecord<K, V> record, TopicPartition interceptTopicPartition, Exception exception) {
        for (Plugin<ProducerInterceptor<K, V>> interceptorPlugin : this.interceptorPlugins) {
            try {
                Headers headers = record != null ? record.headers() : new RecordHeaders();
                if (headers instanceof RecordHeaders && !((RecordHeaders) headers).isReadOnly()) {
                    // make a copy of the headers to make sure we don't change the state of origin record's headers.
                    // original headers are still writable because client might want to mutate them before retrying.
                    RecordHeaders recordHeaders = (RecordHeaders) headers;
                    headers = new RecordHeaders(recordHeaders);
                    ((RecordHeaders) headers).setReadOnly();
                }
                if (record == null && interceptTopicPartition == null) {
                    interceptorPlugin.get().onAcknowledgement(null, exception, headers);
                } else {
                    if (interceptTopicPartition == null) {
                        interceptTopicPartition = extractTopicPartition(record);
                    }
                    interceptorPlugin.get().onAcknowledgement(new RecordMetadata(interceptTopicPartition, -1, -1,
                                    RecordBatch.NO_TIMESTAMP, -1, -1), exception, headers);
                }
            } catch (Exception e) {
                // do not propagate interceptor exceptions, just log
                log.warn("Error executing interceptor onAcknowledgement callback", e);
            }
        }
    }

    public static <K, V> TopicPartition extractTopicPartition(ProducerRecord<K, V> record) {
        return new TopicPartition(record.topic(), record.partition() == null ? RecordMetadata.UNKNOWN_PARTITION : record.partition());
    }

    /**
     * Closes every interceptor in a container.
     */
    @Override
    public void close() {
        for (Plugin<ProducerInterceptor<K, V>> interceptorPlugin : this.interceptorPlugins) {
            try {
                interceptorPlugin.close();
            } catch (Exception e) {
                log.error("Failed to close producer interceptor ", e);
            }
        }
    }
}