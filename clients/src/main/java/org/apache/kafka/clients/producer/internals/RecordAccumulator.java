/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentMap;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.utils.CopyOnWriteMap;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class acts as a queue that accumulates records into {@link org.apache.kafka.common.record.MemoryRecords}
 * instances to be sent to the server.
 * <p>
 * The accumulator uses a bounded amount of memory and append calls will block when that memory is exhausted, unless
 * this behavior is explicitly disabled.
 */
public final class RecordAccumulator {

    private static final Logger log = LoggerFactory.getLogger(RecordAccumulator.class);

    private volatile boolean closed;
    private int drainIndex;
    private final int batchSize;
    private final long lingerMs;
    private final long retryBackoffMs;
    private final BufferPool free;
    private final Time time;
    private final ConcurrentMap<TopicPartition, Deque<RecordBatch>> batches;

    /**
     * Create a new record accumulator
     * 
     * @param batchSize The size to use when allocating {@link org.apache.kafka.common.record.MemoryRecords} instances
     * @param totalSize The maximum memory the record accumulator can use.
     * @param lingerMs An artificial delay time to add before declaring a records instance that isn't full ready for
     *        sending. This allows time for more records to arrive. Setting a non-zero lingerMs will trade off some
     *        latency for potentially better throughput due to more batching (and hence fewer, larger requests).
     * @param retryBackoffMs An artificial delay time to retry the produce request upon receiving an error. This avoids
     *        exhausting all retries in a short period of time.
     * @param blockOnBufferFull If true block when we are out of memory; if false throw an exception when we are out of
     *        memory
     * @param metrics The metrics
     * @param time The time instance to use
     */
    public RecordAccumulator(int batchSize,
                             long totalSize,
                             long lingerMs,
                             long retryBackoffMs,
                             boolean blockOnBufferFull,
                             Metrics metrics,
                             Time time) {
        this.drainIndex = 0;
        this.closed = false;
        this.batchSize = batchSize;
        this.lingerMs = lingerMs;
        this.retryBackoffMs = retryBackoffMs;
        this.batches = new CopyOnWriteMap<TopicPartition, Deque<RecordBatch>>();
        this.free = new BufferPool(totalSize, batchSize, blockOnBufferFull);
        this.time = time;
        registerMetrics(metrics);
    }

    private void registerMetrics(Metrics metrics) {
        metrics.addMetric("waiting-threads",
                          "The number of user threads blocked waiting for buffer memory to enqueue their records",
                          new Measurable() {
                              public double measure(MetricConfig config, long nowMs) {
                                  return free.queued();
                              }
                          });
        metrics.addMetric("buffer-total-bytes",
                          "The maximum amount of buffer memory the client can use (whether or not it is currently used).",
                          new Measurable() {
                              public double measure(MetricConfig config, long nowMs) {
                                  return free.totalMemory();
                              }
                          });
        metrics.addMetric("buffer-available-bytes",
                          "The total amount of buffer memory that is not being used (either unallocated or in the free list).",
                          new Measurable() {
                              public double measure(MetricConfig config, long nowMs) {
                                  return free.availableMemory();
                              }
                          });
    }

    /**
     * Add a record to the accumulator.
     * <p>
     * This method will block if sufficient memory isn't available for the record unless blocking has been disabled.
     * 
     * @param tp The topic/partition to which this record is being sent
     * @param key The key for the record
     * @param value The value for the record
     * @param compression The compression codec for the record
     * @param callback The user-supplied callback to execute when the request is complete
     */
    public FutureRecordMetadata append(TopicPartition tp, byte[] key, byte[] value, CompressionType compression, Callback callback) throws InterruptedException {
        if (closed)
            throw new IllegalStateException("Cannot send after the producer is closed.");
        // check if we have an in-progress batch
        Deque<RecordBatch> dq = dequeFor(tp);
        synchronized (dq) {
            RecordBatch batch = dq.peekLast();
            if (batch != null) {
                FutureRecordMetadata future = batch.tryAppend(key, value, callback);
                if (future != null)
                    return future;
            }
        }

        // we don't have an in-progress record batch try to allocate a new batch
        int size = Math.max(this.batchSize, Records.LOG_OVERHEAD + Record.recordSize(key, value));
        log.trace("Allocating a new {} byte message buffer for topic {} partition {}", size, tp.topic(), tp.partition());
        ByteBuffer buffer = free.allocate(size);
        synchronized (dq) {
            RecordBatch last = dq.peekLast();
            if (last != null) {
                FutureRecordMetadata future = last.tryAppend(key, value, callback);
                if (future != null) {
                    // Somebody else found us a batch, return the one we waited for! Hopefully this doesn't happen
                    // often...
                    free.deallocate(buffer);
                    return future;
                }
            }
            MemoryRecords records = MemoryRecords.emptyRecords(buffer, compression);
            RecordBatch batch = new RecordBatch(tp, records, time.milliseconds());
            FutureRecordMetadata future = Utils.notNull(batch.tryAppend(key, value, callback));

            dq.addLast(batch);
            return future;
        }
    }

    /**
     * Re-enqueue the given record batch in the accumulator to retry
     */
    public void reenqueue(RecordBatch batch, long nowMs) {
        batch.attempts++;
        batch.lastAttemptMs = nowMs;
        Deque<RecordBatch> deque = dequeFor(batch.topicPartition);
        synchronized (deque) {
            deque.addFirst(batch);
        }
    }

    /**
     * Get a list of nodes whose partitions are ready to be sent.
     * <p>
     * A destination node is ready to send data if ANY one of its partition is not backing off the send
     * and ANY of the following are true :
     * <ol>
     * <li>The record set is full
     * <li>The record set has sat in the accumulator for at least lingerMs milliseconds
     * <li>The accumulator is out of memory and threads are blocking waiting for data (in this case all partitions are
     * immediately considered ready).
     * <li>The accumulator has been closed
     * </ol>
     */
    public Set<Node> ready(Cluster cluster, long nowMs) {
        Set<Node> readyNodes = new HashSet<Node>();
        boolean exhausted = this.free.queued() > 0;

        for (Map.Entry<TopicPartition, Deque<RecordBatch>> entry : this.batches.entrySet()) {
            TopicPartition part = entry.getKey();
            Deque<RecordBatch> deque = entry.getValue();
            // if the leader is unknown use an Unknown node placeholder
            Node leader = cluster.leaderFor(part);
            if (!readyNodes.contains(leader)) {
                synchronized (deque) {
                    RecordBatch batch = deque.peekFirst();
                    if (batch != null) {
                        boolean backingOff = batch.attempts > 0 && batch.lastAttemptMs + retryBackoffMs > nowMs;
                        boolean full = deque.size() > 1 || batch.records.isFull();
                        boolean expired = nowMs - batch.createdMs >= lingerMs;
                        boolean sendable = full || expired || exhausted || closed;
                        if (sendable && !backingOff)
                            readyNodes.add(leader);
                    }
                }
            }
        }

        return readyNodes;
    }

    /**
     * @return Whether there is any unsent record in the accumulator.
     */
    public boolean hasUnsent() {
        for (Map.Entry<TopicPartition, Deque<RecordBatch>> entry : this.batches.entrySet()) {
            Deque<RecordBatch> deque = entry.getValue();
            synchronized (deque) {
                if (deque.size() > 0)
                    return true;
            }
        }
        return false;
    }

    /**
     * Drain all the data for the given nodes and collate them into a list of
     * batches that will fit within the specified size on a per-node basis.
     * This method attempts to avoid choosing the same topic-node over and over.
     * 
     * @param cluster The current cluster metadata
     * @param nodes The list of node to drain
     * @param maxSize The maximum number of bytes to drain
     * @param nowMs The current unix time in milliseconds
     * @return A list of {@link RecordBatch} for each node specified with total size less than the requested maxSize.
     *         TODO: There may be a starvation issue due to iteration order
     */
    public Map<Integer, List<RecordBatch>> drain(Cluster cluster, Set<Node> nodes, int maxSize, long nowMs) {
        if (nodes.isEmpty())
            return Collections.emptyMap();

        Map<Integer, List<RecordBatch>> batches = new HashMap<Integer, List<RecordBatch>>();
        for (Node node : nodes) {
            int size = 0;
            List<PartitionInfo> parts = cluster.partitionsForNode(node.id());
            List<RecordBatch> ready = new ArrayList<RecordBatch>();
            /* to make starvation less likely this loop doesn't start at 0 */
            int start = drainIndex = drainIndex % parts.size();
            do {
                PartitionInfo part = parts.get(drainIndex);
                Deque<RecordBatch> deque = dequeFor(new TopicPartition(part.topic(), part.partition()));
                if (deque != null) {
                    synchronized (deque) {
                        RecordBatch first = deque.peekFirst();
                        if (first != null) {
                            if (size + first.records.sizeInBytes() > maxSize && !ready.isEmpty()) {
                                // there is a rare case that a single batch size is larger than the request size due
                                // to compression; in this case we will still eventually send this batch in a single
                                // request
                                break;
                            } else {
                                RecordBatch batch = deque.pollFirst();
                                batch.records.close();
                                size += batch.records.sizeInBytes();
                                ready.add(batch);
                                batch.drainedMs = nowMs;
                            }
                        }
                    }
                }
                this.drainIndex = (this.drainIndex + 1) % parts.size();
            } while (start != drainIndex);
            batches.put(node.id(), ready);
        }
        return batches;
    }

    /**
     * Get the deque for the given topic-partition, creating it if necessary. Since new topics will only be added rarely
     * we copy-on-write the hashmap
     */
    private Deque<RecordBatch> dequeFor(TopicPartition tp) {
        Deque<RecordBatch> d = this.batches.get(tp);
        if (d != null)
            return d;
        this.batches.putIfAbsent(tp, new ArrayDeque<RecordBatch>());
        return this.batches.get(tp);
    }

    /**
     * Deallocate the record batch
     */
    public void deallocate(RecordBatch batch) {
        free.deallocate(batch.records.buffer(), batch.records.capacity());
    }

    /**
     * Close this accumulator and force all the record buffers to be drained
     */
    public void close() {
        this.closed = true;
    }

}
