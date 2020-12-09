/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.filesystem.stream;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.filesystem.PartitionTimeExtractor;
import org.apache.flink.util.StringUtils;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.flink.table.filesystem.DefaultPartTimeExtractor.toMills;
import static org.apache.flink.table.filesystem.FileSystemOptions.PARTITION_TIME_EXTRACTOR_CLASS;
import static org.apache.flink.table.filesystem.FileSystemOptions.PARTITION_TIME_EXTRACTOR_KIND;
import static org.apache.flink.table.filesystem.FileSystemOptions.PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN;
import static org.apache.flink.table.filesystem.FileSystemOptions.SINK_PARTITION_COMMIT_DELAY;
import static org.apache.flink.table.utils.PartitionPathUtils.extractPartitionValues;

/**
 * Partition commit trigger by partition time and watermark,
 * if 'watermark' > 'partition-time' + 'delay', will commit the partition.
 *
 * <p>Compares watermark, and watermark is related to records and checkpoint, so we need store
 * watermark information for checkpoint.
 */
public class PartitionTimeCommitTigger implements PartitionCommitTrigger {
	// 记录等待提交的分区
	private static final ListStateDescriptor<List<String>> PENDING_PARTITIONS_STATE_DESC =
			new ListStateDescriptor<>(
					"pending-partitions",
					new ListSerializer<>(StringSerializer.INSTANCE));
	// 记录checkpointid对应的watermark
	private static final ListStateDescriptor<Map<Long, Long>> WATERMARKS_STATE_DESC =
			new ListStateDescriptor<>(
					"checkpoint-id-to-watermark",
					new MapSerializer<>(LongSerializer.INSTANCE, LongSerializer.INSTANCE));

	private final ListState<List<String>> pendingPartitionsState;
	// 等待中的分区
	private final Set<String> pendingPartitions;

	// watermarker状态，mapkey为checkpointid，value为watermark
	private final ListState<Map<Long, Long>> watermarksState;
	// 记录watermark
	private final TreeMap<Long, Long> watermarks;
	// 分区时间提取器
	private final PartitionTimeExtractor extractor;
	// 提交延迟时间
	private final long commitDelay;
	// 分区key
	private final List<String> partitionKeys;

	public PartitionTimeCommitTigger(
			boolean isRestored,
			OperatorStateStore stateStore,
			Configuration conf,
			ClassLoader cl,
			List<String> partitionKeys) throws Exception {
		// 初始化等待提交分区状态
		this.pendingPartitionsState = stateStore.getListState(PENDING_PARTITIONS_STATE_DESC);
		this.pendingPartitions = new HashSet<>();
		// 是否为恢复状态
		if (isRestored) {
			// 将从状态后端中获取的带提交分区放入内存中
			pendingPartitions.addAll(pendingPartitionsState.get().iterator().next());
		}

		this.partitionKeys = partitionKeys;
		// 获取"sink.partition-commit.delay"延迟，默认为0ms
		this.commitDelay = conf.get(SINK_PARTITION_COMMIT_DELAY).toMillis();
		// 根据指定配置实例化出分区时间提交器
		this.extractor = PartitionTimeExtractor.create(
				cl,
				// 指定分区时间提交器类型，默认为 default，可以选择custom
				conf.get(PARTITION_TIME_EXTRACTOR_KIND),
				// 指定自定义的分区时间提取器
				conf.get(PARTITION_TIME_EXTRACTOR_CLASS),
				// 提取时间匹配的格式
				conf.get(PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN));

		// 获取watermark状态
		this.watermarksState = stateStore.getListState(WATERMARKS_STATE_DESC);
		this.watermarks = new TreeMap<>();
		if (isRestored) {
			// 恢复状态
			watermarks.putAll(watermarksState.get().iterator().next());
		}
	}

	@Override
	public void addPartition(String partition) {
		if (!StringUtils.isNullOrWhitespaceOnly(partition)) {
			this.pendingPartitions.add(partition);
		}
	}

	@Override
	public List<String> committablePartitions(long checkpointId) {
		// 判断提交的分区是否一件checkpoint完毕
		if (!watermarks.containsKey(checkpointId)) {
			throw new IllegalArgumentException(String.format(
					"Checkpoint(%d) has not been snapshot. The watermark information is: %s.",
					checkpointId, watermarks));
		}
		// 获取其watermarker
		long watermark = watermarks.get(checkpointId);

		watermarks.headMap(checkpointId, true).clear();

		List<String> needCommit = new ArrayList<>();
		Iterator<String> iter = pendingPartitions.iterator();
		while (iter.hasNext()) {
			String partition = iter.next();
			// 从指定分区key中提取分区时间
			LocalDateTime partTime = extractor.extract(
					partitionKeys, extractPartitionValues(new Path(partition)));
			// 如果watermarker大于分区时间+延迟时间，则可以提交，提交后移除
			if (watermark > toMills(partTime) + commitDelay) {
				needCommit.add(partition);
				iter.remove();
			}
		}
		return needCommit;
	}

	/**
	 * 快照状态
	 * @param checkpointId
	 * @param watermark
	 * @throws Exception
	 */
	@Override
	public void snapshotState(long checkpointId, long watermark) throws Exception {
		pendingPartitionsState.clear();
		// 将内存中数据加入state
		pendingPartitionsState.add(new ArrayList<>(pendingPartitions));

		watermarks.put(checkpointId, watermark);
		watermarksState.clear();
		watermarksState.add(new HashMap<>(watermarks));
	}

	@Override
	public List<String> endInput() {
		ArrayList<String> partitions = new ArrayList<>(pendingPartitions);
		pendingPartitions.clear();
		return partitions;
	}
}
