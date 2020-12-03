/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.windowing.assigners;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.concurrent.ThreadLocalRandom;

/**
 * A {@code WindowStagger} staggers offset in runtime for each window assignment.
 */
@PublicEvolving
public enum WindowStagger {
	/**
	 * 默认模式下，所有窗格同时在所有分区上触发。
	 * Default mode,  all panes fire at the same time across all partitions.
	 */
	ALIGNED {
		@Override
		public long getStaggerOffset(
				final long currentProcessingTime,
				final long size) {
			return 0L;
		}
	},

	/**
	 * 当在分区运算符中提取第一个事件时，从均匀分布U（0，WindowSize）中采样随机偏移
	 * Stagger offset is sampled from uniform distribution U(0, WindowSize) when first event
	 * ingested in the partitioned operator.
	 */
	RANDOM {
		@Override
		public long getStaggerOffset(
				final long currentProcessingTime,
				final long size) {
			return (long) (ThreadLocalRandom.current().nextDouble() * size);
		}
	},

	/**
	 * 在窗口运算符中接收到第一个事件时，以窗口开始与当前处理时间之间的差作为偏移量。 这样，根据每个并行运算符何时接收到第一个事件来交错窗口。
	 * When the first event is received in the window operator, take the difference between the
	 * start of the window and current procesing time as the offset. This way, windows are staggered
	 * based on when each parallel operator receives the first event.
	 */
	NATURAL {
		@Override
		public long getStaggerOffset(
				final long currentProcessingTime,
				final long size) {
			final long currentProcessingWindowStart = TimeWindow.getWindowStartWithOffset(
					currentProcessingTime,
					0,
					size);
			return Math.max(0, currentProcessingTime - currentProcessingWindowStart);
		}
	};

	public abstract long getStaggerOffset(final long currentProcessingTime, final long size);
}
