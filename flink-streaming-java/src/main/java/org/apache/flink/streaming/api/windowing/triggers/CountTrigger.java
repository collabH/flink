/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.windowing.triggers;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * A {@link Trigger} that fires once the count of elements in a pane reaches the given count.
 *
 * @param <W> The type of {@link Window Windows} on which this trigger can operate.
 */
@PublicEvolving
public class CountTrigger<W extends Window> extends Trigger<Object, W> {
	private static final long serialVersionUID = 1L;

	// 最大触发条数
	private final long maxCount;

	// 创建Reducing状态，传入Sum reduce函数，计算放入stateDesc的数据之和
	private final ReducingStateDescriptor<Long> stateDesc =
			new ReducingStateDescriptor<>("count", new Sum(), LongSerializer.INSTANCE);

	private CountTrigger(long maxCount) {
		this.maxCount = maxCount;
	}

	@Override
	public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx) throws Exception {
		// 获取ReducingState状态
		ReducingState<Long> count = ctx.getPartitionedState(stateDesc);
		// 添加1个元素
		count.add(1L);
		// 如果窗口中以及满足count数，则清空状态并且输出窗口结果
		if (count.get() >= maxCount) {
			count.clear();
			return TriggerResult.FIRE;
		}
		return TriggerResult.CONTINUE;
	}

	@Override
	public TriggerResult onEventTime(long time, W window, TriggerContext ctx) {
		return TriggerResult.CONTINUE;
	}

	@Override
	public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
		return TriggerResult.CONTINUE;
	}

	@Override
	public void clear(W window, TriggerContext ctx) throws Exception {
		ctx.getPartitionedState(stateDesc).clear();
	}

	@Override
	public boolean canMerge() {
		return true;
	}

	@Override
	public void onMerge(W window, OnMergeContext ctx) throws Exception {
		ctx.mergePartitionedState(stateDesc);
	}

	@Override
	public String toString() {
		return "CountTrigger(" +  maxCount + ")";
	}

	/**
	 * Creates a trigger that fires once the number of elements in a pane reaches the given count.
	 *
	 * @param maxCount The count of elements at which to fire.
	 * @param <W> The type of {@link Window Windows} on which this trigger can operate.
	 */
	public static <W extends Window> CountTrigger<W> of(long maxCount) {
		return new CountTrigger<>(maxCount);
	}

	private static class Sum implements ReduceFunction<Long> {
		private static final long serialVersionUID = 1L;

		@Override
		public Long reduce(Long value1, Long value2) throws Exception {
			return value1 + value2;
		}

	}
}
