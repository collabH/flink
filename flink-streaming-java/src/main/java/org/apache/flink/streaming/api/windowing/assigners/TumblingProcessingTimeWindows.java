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

package org.apache.flink.streaming.api.windowing.assigners;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Collection;
import java.util.Collections;

/**
 * 一个WindowAssigner ，用于根据运行该操作的计算机的当前系统时间将元素窗口化为窗口。 Windows不能重叠。
 * 例如，为了每10秒进入1分钟的窗口
 * A {@link WindowAssigner} that windows elements into windows based on the current
 * system time of the machine the operation is running on. Windows cannot overlap.
 *
 * <p>For example, in order to window into windows of 1 minute, every 10 seconds:
 * <pre> {@code
 * DataStream<Tuple2<String, Integer>> in = ...;
 * KeyedStream<String, Tuple2<String, Integer>> keyed = in.keyBy(...);
 * WindowedStream<Tuple2<String, Integer>, String, TimeWindows> windowed =
 *   keyed.window(TumblingProcessingTimeWindows.of(Time.of(1, MINUTES), Time.of(10, SECONDS));
 * } </pre>
 */
public class TumblingProcessingTimeWindows extends WindowAssigner<Object, TimeWindow> {
	private static final long serialVersionUID = 1L;

	// 窗口大小
	private final long size;

	// 全局offset
	private final long globalOffset;

	// 错开窗口offset
	private Long staggerOffset = null;

	// 错开窗口策略
	private final WindowStagger windowStagger;

	/**
	 *
	 * @param size 窗口大小
	 * @param offset 全局offset
	 * @param windowStagger 窗口错开策略
	 */
	private TumblingProcessingTimeWindows(long size, long offset, WindowStagger windowStagger) {
		if (Math.abs(offset) >= size) {
			throw new IllegalArgumentException("TumblingProcessingTimeWindows parameters must satisfy abs(offset) < size");
		}

		this.size = size;
		this.globalOffset = offset;
		this.windowStagger = windowStagger;
	}

	/**
	 * 指定窗口元素到Collection中
	 * @param element The element to which windows should be assigned.
	 * @param timestamp The timestamp of the element.
	 * @param context The {@link WindowAssignerContext} in which the assigner operates.
	 * @return
	 */
	@Override
	public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
		// 获取当前processTime
		final long now = context.getCurrentProcessingTime();
		if (staggerOffset == null) {
			// 获取错开窗口offset
			staggerOffset = windowStagger.getStaggerOffset(context.getCurrentProcessingTime(), size);
		}
		// 计算开始窗口起始时间
		long start = TimeWindow.getWindowStartWithOffset(now, (globalOffset + staggerOffset) % size, size);
		// 塞入窗口集合
		return Collections.singletonList(new TimeWindow(start, start + size));
	}

	public long getSize() {
		return size;
	}

	@Override
	public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
		// 创建默认trigger
		return ProcessingTimeTrigger.create();
	}

	@Override
	public String toString() {
		return "TumblingProcessingTimeWindows(" + size + ")";
	}

	/**
	 * Creates a new {@code TumblingProcessingTimeWindows} {@link WindowAssigner} that assigns
	 * elements to time windows based on the element timestamp.
	 *
	 * @param size The size of the generated windows.
	 * @return The time policy.
	 */
	public static TumblingProcessingTimeWindows of(Time size) {
		// 创建滚动窗口，size为毫秒，offset设置为0，在全部分区上触发
		return new TumblingProcessingTimeWindows(size.toMilliseconds(), 0, WindowStagger.ALIGNED);
	}

	/**
	 * 创建一个新的TumblingProcessingTimeWindows WindowAssigner ，根据元素的时间戳和偏移量将元素分配给时间窗口。
	 * 例如，如果of(Time.hours(1),Time.minutes(15))小时显示流，但窗口从每小时的第15分钟开始，则可以使用of(Time.hours(1),Time.minutes(15)) ，然后将获得时间窗口开始在0：15：00、1：15：00、2：15：00等
	 * 而不是那样，如果您居住在不使用UTC±00：00时间的地方，例如使用UTC + 08：00的中国，并且您想要一个大小为一天的时间窗口，并且该窗口在每个时间开始当地时间00:00:00，您可以使用of(Time.days(1),Time.hours(-8)) 。 由于UTC + 08：00比UTC时间早8小时，所以offset参数为Time.hours(-8))
	 *
	 * Creates a new {@code TumblingProcessingTimeWindows} {@link WindowAssigner} that assigns
	 * elements to time windows based on the element timestamp and offset.
	 *
	 * <p>For example, if you want window a stream by hour,but window begins at the 15th minutes
	 * of each hour, you can use {@code of(Time.hours(1),Time.minutes(15))},then you will get
	 * time windows start at 0:15:00,1:15:00,2:15:00,etc.
	 *
	 * <p>Rather than that,if you are living in somewhere which is not using UTC±00:00 time,
	 * such as China which is using UTC+08:00,and you want a time window with size of one day,
	 * and window begins at every 00:00:00 of local time,you may use {@code of(Time.days(1),Time.hours(-8))}.
	 * The parameter of offset is {@code Time.hours(-8))} since UTC+08:00 is 8 hours earlier than UTC time.
	 *
	 * @param size The size of the generated windows.
	 * @param offset The offset which window start would be shifted by.
	 * @return The time policy.
	 */
	public static TumblingProcessingTimeWindows of(Time size, Time offset) {
		return new TumblingProcessingTimeWindows(size.toMilliseconds(), offset.toMilliseconds(), WindowStagger.ALIGNED);
	}

	/**
	 * Creates a new {@code TumblingProcessingTimeWindows} {@link WindowAssigner} that assigns
	 * elements to time windows based on the element timestamp, offset and a staggering offset,
	 * depending on the staggering policy.
	 *
	 * @param size The size of the generated windows.
	 * @param offset The offset which window start would be shifted by.
	 * @param windowStagger The utility that produces staggering offset in runtime.
	 *
	 * @return The time policy.
	 */
	@PublicEvolving
	public static TumblingProcessingTimeWindows of(Time size, Time offset, WindowStagger windowStagger) {
		return new TumblingProcessingTimeWindows(size.toMilliseconds(), offset.toMilliseconds(), windowStagger);
	}

	@Override
	public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
		return new TimeWindow.Serializer();
	}

	@Override
	public boolean isEventTime() {
		return false;
	}
}
