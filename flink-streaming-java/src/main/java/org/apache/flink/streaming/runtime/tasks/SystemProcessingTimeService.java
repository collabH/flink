/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.util.concurrent.NeverCompleteFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link TimerService} which assigns as current processing time the result of calling
 * {@link System#currentTimeMillis()} and registers timers using a {@link ScheduledThreadPoolExecutor}.
 */
@Internal
public class SystemProcessingTimeService implements TimerService {

	private static final Logger LOG = LoggerFactory.getLogger(SystemProcessingTimeService.class);
	/**
	 * 定义TimeService状态
	 */
	private static final int STATUS_ALIVE = 0;
	private static final int STATUS_QUIESCED = 1;
	private static final int STATUS_SHUTDOWN = 2;

	// ------------------------------------------------------------------------

	/** The executor service that schedules and calls the triggers of this task. */
	// 调度线程池，线程数为1
	private final ScheduledThreadPoolExecutor timerService;
	// 异常处理器
	private final ExceptionHandler exceptionHandler;
	// timeService状态
	private final AtomicInteger status;
	// 未完成的定时器
	private final CompletableFuture<Void> quiesceCompletedFuture;

	@VisibleForTesting
	SystemProcessingTimeService(ExceptionHandler exceptionHandler) {
		this(exceptionHandler, null);
	}

	SystemProcessingTimeService(ExceptionHandler exceptionHandler, ThreadFactory threadFactory) {

		this.exceptionHandler = checkNotNull(exceptionHandler);
		// 默认状态 alive
		this.status = new AtomicInteger(STATUS_ALIVE);
		this.quiesceCompletedFuture = new CompletableFuture<>();

		if (threadFactory == null) {
			this.timerService = new ScheduledTaskExecutor(1);
		} else {
			this.timerService = new ScheduledTaskExecutor(1, threadFactory);
		}

		// tasks should be removed if the future is canceled，如果futrue被取消，task应该被移除
		this.timerService.setRemoveOnCancelPolicy(true);

		// make sure shutdown removes all pending tasks 确保关机删除所有未完成的任务
		// timeService关闭后，任务被终止和移除，相当于shutdownNow
		this.timerService.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
		// 设置为true，标示关闭后执行，false标示不执行
		this.timerService.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
	}

	@Override
	public long getCurrentProcessingTime() {
		// 系统时间
		return System.currentTimeMillis();
	}

	/**
	 * Registers a task to be executed no sooner than time {@code timestamp}, but without strong
	 * guarantees of order.
	 *
	 * @param timestamp Time when the task is to be enabled (in processing time)
	 * @param callback    The task to be executed
	 * @return The future that represents the scheduled task. This always returns some future,
	 *         even if the timer was shut down
	 */
	@Override
	public ScheduledFuture<?> registerTimer(long timestamp, ProcessingTimeCallback callback) {

		// 计算timestamp和getCurrentProcessingTime的延迟，在timestamp和当前时间的差值上再延迟1ms，为了watermark的判断，防止出现边界清空，小于watermakr的数据都会被丢弃
		long delay = ProcessingTimeServiceUtil.getProcessingTimeDelay(timestamp, getCurrentProcessingTime());

		// we directly try to register the timer and only react to the status on exception
		// that way we save unnecessary volatile accesses for each timer
		try {
			// 在delay ms后执行wrapOnTimerCallback
			return timerService.schedule(wrapOnTimerCallback(callback, timestamp), delay, TimeUnit.MILLISECONDS);
		}
		catch (RejectedExecutionException e) {
			final int status = this.status.get();
			// 停止状态，没有timer
			if (status == STATUS_QUIESCED) {
				return new NeverCompleteFuture(delay);
			}
			else if (status == STATUS_SHUTDOWN) {
				throw new IllegalStateException("Timer service is shut down");
			}
			else {
				// something else happened, so propagate the exception
				throw e;
			}
		}
	}

	@Override
	public ScheduledFuture<?> scheduleAtFixedRate(ProcessingTimeCallback callback, long initialDelay, long period) {
		return scheduleRepeatedly(callback, initialDelay, period, false);
	}

	@Override
	public ScheduledFuture<?> scheduleWithFixedDelay(ProcessingTimeCallback callback, long initialDelay, long period) {
		return scheduleRepeatedly(callback, initialDelay, period, true);
	}

	private ScheduledFuture<?> scheduleRepeatedly(ProcessingTimeCallback callback, long initialDelay, long period, boolean fixedDelay) {
		// 下次执行的时间
		final long nextTimestamp = getCurrentProcessingTime() + initialDelay;
		// 获取调度任务
		final Runnable task = wrapOnTimerCallback(callback, nextTimestamp, period);

		// we directly try to register the timer and only react to the status on exception
		// that way we save unnecessary volatile accesses for each timer
		try {
			return fixedDelay
					? timerService.scheduleWithFixedDelay(task, initialDelay, period, TimeUnit.MILLISECONDS)
					: timerService.scheduleAtFixedRate(task, initialDelay, period, TimeUnit.MILLISECONDS);
		} catch (RejectedExecutionException e) {
			final int status = this.status.get();
			if (status == STATUS_QUIESCED) {
				return new NeverCompleteFuture(initialDelay);
			}
			else if (status == STATUS_SHUTDOWN) {
				throw new IllegalStateException("Timer service is shut down");
			}
			else {
				// something else happened, so propagate the exception
				throw e;
			}
		}
	}

	/**
	 * @return {@code true} is the status of the service
	 * is {@link #STATUS_ALIVE}, {@code false} otherwise.
	 */
	@VisibleForTesting
	boolean isAlive() {
		return status.get() == STATUS_ALIVE;
	}

	@Override
	public boolean isTerminated() {
		return status.get() == STATUS_SHUTDOWN;
	}

	@Override
	public CompletableFuture<Void> quiesce() {
		if (status.compareAndSet(STATUS_ALIVE, STATUS_QUIESCED)) {
			timerService.shutdown();
		}

		return quiesceCompletedFuture;
	}

	@Override
	public void shutdownService() {
		if (status.compareAndSet(STATUS_ALIVE, STATUS_SHUTDOWN) ||
				status.compareAndSet(STATUS_QUIESCED, STATUS_SHUTDOWN)) {
			timerService.shutdownNow();
		}
	}

	/**
	 * Shuts down and clean up the timer service provider hard and immediately. This does wait
	 * for all timers to complete or until the time limit is exceeded. Any call to
	 * {@link #registerTimer(long, ProcessingTimeCallback)} will result in a hard exception after calling this method.
	 * @param time time to wait for termination.
	 * @param timeUnit time unit of parameter time.
	 * @return {@code true} if this timer service and all pending timers are terminated and
	 *         {@code false} if the timeout elapsed before this happened.
	 */
	@VisibleForTesting
	boolean shutdownAndAwaitPending(long time, TimeUnit timeUnit) throws InterruptedException {
		shutdownService();
		return timerService.awaitTermination(time, timeUnit);
	}

	@Override
	public boolean shutdownServiceUninterruptible(long timeoutMs) {

		final Deadline deadline = Deadline.fromNow(Duration.ofMillis(timeoutMs));

		boolean shutdownComplete = false;
		boolean receivedInterrupt = false;

		do {
			try {
				// wait for a reasonable time for all pending timer threads to finish
				shutdownComplete = shutdownAndAwaitPending(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS);
			} catch (InterruptedException iex) {
				// 强制终端
				receivedInterrupt = true;
				LOG.trace("Intercepted attempt to interrupt timer service shutdown.", iex);
			}
		} while (deadline.hasTimeLeft() && !shutdownComplete);

		if (receivedInterrupt) {
			Thread.currentThread().interrupt();
		}

		return shutdownComplete;
	}

	// safety net to destroy the thread pool
	// 垃圾回收的时候触发，强制关闭timerService
	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		timerService.shutdownNow();
	}

	@VisibleForTesting
	int getNumTasksScheduled() {
		// 获取调度任务个数
		BlockingQueue<?> queue = timerService.getQueue();
		if (queue == null) {
			return 0;
		} else {
			return queue.size();
		}
	}

	// ------------------------------------------------------------------------

	private class ScheduledTaskExecutor extends ScheduledThreadPoolExecutor {

		public ScheduledTaskExecutor(int corePoolSize) {
			super(corePoolSize);
		}

		public ScheduledTaskExecutor(int corePoolSize, ThreadFactory threadFactory) {
			super(corePoolSize, threadFactory);
		}

		@Override
		protected void terminated() {
			super.terminated();
			quiesceCompletedFuture.complete(null);
		}
	}

	/**
	 * An exception handler, called when {@link ProcessingTimeCallback} throws an exception.
	 */
	interface ExceptionHandler {
		void handleException(Exception ex);
	}

	/**
	 * 将ProcessingTimeCallback包装成Runnable
	 * @param callback
	 * @param timestamp
	 * @return
	 */
	private Runnable wrapOnTimerCallback(ProcessingTimeCallback callback, long timestamp) {
		return new ScheduledTask(status, exceptionHandler, callback, timestamp, 0);
	}

	private Runnable wrapOnTimerCallback(ProcessingTimeCallback callback, long nextTimestamp, long period) {
		return new ScheduledTask(status, exceptionHandler, callback, nextTimestamp, period);
	}

	/**
	 * Timer调度Task
	 */
	private static final class ScheduledTask implements Runnable {
		// 服务状态
		private final AtomicInteger serviceStatus;
		// 异常处理器
		private final ExceptionHandler exceptionHandler;
		// Processing回调函数
		private final ProcessingTimeCallback callback;
		// 下次触发的时间
		private long nextTimestamp;
		// 间隔的周期
		private final long period;

		ScheduledTask(
				AtomicInteger serviceStatus,
				ExceptionHandler exceptionHandler,
				ProcessingTimeCallback callback,
				long timestamp,
				long period) {
			this.serviceStatus = serviceStatus;
			this.exceptionHandler = exceptionHandler;
			this.callback = callback;
			this.nextTimestamp = timestamp;
			this.period = period;
		}

		@Override
		public void run() {
			// 判断服务状态
			if (serviceStatus.get() != STATUS_ALIVE) {
				return;
			}
			try {
				// 触发onProcessingTime
				callback.onProcessingTime(nextTimestamp);
			} catch (Exception ex) {
				exceptionHandler.handleException(ex);
			}
			// 周期调用，每隔period执行一次
			nextTimestamp += period;
		}
	}
}
