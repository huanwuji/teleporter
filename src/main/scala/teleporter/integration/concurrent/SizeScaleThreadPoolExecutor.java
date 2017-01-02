package teleporter.integration.concurrent;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by huanwuji
 * date 2016/12/15.
 */
public class SizeScaleThreadPoolExecutor extends ThreadPoolExecutor {
    private int threshold;
    private int initialPoolSize;
    private AtomicBoolean changed = new AtomicBoolean(false);

    public SizeScaleThreadPoolExecutor(int corePoolSize, int maximumPoolSize, int threshold, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
        this(corePoolSize, maximumPoolSize, threshold, keepAliveTime, unit, workQueue, threadFactory, new AbortPolicy());
    }

    public SizeScaleThreadPoolExecutor(int corePoolSize, int maximumPoolSize, int threshold, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, RejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
        this.initialPoolSize = corePoolSize;
        this.threshold = threshold;
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        int queueSize = this.getQueue().size();
        if (queueSize == 0 && this.getCorePoolSize() > this.initialPoolSize) {
            changeCorePoolSize(this.initialPoolSize);
        }
        int delta = queueSize / threshold - this.getCorePoolSize();
        if (delta > 0 && (delta + this.getCorePoolSize()) < this.getMaximumPoolSize()) {
            changeCorePoolSize(this.getCorePoolSize() + delta);
        }
    }

    private void changeCorePoolSize(int poolSize) {
        if (changed.compareAndSet(false, true)) {
            this.setCorePoolSize(poolSize);
            changed.set(false);
        }
    }
}