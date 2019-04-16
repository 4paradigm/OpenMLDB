package com._4paradigm.dataimporter.initialization;

import com.sun.istack.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class InitThreadPool {
    private static Logger logger = LoggerFactory.getLogger(InitThreadPool.class);
    private static final int MAXIMUMPOOLSIZE = Constant.MAXIMUMPOOLSIZE;
    private static final int BLOCKINGQUEUESIZE = Constant.BLOCKINGQUEUESIZE;
    private static BlockingQueue<Runnable> limitArray;
    private static ThreadFactory threadFactory;
    private static RejectedExecutionHandler rejectedExecutionHandler = new ThreadPoolExecutor.CallerRunsPolicy();
    private static ThreadPoolExecutor executor = null;

    /**
     * 初始化线程池
     */
    public static void initThreadPool() {
//        limitArray = new ArrayBlockingQueue<>(BLOCKINGQUEUESIZE);
        limitArray = new LinkedBlockingQueue<>(BLOCKINGQUEUESIZE);
        //设置线程池中新建线程的属性
        threadFactory = new ThreadFactory() {
            AtomicLong mAtomicLong = new AtomicLong(0);

            @Override
            public Thread newThread(@NotNull Runnable r) {
                Thread thread = new Thread(r, "My-Thread-" + mAtomicLong.getAndIncrement());
                // thread.setDaemon(true);
                // 创建新线程的时候打印新线程名字
                logger.info("Create new Thread(): " + thread.getName());
                return thread;
            }
        };
        //自定义饱和策略：实现接口RejectedExecutionHandler
//        RejectedExecutionHandler handler = new RejectedExecutionHandler() {
//            @Override
//            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
//                // 打印被拒绝的任务
//                logger.info("rejectedExecution:" + r.toString());
//            }
//        };
        //创建线程池
        executor = new ThreadPoolExecutor(MAXIMUMPOOLSIZE, MAXIMUMPOOLSIZE,
                30, TimeUnit.SECONDS, limitArray, threadFactory, rejectedExecutionHandler);

    }

    public static ThreadPoolExecutor getExecutor() {
        return executor;
    }
}
