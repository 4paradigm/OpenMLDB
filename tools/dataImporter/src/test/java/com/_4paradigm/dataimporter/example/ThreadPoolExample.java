package com._4paradigm.dataimporter.example;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class ThreadPoolExample {

    // 有界队列，最多能存放5个任务对象
    private static BlockingQueue<Runnable> limitArray = new ArrayBlockingQueue<>(5);
    private static BlockingQueue<Runnable> limitlist = new LinkedBlockingQueue<>(5);

    // 无界队列可以存放Integer.MAX_VALUE个任务
    private static BlockingQueue<Runnable> unlimitList = new LinkedBlockingQueue<>();
    private static ThreadFactory threadFactory = new ThreadFactory() {
        private AtomicLong mAtomicLong = new AtomicLong(0);

        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r, "My-Thread-" + mAtomicLong.getAndIncrement());
            // 创建新线程的时候打印新线程名字
            System.out.println("Create new Thread(): " + thread.getName());
            return thread;
        }
    };
    private static RejectedExecutionHandler handler = new RejectedExecutionHandler() {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            // 打印被拒绝的任务
            System.out.println("rejectedExecution" + r.toString());
        }
    };


    private static ExecutorService executorService = new ThreadPoolExecutor(3, 6,
            30, TimeUnit.SECONDS, limitArray, threadFactory, handler);

    public static void excuteThreadPool() {
        int size = 15; // 需要执行的任务数量
        for (int i = 0; i < size; i++) {
            final int index = i;
            executorService.execute(new Runnable() {
                //                private int successfulCount = index;
                @Override
                public void run() {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println(Thread.currentThread().getName() + " -> " + "Current Task Num: #" + index);
                }

                @Override
                public String toString() {
                    return "$classname{" + "successfulCount=" + index + '}';
                }
            });
        }
        System.out.println("11--------------------------------------1111");
//        executorService.shutdown();
//        try {
//            executorService.awaitTermination(10, TimeUnit.SECONDS);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
    }

    public static void main(String[] args) {
//        excuteThreadPool();
//        for(int i=0;i<100;i++){
//            System.out.println(new Random().nextInt(6));
//        }
//        int ncpus = Runtime.getRuntime().availableProcessors();
//        System.out.println(ncpus);
//        List<String> list = new ArrayList<String>();
//        list.add("1");
//        list.add("2");
//        for (String item : list) {
//            if ("2".equals(item)) {
//                list.remove(item);
//            }
//        }
//        System.out.println(list);
//        String string = "1,2  " ;
//        String[] arr = string.split(",");
//        System.out.println(arr[1].length());


        int a = 3;
        System.out.println(a =+ 4);
        System.out.println(a);
    }
}



