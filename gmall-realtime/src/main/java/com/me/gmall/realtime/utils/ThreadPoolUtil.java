package com.me.gmall.realtime.utils;


import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Author: zs
 * Date: 2021/5/17
 * Desc: 线程池工具类
 */
public class ThreadPoolUtil {
    // 单例 懒汉式--线程不安全，需要加锁 http://c.biancheng.net/view/1338.html
    private static ThreadPoolExecutor pool ;
    /*
        TODO 获取单例的线程池对象
        corePoolSize:指定了线程池中的线程数量，它的数量决定了添加的任务是开辟新的线程去执行，还是放到workQueue任务队列中去；
        maximumPoolSize:指定了线程池中的最大线程数量，这个参数会根据你使用的workQueue任务队列的类型，决定线程池会开辟的最大线程数量；
        keepAliveTime:当线程池中空闲线程数量超过corePoolSize时，多余的线程会在多长时间内被销毁；
        unit:keepAliveTime的单位
        workQueue:任务队列，被添加到线程池中，但尚未被执行的任务

        TODO 懒汉式线程不安全体现在：如果现在有2个线程A、B，那么A和B会轮流获取CPU的时间片，加入A获取到CPU时间片了，判断if(pool == null)
                如果这个时候还没有创建对象，那么A会创建一个线程池。如果现在A还每创建线程池，B获取到了时间片，B也要执行if(pool == null)
                这个时候A还没有创建线程池，那么B也要创建线程池，这样的话A和B都要创建线程池，这么就不合适了。
                这样就需要解决线程安全的问题，需要加锁。如果 synchronized 加在方法的前面，效率不好，就像 StringBuffer，每一个方法上都加了 synchronized
                好处是线程安全了，但是效率低，就好像买了一个自行车，自行车加了100把锁。
                如果现在加在了代码块上，加在最外面不合适，和加在类上是一样的
                public static ThreadPoolExecutor getInstance(){
                    synchronized (ThreadPoolUtil.class){
                        if(pool == null){
                            ....
                        }
                    }
                }
                如果pool对象不等于null就不要考虑线程安全的问题了，这样就先判断是否为null的情况，
                public static ThreadPoolExecutor getInstance(){
                    if(pool == null){
                        synchronized (ThreadPoolUtil.class){
                            。。。。
                        }
                    }
                }
                但是这样还是存在线程安全的问题。比如说A线程进来了，走到了 synchronized，但是此时B也进来了也走到了 synchronized，
                这样A执行完了B还要执行，只不过A执行下面的代码的时候B不会插进来，但是依然会创建2个对象，这样就需要再次判断null，就想下面写的方式
                双重判断保证线程安全
    */

    // 其实工具类使用了懒汉式加载都需要线程安全
    public static ThreadPoolExecutor getInstance(){
        if(pool == null){
            synchronized (ThreadPoolUtil.class){
                if(pool == null) {
                    System.out.println("---开辟线程池---");
                    pool = new ThreadPoolExecutor(4, 20, 300, TimeUnit.SECONDS,
                            new LinkedBlockingDeque<Runnable>(Integer.MAX_VALUE));
                }
            }
        }
        return pool;
    }
}

