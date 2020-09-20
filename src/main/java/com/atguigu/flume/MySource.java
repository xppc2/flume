package com.atguigu.flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

import java.util.UUID;

public class MySource extends AbstractSource implements Configurable, PollableSource {

    private String prefix;

    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;

        try {
            //采集数据封装Event对象
            Event e = getSomeData();

            // 获取Channel的处理器
            ChannelProcessor channelProcessor = getChannelProcessor();
            //处理event
            channelProcessor.processEvent(e);

            status = Status.READY;
        } catch (Throwable t) {
            // Log exception, handle individual exceptions as needed

            status = Status.BACKOFF;

            // re-throw all Errors
            if (t instanceof Error) {
                throw (Error)t;
            }
        }

        return status;
    }

    //采集数据封装Event对象
    private Event getSomeData() throws InterruptedException {
        //睡一会儿
        Thread.sleep(2000);
        //使用UUID生成字符串
        String uuid = UUID.randomUUID().toString();
        //创建Event对象
        SimpleEvent simpleEvent = new SimpleEvent();
        //设置header
        simpleEvent.getHeaders().put("my","source");
        //设置body
        simpleEvent.setBody((prefix+uuid).getBytes());
        return simpleEvent;

    }

    @Override
    public long getBackOffSleepIncrement() {
        return 1000;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 10000;
    }

    @Override
    public void configure(Context context) {
        prefix = context.getString("prefix", "mySource-");
    }
}
