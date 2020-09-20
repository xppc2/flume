package com.atguigu.flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TypeInterceptor implements Interceptor {

    private List<Event> addHeaderEvents;

    @Override
    public void initialize() {
        addHeaderEvents = new ArrayList<>();

    }

    //单个事件拦截
    @Override
    public Event intercept(Event event) {
        //获取事件的头信息
        Map<String, String> headers = event.getHeaders();

        //获取事件的body
        String body = new String(event.getBody());

        if (body.contains("hello")){
            //添加头信息
            headers.put("type","atguigu");
        }else {
            headers.put("type","bigdata");
        }

        return event;
    }

    //批量事件的拦截
    @Override
    public List<Event> intercept(List<Event> events) {
        //清空集合
        addHeaderEvents.clear();

        //遍历events,给每个事件添加头信息
        for (Event event:events){
            addHeaderEvents.add(intercept(event));
        }
        //返回结果
        return addHeaderEvents;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new TypeInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
