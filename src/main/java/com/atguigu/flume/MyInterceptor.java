package com.atguigu.flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;
import java.util.Map;


public class MyInterceptor implements Interceptor {
    //初始化的方法
    public void initialize() {

    }

    //拦截的方法，处理一个event
    public Event intercept(Event event) {
        //获取header
        Map<String, String> headers = event.getHeaders();
        //获取body
        String body = new String(event.getBody());
        //判断body中是否包含atguigu
        if(body.contains("atguigu")){
            //在header中添加kv
            headers.put("flag","atguigu");
        }else{
            //不包含atguigu的event
            headers.put("flag","other");
        }
        return event;
    }

    //拦截的方法，处理批量的event
    public List<Event> intercept(List<Event> list) {
        for (Event event : list) {
            intercept(event);
        }
        return list;
    }

    //关闭资源的方法
    public void close() {

    }

    public static class MyBuilder implements Builder{

        public Interceptor build() {
            return new MyInterceptor();
        }

        //获取flume中配置文件中的属性
        public void configure(Context context) {

        }
    }
}
