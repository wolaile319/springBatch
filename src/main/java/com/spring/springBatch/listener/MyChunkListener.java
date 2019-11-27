package com.spring.springBatch.listener;

import org.springframework.batch.core.annotation.AfterChunk;
import org.springframework.batch.core.annotation.BeforeChunk;

/**
 * chunk的监听器，属于step监听器
 */
public class MyChunkListener {

    /**
     * chunk的前置监听器
     */
    @BeforeChunk
    public  void beforeChunk(){
        System.out.println("这是chunk的前置的监听器");
    }

    /**
     * chunk的后置监听器
     */
    @AfterChunk
    public  void afterChunk(){
        System.out.println("这是chunk的后置的监听器");
    }
}
