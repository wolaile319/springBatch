package com.spring.springBatch.listener;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;

/**
 * job监听器
 */
public class MyListener implements JobExecutionListener {
    /**
     * 前置监听
     * @param jobExecution
     */
    @Override
    public void beforeJob(JobExecution jobExecution) {
        System.out.println("这是job的前置监听器");
    }

    /**
     * 后置监听
     * @param jobExecution
     */
    @Override
    public void afterJob(JobExecution jobExecution) {
        System.out.println("这是job的后置监听器");
    }
}
