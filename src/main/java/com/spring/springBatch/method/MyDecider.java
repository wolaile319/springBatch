package com.spring.springBatch.method;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;

//自定义决策器
public class MyDecider implements JobExecutionDecider {

    private  int count;
    @Override
    public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {
        count ++;
        if(count % 2 ==0){
            System.out.println("我是偶数");
            return new FlowExecutionStatus("even");
        }else {
            System.out.println("我是鸡数");
            return new FlowExecutionStatus("odd");
        }
    }
}
