package com.spring.springBatch.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableBatchProcessing
public class JobConfigration {
    //注入创建任务对象的对象
    @Autowired
    private JobBuilderFactory jobBuilderFactory;
    //任务的执行由step决定，创建step对象
    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    @Bean
    public Job helloWorldJob(){
      return  jobBuilderFactory.get("helloWorldJob").start(getStep()).build();
    }

    public Step getStep(){

        return stepBuilderFactory.get("getStep").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("我的第一个springBatch");
                return RepeatStatus.FINISHED;
            }
        }).build();
    }
}
