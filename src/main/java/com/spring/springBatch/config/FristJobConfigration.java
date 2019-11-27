package com.spring.springBatch.config;

import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Configuration
@EnableBatchProcessing
public class FristJobConfigration implements StepExecutionListener {
    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    private  Map<String, JobParameter> map;

    @Bean
    public Step fristJobStep1(){
        return  stepBuilderFactory.get("fristJobStep1").listener(this).tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("=========参数为："+map.get("msg").getValue());
                System.out.println("这是第一个job的第一步");
                return RepeatStatus.FINISHED;
            }
        }).build();
    }


    @Bean
    public Step fristJobStep2(){
        return  stepBuilderFactory.get("fristJobStep2").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("这是第一个job的第二步");
                return RepeatStatus.FINISHED;
            }
        }).build();
    }

    @Bean
    public Job firstJob(){
        return jobBuilderFactory.get("firstJob").start(fristJobStep1()).next(fristJobStep2()).build();
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
      map = stepExecution.getJobParameters().getParameters();
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        return null;
    }
}
