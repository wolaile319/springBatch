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
public class SecondJobConfigration {
    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Bean
    public Step secondJobStep1(){
        return  stepBuilderFactory.get("secondJobStep1").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("这是第二个job的第一步");
                return RepeatStatus.FINISHED;
            }
        }).build();
    }


    @Bean
    public Step secondJobStep2(){
        return  stepBuilderFactory.get("secondJobStep2").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("这是第二个job的第二步");
                return RepeatStatus.FINISHED;
            }
        }).build();
    }

    @Bean
    public Job secondJob(){
        return jobBuilderFactory.get("secondJob").start(secondJobStep1()).next(secondJobStep2()).build();
    }
}
