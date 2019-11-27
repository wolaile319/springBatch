package com.spring.springBatch.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

@Configuration
@EnableBatchProcessing
public class SplitConfiguration {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;
    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Bean
    public Step splitStep1(){
        return  stepBuilderFactory.get("splitStep1").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("我是老大！");
                return RepeatStatus.FINISHED;
            }
        }).build();
    }


    @Bean
    public Step splitStep2(){
        return  stepBuilderFactory.get("splitStep2").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("我是老二！");
                return RepeatStatus.FINISHED;
            }
        }).build();
    }


    @Bean
    public Step splitStep3(){
        return  stepBuilderFactory.get("splitStep3").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("我是老三！");
                return RepeatStatus.FINISHED;
            }
        }).build();
    }

    @Bean
    public Flow splitFlow1(){
        return new FlowBuilder<Flow>("splitFlow1").start(splitStep1()).build();
    }

    @Bean
    public Flow splitFlow2(){
        return new FlowBuilder<Flow>("splitFlow2").start(splitStep2()).next(splitStep3()).build();
    }


    @Bean
    public Job splitJob(){
        return jobBuilderFactory.get("splitJob2").start(splitFlow2()).split(new SimpleAsyncTaskExecutor( )).add(splitFlow1()).end().build();
    }
}
