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

/**
 * 很多step要执行的方式
 */

@Configuration
@EnableBatchProcessing
public class JobsConfigration {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Bean
    public Job myJob(){
        return  jobBuilderFactory.get("myJob2")
                /*.start(step1()).on("COMPLETED").to(step2())
                .from(step2()).on("COMPLETED").to(step3())
                .from(step3()).end().build();*/
                .start(step1()).next(step2()).next(step3()).build();
    }

    @Bean
    public Step step1(){
        return  stepBuilderFactory.get("step1").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("第一步");
                return RepeatStatus.FINISHED;
            }
        }).build();
    }

    @Bean
    public Step step2(){
        return  stepBuilderFactory.get("step2").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("第二步");
                return RepeatStatus.FINISHED;
            }
        }).build();
    }
    @Bean
    public Step step3(){
        return  stepBuilderFactory.get("step3").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("第三步");
                return RepeatStatus.FINISHED;
            }
        }).build();
    }
}
