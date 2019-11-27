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
public class ParameterConfiguration implements StepExecutionListener {
    @Autowired
    private JobBuilderFactory jobBuilderFactory;
    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    private Map<String,JobParameter> jobParameters;
    @Bean
    public Step parameterSte(){
        return stepBuilderFactory.get("parameterStep").listener(this).tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println(jobParameters.get("info"));
                return RepeatStatus.FINISHED;
            }
        }).build();

    }
    @Bean
    public Job parameterJob(){
        return jobBuilderFactory.get("parameterJob").start(parameterSte()).build();
    }


    @Override
    public void beforeStep(StepExecution stepExecution) {
        jobParameters = stepExecution.getJobParameters().getParameters();
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        return null;
    }
}
