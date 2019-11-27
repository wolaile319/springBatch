package com.spring.springBatch.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.JobStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

@EnableBatchProcessing
@Configuration
public class ParentJobConfiguration {
    @Autowired
    private JobBuilderFactory jobBuilderFactory;
    @Autowired
    private Job firstJob;
    @Autowired
    private Job secondJob;
    @Autowired
    private JobLauncher launcher;

    @Bean
    public Job parentJob(JobRepository repository, PlatformTransactionManager transactionManager){
        return jobBuilderFactory.get("parentJob").start(step1(repository,transactionManager)).next(step2(repository,transactionManager)).build();
    }

    //返回job类型的step；是一种特殊的step
    private Step step1(JobRepository repository, PlatformTransactionManager transactionManager){
        return new JobStepBuilder(new StepBuilder("step1")).job(firstJob)
                .launcher(launcher)//使用启动父job的启动对象
                .repository(repository).transactionManager(transactionManager).build();
    }

    private Step step2(JobRepository repository, PlatformTransactionManager transactionManager){
        return new JobStepBuilder(new StepBuilder("step2")).job(secondJob)
                .launcher(launcher)//使用启动父job的启动对象
                .repository(repository).transactionManager(transactionManager).build();
    }
}
