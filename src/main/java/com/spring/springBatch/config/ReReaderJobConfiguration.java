package com.spring.springBatch.config;

import com.spring.springBatch.domain.User;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableBatchProcessing
public class ReReaderJobConfiguration {
    @Autowired
    private JobBuilderFactory jobBuilderFactory;
    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    @Autowired
    @Qualifier("reReader")
    private ItemReader<? extends User> reReader;
    @Qualifier("itemWriterFile")
    @Autowired
    private ItemWriter<? super User> reWriter;

    @Bean
    public Job reReaderJob(){
        return jobBuilderFactory.get("reReaderJob").start(reReaderStep()).build();
    }

    @Bean
    public Step reReaderStep() {
        return stepBuilderFactory.get("reReaderStep").<User,User>chunk(2).reader(reReader).writer(reWriter).build();
    }
}
