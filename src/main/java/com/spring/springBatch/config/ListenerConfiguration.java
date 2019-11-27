package com.spring.springBatch.config;

import com.spring.springBatch.listener.MyChunkListener;
import com.spring.springBatch.listener.MyListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.List;

@Configuration
@EnableBatchProcessing
public class ListenerConfiguration {
    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Bean
    public Step listenerStep1(){
        return  stepBuilderFactory.get("listenerStep1").<String,String>chunk(2)
                .faultTolerant()//容错的
                .listener(new MyChunkListener()).reader(reader()).writer(writer()).build();
    }

    @Bean
    public ItemReader<String> reader(){
        return new ListItemReader<>(Arrays.asList("java","spring","jquery"));
    }

    @Bean
    public ItemWriter<String> writer(){
        return new ItemWriter<String>() {
            @Override
            public void write(List<? extends String> list) throws Exception {
                for (String ss: list) {
                    System.out.println("写操作："+ ss);
                }
            }
        };
    }

    @Bean
    public Job listenerJob(){
        return  jobBuilderFactory.get("listenerJob").listener(new MyListener()).start(listenerStep1()).build();
    }


}
