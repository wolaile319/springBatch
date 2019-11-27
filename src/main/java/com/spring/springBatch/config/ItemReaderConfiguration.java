package com.spring.springBatch.config;


import com.spring.springBatch.method.MyReader;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.List;

@Configuration
@EnableBatchProcessing
public class ItemReaderConfiguration {
    @Autowired
    private JobBuilderFactory jobBuilderFactory;
    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Bean
    public Step itemReaderStep(){
        return  stepBuilderFactory.get("itemReaderStep").<String,String>chunk(2).reader(myReaderMethod()).writer(
                list -> {
                    for (String iterm : list) {
                        System.out.println(iterm+".......");
                    }
                }
        ).build();
    }

    @Bean
    public Job itemReaderJob(){
        return  jobBuilderFactory.get("itemReaderJob").start(itemReaderStep()).build();
    }

    public MyReader myReaderMethod(){
       List<String> list = Arrays.asList("qinLing","zhangYang","zhangZhuo");
       return  new MyReader(list);
    }
}
