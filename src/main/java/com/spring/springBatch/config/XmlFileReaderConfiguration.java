package com.spring.springBatch.config;

import com.spring.springBatch.domain.User;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.oxm.xstream.XStreamMarshaller;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableBatchProcessing
public class XmlFileReaderConfiguration {
   @Autowired
   private JobBuilderFactory jobBuilderFactory;
   @Autowired
   private StepBuilderFactory stepBuilderFactory;
   @Qualifier("xmlWriterFile")
   @Autowired
   private ItemWriter xmlWriterFile;

   @Bean
   public Step xmlReaderFileStep(){
      return stepBuilderFactory.get("xmlReaderFileStep").<User,User>chunk(2).reader(xmlReader()).writer(xmlWriterFile).build();
   }
    @Bean
    @StepScope
    public StaxEventItemReader<User> xmlReader() {
        StaxEventItemReader<User> itemReader = new StaxEventItemReader<>();
        itemReader.setResource(new ClassPathResource("User.xml"));
        //指定需要处理的根标签
        itemReader.setFragmentRootElementName("user");
        //把xml转化为对象
        XStreamMarshaller marshaller = new XStreamMarshaller();
        Map<String,Class> map = new HashMap<>();
        map.put("user",User.class);
        marshaller.setAliases(map);
        itemReader.setUnmarshaller(marshaller);
        return itemReader;
    }

    @Bean
   public Job xmlReaderFileJob(){
       return  jobBuilderFactory.get("xmlReaderFileJob3").start(xmlReaderFileStep()).build();
    }



}
