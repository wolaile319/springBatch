package com.spring.springBatch.itermwriterconfig;

import com.spring.springBatch.domain.User;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.validation.BindException;

/**
 *  把文件写入数据库中
 */
@Configuration
@EnableBatchProcessing
public class ItermWriterDB {
    @Autowired
    private JobBuilderFactory jobBuilderFactory;
    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Qualifier("ItermFileWriterDb")
    @Autowired
    private ItemWriter itermFileWriterDB;


    @Bean
    public Job itermWriterDomeJob(){
        return jobBuilderFactory.get("itermWriterDomeJobDB").start(itermWriterDomeStep()).build();
    }

    @Bean
    public Step itermWriterDomeStep() {
        return stepBuilderFactory.get("itermWriterDomeStepDB").<User,User>chunk(2)
                .reader(fileItemReader()).writer(itermFileWriterDB).build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<User> fileItemReader(){
        FlatFileItemReader<User> flatFileItemReader = new FlatFileItemReader<>();
        flatFileItemReader.setLinesToSkip(1);
        flatFileItemReader.setResource(new ClassPathResource("User.txt"));
        //解析数据
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames(new String[]{"id","name","sex","birthday"});
        //映射为User对象
        DefaultLineMapper<User> mapper = new DefaultLineMapper<>();
        mapper.setLineTokenizer(tokenizer);
        mapper.setFieldSetMapper(new FieldSetMapper<User>() {
            @Override
            public User mapFieldSet(FieldSet fieldSet) throws BindException {
                User user = new User();
                user.setId(fieldSet.readInt("id"));
                user.setBirthday(fieldSet.readString("birthday"));
                user.setName(fieldSet.readString("name"));
                user.setSex(fieldSet.readString("sex"));
                return user;
            }
        });
        mapper.afterPropertiesSet();//安全检查
        flatFileItemReader.setLineMapper(mapper);

        return  flatFileItemReader;
    }

}
