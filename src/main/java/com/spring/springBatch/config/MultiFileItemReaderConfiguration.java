package com.spring.springBatch.config;


import com.spring.springBatch.domain.User;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.validation.BindException;

/**
 * 多文件批处理
 */
@Configuration
@EnableBatchProcessing
public class MultiFileItemReaderConfiguration {
    @Autowired
    private JobBuilderFactory jobBuilderFactory;
    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    @Value("classpath:/file*.txt")
    private Resource[] resource;
    @Autowired
    @Qualifier("itemWriterFile")
    private ItemWriter multiFileItemWrite;

    @Bean
    public Job multiFileItemReaderJob(){
        return  jobBuilderFactory.get("multiFileItemReaderJob").start(multiFileItemReaderStep()).build();
    }

    @Bean
    public Step multiFileItemReaderStep() {
        return  stepBuilderFactory.get("multiFileItemReaderStep").<User,User>chunk(2).reader(multiFileItemReader()).writer(multiFileItemWrite)
                .build();
    }

    @Bean
    @StepScope
    public MultiResourceItemReader<User> multiFileItemReader() {
        MultiResourceItemReader<User> resourceItemReader = new MultiResourceItemReader<>();
        resourceItemReader.setResources(resource);
        resourceItemReader.setDelegate(flatFileReader());

        return resourceItemReader;
    }

    private FlatFileItemReader<User> flatFileReader() {
        FlatFileItemReader<User> itemReader = new FlatFileItemReader<>();
//        itemReader.setLinesToSkip(0);

        //映射数据封装为实体
        DefaultLineMapper<User> mapper = new DefaultLineMapper<>();
        //解析数据
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames(new String[]{"id","name","sex","birthday"});
        mapper.setLineTokenizer(tokenizer);
        mapper.setFieldSetMapper(new FieldSetMapper<User>() {
            @Override
            public User mapFieldSet(FieldSet fieldSet) throws BindException {
                User user = new User();
                user.setId(fieldSet.readInt("id"));
                user.setName(fieldSet.readString("name"));
                user.setSex(fieldSet.readString("sex"));
                user.setBirthday(fieldSet.readString("birthday"));
                return user;
            }
        });
        mapper.afterPropertiesSet();//做检查
        itemReader.setLineMapper(mapper);
        return  itemReader;
    }
}
