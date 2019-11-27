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


@Configuration
@EnableBatchProcessing
public class ItemReaderFileConfiguration {
    @Autowired
    private JobBuilderFactory jobBuilderFactory;
    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Qualifier("itemWriterFile")
    @Autowired
    private ItemWriter itemWriterFile;

    @Bean
    public Step itemReaderFileStep(){
        return  stepBuilderFactory.get("itemReaderFileStep").<User,User>chunk(5).reader(fileRead()).writer(itemWriterFile).build();
    }
    @Bean
    public Job itemReaderFileJob(){
        return jobBuilderFactory.get("itemReaderFileJob").start(itemReaderFileStep()).build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<User> fileRead() {
        FlatFileItemReader<User> reader = new FlatFileItemReader<>();
        reader.setResource(new ClassPathResource("User.txt"));
        reader.setLinesToSkip(1);//跳过第一行

        //解析数据
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames(new String[]{"id","name","sex","birthday"});
        //把解析出的一行数据映射为customer对象
        DefaultLineMapper<User> mapper = new DefaultLineMapper<>();
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
        reader.setLineMapper(mapper);
        return reader;
    }
}
