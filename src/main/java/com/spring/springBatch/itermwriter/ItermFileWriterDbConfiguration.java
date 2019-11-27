package com.spring.springBatch.itermwriter;


import com.spring.springBatch.domain.User;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
public class ItermFileWriterDbConfiguration {
    @Autowired
    private DataSource dataSource;
    @Bean
    public JdbcBatchItemWriter<User> ItermFileWriterDb(){
        JdbcBatchItemWriter<User> batchItemWriter = new JdbcBatchItemWriter<>();
        batchItemWriter.setDataSource(dataSource);
        batchItemWriter.setSql("insert into user(id,name,sex,birthday) values(:id,:name,:sex,:birthday)");
        batchItemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
        return batchItemWriter;
    }


}
