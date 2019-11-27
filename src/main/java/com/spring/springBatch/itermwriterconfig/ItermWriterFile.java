package com.spring.springBatch.itermwriterconfig;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spring.springBatch.domain.User;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.RowMapper;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * 把数据库的数据写入指定文件中
 */
@Configuration
@EnableBatchProcessing
public class ItermWriterFile {
    @Autowired
    private JobBuilderFactory jobBuilderFactory;
    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    @Autowired
    private DataSource dataSource;

    @Bean
    public Job itermWriterFileJob(){
        return jobBuilderFactory.get("itermWriterFileJob").start(itermWriterFileStep()).build();
    }

    @Bean
    public Step itermWriterFileStep() {
        return stepBuilderFactory.get("itermWriterFileStep").<User,User>chunk(3)
                .reader(pagingItemReader()).writer(pagingItemWriter()).build();
    }

    /**
     * 将读取的数据保存在文件中
     * @return
     */
    public FlatFileItemWriter<User> pagingItemWriter() {
        FlatFileItemWriter<User> writer = new FlatFileItemWriter<>();
        String path = "E:\\user.txt";
        writer.setResource(new FileSystemResource(path));
        //把对象转化为字符串
        writer.setLineAggregator(new LineAggregator<User>() {
            ObjectMapper mapper = new ObjectMapper();
            @Override
            public String aggregate(User user) {
                String str = null;
                try {
                   str = mapper.writeValueAsString(user);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
                return str;
            }
        });
        try {
            writer.afterPropertiesSet();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return  writer;
    }

    /**
     * 对数据库中读取数据
     * @return
     */
    @Bean
    public JdbcPagingItemReader<User> pagingItemReader(){
        JdbcPagingItemReader<User> reader = new JdbcPagingItemReader<>();
        reader.setDataSource(dataSource);
        reader.setFetchSize(2);
        MySqlPagingQueryProvider provider = new MySqlPagingQueryProvider();
        provider.setSelectClause("id,name,sex,birthday");
        provider.setFromClause("user");
        Map<String,Order> sort = new HashMap<>(1);
        sort.put("id",Order.ASCENDING);
        provider.setSortKeys(sort);
        reader.setQueryProvider(provider);
        //把读到的数据映射到User对象里面去
        reader.setRowMapper(new RowMapper<User>() {
            @Override
            public User mapRow(ResultSet resultSet, int i) throws SQLException {
                User user = new User();
                user.setId(resultSet.getInt(1));
                user.setName(resultSet.getString(2));
                user.setSex(resultSet.getString(3));
                user.setBirthday(resultSet.getString(4));
                return user;
            }
        });
        return reader;
    }



}
