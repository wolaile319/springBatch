package com.spring.springBatch.Itermprocess;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spring.springBatch.domain.User;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.RowMapper;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableBatchProcessing
public class ItermProcessDemo {
    @Autowired
    private JobBuilderFactory jobBuilderFactory;
    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    @Autowired
    private DataSource dataSource;

    @Bean
    public Job processorJob(){
        return  jobBuilderFactory.get("processorJob").start(processorStep()).build();
    }

    @Bean
    public Step processorStep(){
        return stepBuilderFactory.get("processorStep").<User,User>chunk(2)
                .reader(jdbcReader()).processor(compositeItemProcessorDemo()).writer(processorFile()).build();
    }

    /**
     * 第一种处理方式
     * @return
     */
    @Bean
    public ItemProcessor<User,User> jdbcprocessor1() {
        ItemProcessor<User,User>  itemProcessor = new ItemProcessor<User, User>() {
            @Override
            public User process(User user) throws Exception {
               String sex = user.getSex();
               if (sex.equals("男")){
                   user.setSex("女");
               }
                if (sex.equals("女")){
                    user.setSex("男");
                }
                return user;
            }
        };
        return itemProcessor;
    }


    /**
     * 第二种处理方式
     * @return
     */
    @Bean
    public ItemProcessor<User,User> jdbcprocessor2() {
        ItemProcessor<User,User>  itemProcessor = new ItemProcessor<User, User>() {
            @Override
            public User process(User user) throws Exception {
                String name = user.getName();
                user.setName(name+"帅");
                return user;
            }
        };
        return itemProcessor;
    }
    @Bean
    public CompositeItemProcessor<User,User> compositeItemProcessorDemo(){
        CompositeItemProcessor<User,User> compositeItemProcessor = new CompositeItemProcessor<>();
        compositeItemProcessor.setDelegates(Arrays.asList(jdbcprocessor1(),jdbcprocessor2()));
        try {
            compositeItemProcessor.afterPropertiesSet();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return compositeItemProcessor;
    }
    /**
     * 从数据库中读数据
     * @return
     */
    @Bean
    public JdbcPagingItemReader<User> jdbcReader() {
        JdbcPagingItemReader<User> itemReader = new JdbcPagingItemReader<>();
        itemReader.setFetchSize(3);
        //设置数据源
        itemReader.setDataSource(dataSource);
        //从数据库中查询数据
        MySqlPagingQueryProvider pagingQueryProvider = new MySqlPagingQueryProvider();
        pagingQueryProvider.setFromClause("user");
        pagingQueryProvider.setSelectClause("id,name,sex,birthday");
        Map<String, Order> sort = new HashMap<>();
        sort.put("id",Order.ASCENDING);
        pagingQueryProvider.setSortKeys(sort);
        itemReader.setQueryProvider(pagingQueryProvider);
        //把数据映射为对象
        itemReader.setRowMapper(new RowMapper<User>() {
            @Override
            public User mapRow(ResultSet resultSet, int i) throws SQLException {
                User user = new User();
                user.setId(resultSet.getInt("id"));
                user.setName(resultSet.getString("name"));
                user.setBirthday(resultSet.getString("birthday"));
                user.setSex(resultSet.getString("sex"));
                return user;
            }
        });
        try {
            itemReader.afterPropertiesSet();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return itemReader;
    }

    /**
     * 从数据库中写数据
     * @return
     */
    @Bean
    public FlatFileItemWriter<User> processorFile(){
        FlatFileItemWriter<User> flatFileItemWriter = new FlatFileItemWriter<>();
        //设置文件保存地址
        String path = "E:\\user5.txt";
        flatFileItemWriter.setResource(new FileSystemResource(path));
        //将user对象转化为字符串
        flatFileItemWriter.setLineAggregator(new LineAggregator<User>() {
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
        return flatFileItemWriter;
    }
}
