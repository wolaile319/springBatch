package com.spring.springBatch.retrydemo;

import com.spring.springBatch.domain.User;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.RowMapper;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * 错误重试或跳过的方法
 */
@Configuration
@EnableBatchProcessing
public class RetryDemo{

    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    @Autowired
    private JobBuilderFactory jobBuilderFactory;
    @Autowired
    private DataSource dataSource;

    private  int count = 0;

    @Bean
    public Step retryStep(){
        return stepBuilderFactory.get("retryStep").<User,User>chunk(3).reader(reTryJdbcReader())
                .processor(retryProcessor())
                .writer(printWriter())
                .faultTolerant()//容错
               /* .retry(Exception.class)// 发生这样发生异常时重试
                .retryLimit(3)//重试几次，才不重试了*/
                .skip(Exception.class)// 发生这样错误时跳过
                .skipLimit(2)//跳过几次，才不跳了*/
                .listener(mySkipListener())
                .listener(this)
                .build();
    }
    @Bean
    public ItemWriter<User> printWriter() {
        ItemWriter<User> itemWriter = list -> {
            for (User u:list) {
                System.out.println(u);
            }
        };
        return itemWriter;
    }

    @Bean
    public ItemProcessor<User,User> retryProcessor() {
//        System.out.println(map.get("msg").getValue());
        ItemProcessor<User,User> processor = new ItemProcessor<User, User>() {
            @Override
            public User process(User user) throws Exception {
                System.out.println("processing item" + user.getId());
               Integer id = user.getId();
               if(id == 5){
                   count++;
                   System.out.println("===========reTry the" + count +"次");
                   throw new Exception("我要报仇");
               }
               if(id == 6) {
                   count++;
                   System.out.println("===========reTry the" + count +"次");
                   if (count <= 2) {
                       throw new Exception("我要报仇");
                   } else {
                       System.out.println("我成功了");
                       System.out.println("processed the" + user);
                   }
               }else{
                   System.out.println("processed the" + user);
               }
                return user;
            }
        };
        return processor;
    }

    /**
     * 从数据库中读取数据
     * @return
     */
    @Bean
    public JdbcPagingItemReader<User> reTryJdbcReader() {
        JdbcPagingItemReader<User> itemReader = new JdbcPagingItemReader<User>();
        itemReader.setDataSource(dataSource);
        itemReader.setFetchSize(2);
        MySqlPagingQueryProvider pagingQueryProvider = new MySqlPagingQueryProvider();
        pagingQueryProvider.setFromClause("user");
        pagingQueryProvider.setSelectClause("id,name,sex,birthday");
        Map<String,Order> map = new HashMap<>();
        map.put("id",Order.ASCENDING);
        pagingQueryProvider.setSortKeys(map);
        itemReader.setQueryProvider(pagingQueryProvider);
        itemReader.setRowMapper(new RowMapper<User>() {
            @Override
            public User mapRow(ResultSet resultSet, int i) throws SQLException {
                User user = new User();
                user.setId(resultSet.getInt("id"));
                user.setName(resultSet.getString("name"));
                user.setSex(resultSet.getString("sex"));
                user.setBirthday(resultSet.getString("birthday"));
                return user;
            }
        });
        return itemReader;
    }

    @Bean
    public SkipListener<User,User> mySkipListener(){
        SkipListener<User,User> listener = new SkipListener<User, User>() {
            @Override
            public void onSkipInRead(Throwable throwable) {

            }

            @Override
            public void onSkipInWrite(User user, Throwable throwable) {

            }

            @Override
            public void onSkipInProcess(User user, Throwable throwable) {
                System.out.println("发生的异常为" + user.getId() + ",异常信息为" + throwable.getMessage());
            }
        };
        return listener;
    }

    @Bean
    public Job retryJob(){
        return jobBuilderFactory.get("retryJob13").start(retryStep()).build();
    }
}
