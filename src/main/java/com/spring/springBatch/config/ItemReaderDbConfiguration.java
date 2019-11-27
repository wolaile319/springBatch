package com.spring.springBatch.config;


import com.spring.springBatch.domain.Customer;
import com.spring.springBatch.itermwriter.DbJdbcWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.RowMapper;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * ItemReader从数据库中读取数据
 */
@Configuration
@EnableBatchProcessing
public class ItemReaderDbConfiguration {
    @Autowired
    private JobBuilderFactory jobBuilderFactory;
    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    @Autowired
    private DataSource dataSource;
    @Autowired
    @Qualifier("dbJdbcWriter")
    private DbJdbcWriter dbJdbcWriter;

    @Bean
    public Step itemReaderDbStep(){
        return  stepBuilderFactory.get("itemReaderDbStep").<Customer,Customer>chunk(2)
                .reader(dbJdbcReader()).writer(dbJdbcWriter).build();
    }
    @Bean
    public JdbcPagingItemReader<Customer> dbJdbcReader() {
        JdbcPagingItemReader<Customer> reader = new JdbcPagingItemReader<Customer>();
        reader.setDataSource(dataSource);
        reader.setFetchSize(2);
        //把读取到的记录转化为Customer对象
        reader.setRowMapper(new RowMapper<Customer>() {
            @Override
            public Customer mapRow(ResultSet resultSet, int i) throws SQLException {
                Customer customer = new Customer();
                customer.setId(resultSet.getInt(1));
                customer.setAge(resultSet.getInt(2));
                customer.setBirth(resultSet.getDate(3));
                customer.setEmail(resultSet.getString(4));
                customer.setLastName(resultSet.getString(5));
                customer.setCreateTime(resultSet.getTimestamp(6));
                return customer;
            }
        });
        MySqlPagingQueryProvider provider = new MySqlPagingQueryProvider();
        provider.setSelectClause("id,age,birth,email,last_name,createdTime");
        provider.setFromClause("jpa_customers");
        //
        Map<String,Order> sort = new HashMap<>(1);
        sort.put("id",Order.ASCENDING);
        provider.setSortKeys(sort);
        reader.setQueryProvider(provider);
        return  reader;
    }

    @Bean
    public Job itemReadJob() {
        return jobBuilderFactory.get("itemReadJob").start(itemReaderDbStep()).build();
    }
}
