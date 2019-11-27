package com.spring.springBatch.itermwriterconfig;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spring.springBatch.domain.User;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.batch.item.support.ClassifierCompositeItemWriter;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.classify.Classifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.oxm.xstream.XStreamMarshaller;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * 将数据库中的数据按照一定规则读到不同的文件内
 */
@Configuration
@EnableBatchProcessing
public class MoreFileItemWriterConfiguration {
    @Autowired
    private JobBuilderFactory jobBuilderFactory;
    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    @Autowired
    private DataSource dataSource;

    @Bean
    public Job moreFileItemJob(){
        return  jobBuilderFactory.get("moreFileItemJob3").start(moreFileItemStep()).build();
    }
    @Bean
    public Step moreFileItemStep(){
        return stepBuilderFactory.get("moreFileItemStep3").<User,User>chunk(3).
                reader(jdbcItemReader()).writer(fenLieItemWriter())
                .stream(jdbcItemWriter()).stream(xmlFileWriter()).build();
    }

    /**
     * 按照具体的规则写入不同的文件
     * @return
     */
    @Bean
    public ClassifierCompositeItemWriter<User> fenLieItemWriter(){
        ClassifierCompositeItemWriter<User> write = new ClassifierCompositeItemWriter<>();
        write.setClassifier(new Classifier<User, ItemWriter<? super User>>() {
            @Override
            public ItemWriter<? super User> classify(User user) {
                ItemWriter<User> writer = user.getId() % 2 == 0 ? jdbcItemWriter() : xmlFileWriter();
                return writer;
            }
        });
        return  write;
    }


    /**
     * 按照不同的方式写入不同的文件
     * @return
     */
    @Bean
    public CompositeItemWriter<User> moreItemWriter() {
        CompositeItemWriter<User> compositeItemWriter = new CompositeItemWriter<>();
        compositeItemWriter.setDelegates(Arrays.asList(jdbcItemWriter(),xmlFileWriter()));
        try {
            compositeItemWriter.afterPropertiesSet();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return compositeItemWriter;
    }


    /**
     * 读到普通的文件中去
     * @return
     */
    @Bean
    public FlatFileItemWriter<User> jdbcItemWriter() {
        FlatFileItemWriter<User> flatFileItemWriter = new FlatFileItemWriter<>();
        String path = "E:\\user2.txt";
        flatFileItemWriter.setResource(new FileSystemResource(path));
        //把对象转化为字符串
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
        try {
            flatFileItemWriter.afterPropertiesSet();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return flatFileItemWriter;
    }

    /**
     * 把文件写入xml里面
     * @return
     */
    @Bean
    public StaxEventItemWriter<User> xmlFileWriter(){
        StaxEventItemWriter<User> writer = new StaxEventItemWriter<>();
        String path = "E:\\user2.xml";
        writer.setResource(new FileSystemResource(path));

        XStreamMarshaller marshaller = new XStreamMarshaller();
        Map<String,Class> map = new HashMap<>();
        map.put("user",User.class);
        marshaller.setAliases(map);

        writer.setRootTagName("users");
        writer.setMarshaller(marshaller);
        try {
            writer.afterPropertiesSet();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return writer;

    }

    /**
     * 重数据库中读取数据
     * @return
     */
    @Bean
    public JdbcPagingItemReader<User> jdbcItemReader() {
        JdbcPagingItemReader<User> reader = new JdbcPagingItemReader<>();
        reader.setDataSource(dataSource);
        reader.setFetchSize(2);
        //查询数据
        MySqlPagingQueryProvider queryProvider = new MySqlPagingQueryProvider();
        queryProvider.setFromClause("user");
        queryProvider.setSelectClause("id,name,sex,birthday");
        Map<String,Order> sort = new HashMap<>();
        sort.put("id",Order.ASCENDING);
        queryProvider.setSortKeys(sort);
        reader.setQueryProvider(queryProvider);
        //查询的数据做映射
        reader.setRowMapper(new RowMapper<User>() {
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
        return reader;
    }

}
