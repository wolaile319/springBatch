package com.spring.springBatch.itermreader;


import com.spring.springBatch.domain.User;
import org.springframework.batch.item.*;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;
import org.springframework.validation.BindException;

import java.util.Objects;

@Component("reReader")
public class ReItemReader implements ItemStreamReader<User> {

    private FlatFileItemReader<User> flatFileItemReader  = new FlatFileItemReader<>();
    private long curLine = 0L;
    private  boolean restart = false;

    private  ExecutionContext executionContext;

    public ReItemReader(){
        flatFileItemReader.setResource(new ClassPathResource("file1.txt"));
//        flatFileItemReader.setLinesToSkip(0);
        DefaultLineMapper<User> mapper = new DefaultLineMapper<>();
       DelimitedLineTokenizer  tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames(new String[]{"id","name","sex","birthday"});
        mapper.setLineTokenizer(tokenizer);
        FieldSetMapper<User> fieldSetMapper = new FieldSetMapper<User>() {
            @Override
            public User mapFieldSet(FieldSet fieldSet) throws BindException {
                User user = new User();
                user.setId(fieldSet.readInt("id"));
                user.setName(fieldSet.readString("name"));
                user.setSex(fieldSet.readString("sex"));
                user.setBirthday(fieldSet.readString("birthday"));
                return user;
            }
        };
        mapper.setFieldSetMapper(fieldSetMapper);
        mapper.afterPropertiesSet();//安全检查
        flatFileItemReader.setLineMapper(mapper);
    }
    @Override
    public User read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        User user = null;
        this.curLine++;
        if(restart){
            flatFileItemReader.setLinesToSkip((int)this.curLine-1);
            restart = false;
            System.out.println("start1 reading from line" + this.curLine);
        }
        flatFileItemReader.open(this.executionContext);
        user = flatFileItemReader.read();
        if(!Objects.isNull(user) && 3 == user.getId()){
            throw  new Exception("这是id为"+user.getId()+"当前行数为" + this.curLine);
        }
        return user;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        System.out.println("我要开始批量了哦");
        this.executionContext = executionContext;
        if(executionContext.containsKey("curLine")){
            this.curLine = executionContext.getLong("curLine");
            this.restart = true;
        }else {
            this.curLine = 0L;
            executionContext.put("curLine",this.curLine);
            System.out.println("start reading from line " + (this.curLine + 1));
        }
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        executionContext.put("curLine",this.curLine);
        System.out.println("curLine" + this.curLine);
    }

    @Override
    public void close() throws ItemStreamException {

    }
}
