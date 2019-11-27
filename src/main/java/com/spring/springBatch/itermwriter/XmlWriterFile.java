package com.spring.springBatch.itermwriter;

import com.spring.springBatch.domain.User;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

import java.util.List;

@Component("xmlWriterFile")
public class XmlWriterFile implements ItemWriter<User> {
    @Override
    public void write(List<? extends User> list) throws Exception {
        for (User user : list) {
            System.out.println(user);
        }
    }
}
