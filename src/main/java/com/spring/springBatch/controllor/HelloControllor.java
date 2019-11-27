package com.spring.springBatch.controllor;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HelloControllor {
    @Autowired
    private JobLauncher launcher;
    @Autowired
    private Job firstJob;

    @GetMapping("/hello")
    public String helloWorld(){
        return  "我爱你!！！！";
    }

    @GetMapping("/sendJob/{msg}")
    public String sendJobSingle(@PathVariable("msg") String msg){
        //接受任务参数
        JobParameters parameters = new JobParametersBuilder().addString("msg",msg).toJobParameters();
        //启动任务，并把任务传给参数
        try {
            launcher.run(firstJob,parameters);
        } catch (JobExecutionAlreadyRunningException e) {
            e.printStackTrace();
        } catch (JobRestartException e) {
            e.printStackTrace();
        } catch (JobInstanceAlreadyCompleteException e) {
            e.printStackTrace();
        } catch (JobParametersInvalidException e) {
            e.printStackTrace();
        }
        return  "job 执行成功！";
    }
}
