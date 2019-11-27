package com.spring.springBatch.config;


import com.spring.springBatch.method.MyDecider;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 决策器
 */
@Configuration
@EnableBatchProcessing
public class DecoderConfiguration {
    @Autowired
    private JobBuilderFactory jobBuilderFactory;
    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Bean
    public Step deciderStep1(){
        return stepBuilderFactory.get("deciderStep1").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("我是even");
                return RepeatStatus.FINISHED;
            }
        }).build();
    }

    @Bean
    public Step deciderStep2(){
        return stepBuilderFactory.get("deciderStep2").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("我是odd");
                return RepeatStatus.FINISHED;
            }
        }).build();
    }

    @Bean
    public Step deciderStep3(){
        return stepBuilderFactory.get("deciderStep3").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("我是deciderStep3");
                return RepeatStatus.FINISHED;
            }
        }).build();
    }

    //创建决策器
    @Bean
    public JobExecutionDecider myDecider(){
        return  new MyDecider();
    }

    @Bean
    public Job decoderJob(){
        return  jobBuilderFactory.get("decoderJob8").start(deciderStep3()).next(myDecider())
                .from(myDecider()).on("even").to(deciderStep1())
                .from(myDecider()).on("odd").to(deciderStep2())
                .from(deciderStep2()).on("*").to(myDecider())
                .end().build();
    }

}
