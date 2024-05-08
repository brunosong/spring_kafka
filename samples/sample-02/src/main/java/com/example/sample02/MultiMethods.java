package com.example.sample02;

import com.example.sample02.common.Bar2;
import com.example.sample02.common.Foo2;
import com.example.sample02.common.Song2;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@KafkaListener(id = "multiGroup" , topics = { "foos" , "bars" })
public class MultiMethods {

    private final TaskExecutor executor = new SimpleAsyncTaskExecutor();

    @KafkaHandler
    public void foo(Foo2 foo) {
        log.info("call foo() : {}", foo);
        terminateMessage();
    }

    @KafkaHandler
    public void bar(Bar2 bar) {
        log.info("call bar() : {}", bar);
        terminateMessage();
    }

    @KafkaHandler
    public void song(Song2 song) {
        log.info("call song() : {}", song);
        terminateMessage();
    }

    @KafkaHandler(isDefault = true)
    public void unknown(Object object) {
        log.info("call unknown() : {}", object);
        terminateMessage();
    }

    public void terminateMessage() {
        executor.execute(() -> {
            System.out.println("call terminateMessage");
        });
    }


}
