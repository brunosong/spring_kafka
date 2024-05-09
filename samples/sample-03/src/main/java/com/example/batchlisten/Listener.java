package com.example.batchlisten;

import com.example.batchlisten.common.Foo2;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.convert.ConversionException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;

@Slf4j
@Component
public class Listener {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @KafkaListener(id = "fooGroup2", topics = "topic2")
    public void listen1(List<Foo2> foos) throws IOException {
        log.info("Received : {}" , foos);
        foos.forEach(f -> kafkaTemplate.send("topic3", f.getFoo().toUpperCase()) );
        log.info("Messages sent, hit Enter to commit tx");
        System.in.read();
    }

    @KafkaListener(id = "fooGroup3", topics = "topic3")
    public void listen2(List<String> in) {
        log.info("Received : {} ", in);
        BatchListenApplication.LATCH.countDown();
    }

    /* offset 롤백 테스트 용 */
    @KafkaListener(id = "fooFailGroup", topics = "topic_fail")
    public void listen1Fail(List<Foo2> foos, @Header(KafkaHeaders.CONVERSION_FAILURES) List<ConversionException> exceptions) {
        log.info("Received : {}" , foos);
        if(foos.size() > 1) {
            throw new BatchListenerFailedException("Conversion error", exceptions.get(0), 0);
        }
    }

}
