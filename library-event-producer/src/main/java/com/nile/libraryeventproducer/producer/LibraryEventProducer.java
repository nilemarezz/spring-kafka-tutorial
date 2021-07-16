package com.nile.libraryeventproducer.producer;


import com.nile.libraryeventproducer.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;


@Slf4j
@Component
public class LibraryEventProducer {

    @Autowired
    KafkaTemplate<Integer , LibraryEvent> kafkaTemplate;

//
//
    public static final String topic = "library-events";
    public void sendLibraryEvent_Approach2(LibraryEvent libraryEvent){
        Integer key = libraryEvent.getLibraryEventId();
        ProducerRecord<Integer, LibraryEvent> producerRecord = buildProducerRecord(key, libraryEvent, topic);
        ListenableFuture<SendResult<Integer, LibraryEvent>> listenableFuture =  kafkaTemplate.send(producerRecord);
        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, LibraryEvent>>() {

            @Override
            public void onSuccess(SendResult<Integer, LibraryEvent> result) {
                handleSuccess(key , libraryEvent.toString() , result);
            }

            @Override
            public void onFailure(Throwable ex) {
                handleFailure(key , libraryEvent.toString() , ex);
            }
        });
    }


    private ProducerRecord<Integer, LibraryEvent> buildProducerRecord(Integer key, LibraryEvent value, String topic) {

        return new ProducerRecord<>(topic, null, key, value, null);
    }

    private void handleFailure(Integer key, String value, Throwable ex) {
        log.error("Error Sending the Message and the exception is {}", ex.getMessage());

    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, LibraryEvent> result) {
        log.info("Message Sent SuccessFully for the key : {} and the value is {} , partition is {}", key, value, result.getRecordMetadata().partition());
    }
}
