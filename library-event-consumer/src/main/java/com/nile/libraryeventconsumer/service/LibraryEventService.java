package com.nile.libraryeventconsumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nile.libraryeventconsumer.entity.LibraryEvent;
import com.nile.libraryeventconsumer.repository.LibraryEventRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.Optional;

@Service
@Slf4j
public class LibraryEventService {
    @Autowired
    LibraryEventRepository libraryEventRepository;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    KafkaTemplate<Integer , String> kafkaTemplate;

    public void processLibraryEvent(ConsumerRecord<Integer , String> consumerRecord) throws JsonProcessingException {
        log.info("Process library Event ");
        System.out.println(consumerRecord.toString());
        LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value() , LibraryEvent.class);

        if(libraryEvent.getLibraryEventId() != null && libraryEvent.getLibraryEventId() == 111){
            throw new RecoverableDataAccessException("Temporary Network Issue");
        }
        switch (libraryEvent.getLibraryEventType()){
            case NEW:
                save(libraryEvent);
                break;
            case UPDATE:
                validate(libraryEvent);
                save(libraryEvent);
                break;
        }
    }

    private void save(LibraryEvent libraryEvent){
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventRepository.save(libraryEvent);
        log.info("Success Add To Database");
    }

    private void validate(LibraryEvent libraryEvent){
        if(libraryEvent.getLibraryEventId() == null){
            throw new IllegalArgumentException("No id Avaliable");
        }

        Optional<LibraryEvent> find = libraryEventRepository.findById(libraryEvent.getLibraryEventId());
        if(!find.isPresent()){
            throw new IllegalArgumentException("ID Not found");
        }
    }

    public void handleRecovery(ConsumerRecord<Integer , String> record){
        Integer key = record.key();
        String value = record.value();
        ListenableFuture<SendResult<Integer, String>> listenableFuture =   kafkaTemplate.sendDefault(key , value);
        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSuccess(key , value , result);
            }

            @Override
            public void onFailure(Throwable ex) {
                handleFailure(key , value , ex);
            }
        });
    }

    private void handleFailure(Integer key, String value, Throwable ex) {
        log.error("Error Sending the Message and the exception is {}", ex.getMessage());

    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
        log.info("Message Sent SuccessFully for the key : {} and the value is {} , partition is {}", key, value, result.getRecordMetadata().partition());
    }
}
