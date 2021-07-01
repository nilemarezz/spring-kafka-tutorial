package com.nile.libraryeventproducer.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.nile.libraryeventproducer.domain.LibraryEvent;
import com.nile.libraryeventproducer.domain.LibraryEventType;
import com.nile.libraryeventproducer.producer.LibraryProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1")
public class LibraryEventController {

    @Autowired
    LibraryProducer libraryProducer;

    @PostMapping("/libraryevent")
    public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException {
        libraryEvent.setType(LibraryEventType.NEW);
        libraryProducer.sendLibraryEvent_Approach2(libraryEvent);
        return new ResponseEntity<>(HttpStatus.CREATED);
    }
}
