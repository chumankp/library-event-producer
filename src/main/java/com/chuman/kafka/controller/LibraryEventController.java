package com.chuman.kafka.controller;

import com.chuman.kafka.domain.LibraryEvent;
import com.chuman.kafka.domain.LibraryEventType;
import com.chuman.kafka.producer.LibraryEventProducer;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@RestController
@Slf4j
public class LibraryEventController {

    @Autowired
    LibraryEventProducer eventProducer;

    @PostMapping("/v1/libraryevent")
    public ResponseEntity<LibraryEvent> createLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {
        log.info("Before sendLibraryEvent");
        //eventProducer.sendLibraryEvent(libraryEvent);
        //SendResult<Integer,String> sendResult = eventProducer.sendLibraryEventSynchronous(libraryEvent);

        libraryEvent.setLibraryEventType(LibraryEventType.NEW);
        eventProducer.sendLibraryEventApproach2(libraryEvent);
        //log.info("sendResult is {} ", sendResult.toString() );
        log.info("after sendLibraryEvent");
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }
    @PutMapping("/v1/libraryevent")
    public ResponseEntity<?> updateLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {
        if(libraryEvent.getLibraryEventId() == null){
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please send the LibraryEventId");
        }

        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        eventProducer.sendLibraryEventApproach2(libraryEvent);
        return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
    }
}
