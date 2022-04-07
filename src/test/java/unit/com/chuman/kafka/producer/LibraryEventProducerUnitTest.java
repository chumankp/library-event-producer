package com.chuman.kafka.producer;

import com.chuman.kafka.domain.Book;
import com.chuman.kafka.domain.LibraryEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
 class LibraryEventProducerUnitTest {
    @Mock
    KafkaTemplate<Integer, String> kafkaTemplate;
    @Spy
    ObjectMapper objectMapper = new ObjectMapper();

    @InjectMocks
    LibraryEventProducer eventProducer;

    @Test
     void sendLibraryEvent_Approach2_success() throws JsonProcessingException, ExecutionException, InterruptedException {
        //given
        Book book = Book.builder()
                .bookId(123)
                .bookAuthor("chuman")
                .bookName("Kafka using Spring Boot")
                .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();
        SettableListenableFuture listenableFuture = new SettableListenableFuture();

        String record = objectMapper.writeValueAsString(libraryEvent);

        ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>("library-events", libraryEvent.getLibraryEventId(), record);
        RecordMetadata metadata = new RecordMetadata(
                new TopicPartition("library-events", 1),1,1,342,
                System.currentTimeMillis(), 1, 2);

        SendResult<Integer, String> sendResult = new SendResult<Integer, String>(producerRecord, metadata);
        listenableFuture.set(sendResult);
        when(kafkaTemplate.send(isA(ProducerRecord.class))).
                thenReturn(listenableFuture);
        //when
       ListenableFuture<SendResult<Integer, String>> future = eventProducer.sendLibraryEventApproach2(libraryEvent);
       //then
       SendResult<Integer, String> result = future.get();

       assert result.getRecordMetadata().partition() == 1;
    }

    @Test
     void sendLibraryEvent_Approach2_failure() throws JsonProcessingException, ExecutionException, InterruptedException {
       //given
        Book book = Book.builder()
                .bookId(123)
                .bookAuthor("chuman")
                .bookName("Kafka using Spring Boot")
                .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();
        SettableListenableFuture listenableFuture = new SettableListenableFuture();

        listenableFuture.setException(new RuntimeException("Exception Calling Kafka"));
        when(kafkaTemplate.send(isA(ProducerRecord.class))).
                thenReturn(listenableFuture);
        //when
        assertThrows( Exception.class, ()->eventProducer.sendLibraryEventApproach2(libraryEvent).get());
    }
}
