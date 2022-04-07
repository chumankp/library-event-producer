package com.chuman.kafka.controller;

import com.chuman.kafka.domain.Book;
import com.chuman.kafka.domain.LibraryEvent;
import com.chuman.kafka.producer.LibraryEventProducer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(LibraryEventController.class)
@AutoConfigureMockMvc
class LibraryEventControllerUnitTest {
    @Autowired
    MockMvc mockMvc;

    ObjectMapper objectMapper = new ObjectMapper();

    @MockBean
    LibraryEventProducer eventProducer;

    @Test
    void createLibraryEventTest() throws Exception {
        //given
        Book book = Book.builder().bookId(123).bookAuthor("chuman")
                .bookName("kafka useing spring boot")
                .build();
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book).build();

        String json = objectMapper.writeValueAsString(libraryEvent);

        when(eventProducer.sendLibraryEventApproach2(isA(LibraryEvent.class))).thenReturn(null);

        //expect
        mockMvc.perform(post("/v1/libraryevent")
                .content(json)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isCreated());

    }

    @Test
    void createLibraryEventTest_4XX_Error() throws Exception {
            //given
        Book book = Book.builder()
                .bookId(null)
                .bookAuthor(null)
                .bookName("Kafka using Spring Boot")
                .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();

        String json = objectMapper.writeValueAsString(libraryEvent);
        //for void return type
        //doNothing().when(eventProducer).sendLibraryEvent_Approach2((isA(LibraryEvent.class)));
        when(eventProducer.sendLibraryEventApproach2(isA(LibraryEvent.class))).thenReturn(null);
        //expect
        String expectedErrorMessage = "book.bookAuthor - must not be blank, book.bookId - must not be null";
        mockMvc.perform(post("/v1/libraryevent")
                        .content(json)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().is4xxClientError())
                .andExpect(content().string(expectedErrorMessage));
    }

    @Test
    void updateLibraryEventTest() throws Exception {
        //given
        Book book = Book.builder()
                .bookId(123)
                .bookAuthor("chuman")
                .bookName("kafka useing spring boot")
                .build();
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(123)
                .book(book).build();

        String json = objectMapper.writeValueAsString(libraryEvent);

        when(eventProducer.sendLibraryEventApproach2(isA(LibraryEvent.class))).thenReturn(null);

        //expect
        mockMvc.perform(put("/v1/libraryevent")
                        .content(json)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());
    }

    @Test
     void updateLibraryEvent_withNullLibraryEventIdTest() throws Exception {
        //given
        Book book = Book.builder()
                .bookId(123)
                .bookAuthor("chuman")
                .bookName("kafka useing spring boot")
                .build();
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book).build();

        String json = objectMapper.writeValueAsString(libraryEvent);

        when(eventProducer.sendLibraryEventApproach2(isA(LibraryEvent.class))).thenReturn(null);

        //expect
        mockMvc.perform(put("/v1/libraryevent")
                        .content(json)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().is4xxClientError())
                .andExpect(content().string("Please send the LibraryEventId"));
    }
}
