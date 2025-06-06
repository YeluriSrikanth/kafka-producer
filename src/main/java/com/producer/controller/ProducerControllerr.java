package com.producer.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.produer.entity.User;
import org.springframework.web.client.RestTemplate;

@RestController
@RequestMapping("/producer")
public class ProducerControllerr {
    public static String message;

    @Value("${kafka.producer.topic.name}")
    private String topic_name;

    @Value("${kafka.producer.topic2.name}")
    private String t_name;


    @Autowired
    KafkaTemplate<String, String> kafkaTemplate1;

    @Autowired
    KafkaTemplate<String, Object> kafkaTemplate;


    @Autowired
   private  RestTemplate restTemplate;


    @GetMapping("/process/{count}")
    public void postMessage(@PathVariable int count) {
        for (int i = 1; i < 100; i++) {
            String s = "Hello Mr." + i;
            kafkaTemplate1.send(topic_name, s);

        }

    }

    @GetMapping("/test")
    public void  testApi(){
        for(int i=1;i<=10;i++){
          String res= restTemplate.getForObject("http://localhost:8181/producer/"+i,String.class);
          System.out.println("Final outcome........"+res);
        }

    }



    @GetMapping("/{name}")
    public String getName(@PathVariable String name) throws InterruptedException {
        kafkaTemplate.send(t_name, name);

        Thread.sleep(5000);
        System.out.println(message);
        return message;

    }

    @PostMapping("/post")
    public String doTransation(@RequestBody User user) {
        try {
            kafkaTemplate.send("upi-topic", new ObjectMapper().writeValueAsString(user));
        } catch (JsonProcessingException e) {

            e.printStackTrace();
        }

        return "Transaction successful funds debited ! for User";

    }

    @KafkaListener(topics = "transaction-topic", groupId = "transaction-group")
    @SendTo("upi-topic")
    public User afterTransation(User user) {

        return user;

    }

//	 @KafkaListener(topics = "response-topic", groupId = "producer-group")
//	    @SendTo("producer_bucket")
//	    public void handleResponse(String message) {
//		 System.out.println("responce........."+message);
//	       // return "Processed: " + message;
//	    }


    @KafkaListener(topics = "name-response", groupId = "name-group")
    @SendTo("name-topic")
    public void handleNameresponce(String message) {
        System.out.println("Kafka Listening....");
        ProducerControllerr.message = message;
       // System.out.println(message);

    }
}
