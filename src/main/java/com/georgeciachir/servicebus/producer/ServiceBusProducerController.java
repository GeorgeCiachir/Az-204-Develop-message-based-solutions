package com.georgeciachir.servicebus.producer;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/servicebus/producer")
public class ServiceBusProducerController {

    private static final Logger LOG = LoggerFactory.getLogger(ServiceBusProducerController.class);

    private static final String SAS_TOPIC_CONNECTION = "Endpoint=sb://my-messages.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=gVcd16UkTFNIeH5kG6mkC+VA+scqmbKUdAQeQsNI+9k=";
    private static final String TOPIC_NAME = "firsttopic";

    private static final ServiceBusSenderClient TOPIC_PRODUCER = createTopicProducer();

    @PostMapping("/send")
    public void send(@RequestParam String message,
                     @RequestParam(required = false) String messageId) {
        LOG.info("Sending a message");

        ServiceBusMessage busMessage = new ServiceBusMessage(message);

        if (messageId != null) {
            busMessage.setMessageId(messageId);
        } else {
            busMessage.setMessageId(UUID.randomUUID().toString());
        }

        TOPIC_PRODUCER.sendMessage(busMessage);
    }

    private static ServiceBusSenderClient createTopicProducer() {
        return new ServiceBusClientBuilder()
                .connectionString(SAS_TOPIC_CONNECTION)
                .sender()
                .topicName(TOPIC_NAME)
                .buildClient();
    }
}
