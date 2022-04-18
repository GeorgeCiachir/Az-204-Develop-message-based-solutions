package com.georgeciachir.servicebus.consumer;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusErrorContext;
import com.azure.messaging.servicebus.ServiceBusProcessorClient;
import com.azure.messaging.servicebus.ServiceBusReceivedMessageContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.function.Consumer;

@RestController
@RequestMapping("/servicebus/consumer")
public class ServiceBusConsumerController {

    private static final Logger LOG = LoggerFactory.getLogger(ServiceBusConsumerController.class);

    private static final String SAS_TOPIC_CONNECTION = "Endpoint=sb://my-messages.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=gVcd16UkTFNIeH5kG6mkC+VA+scqmbKUdAQeQsNI+9k=";
    private static final String TOPIC_NAME = "firsttopic";
    private static final String SUBSCRIPTION_NAME = "simple_subscription";
    private static final ServiceBusProcessorClient CONSUMER = createTopicConsumer();

    @PostMapping("/start")
    public void start() {
        LOG.info("Starting the consumer");
        CONSUMER.start();
    }

    @PostMapping("/stop")
    public void stop() {
        LOG.info("Stopping the consumer");
        CONSUMER.start();
    }

    private static ServiceBusProcessorClient createTopicConsumer() {
        return new ServiceBusClientBuilder()
                .connectionString(SAS_TOPIC_CONNECTION)
                .processor()
                .topicName(TOPIC_NAME)
                .subscriptionName(SUBSCRIPTION_NAME)
                .processMessage(processor())
                .processError(errorHandler())
                .disableAutoComplete()
                .buildProcessorClient();
    }

    private static Consumer<ServiceBusReceivedMessageContext> processor() {
        return messageContext -> {
            try {
                String id = messageContext.getMessage().getMessageId();
                String value = messageContext.getMessage().getBody().toString();
                LOG.info("Message received: [{}] with id: [{}]", value, id);
                messageContext.complete();
            } catch (Exception ex) {
                messageContext.abandon();
            }
        };
    }

    private static Consumer<ServiceBusErrorContext> errorHandler() {
        return errorContext ->
                LOG.error("Error occurred while receiving message: " + errorContext.getException());
    }
}
