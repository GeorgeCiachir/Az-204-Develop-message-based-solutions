package com.georgeciachir.storagequeue.consumer;

import com.azure.storage.queue.QueueClient;
import com.azure.storage.queue.QueueClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/storagequeue/consumer")
public class StorageQueueConsumerController {

    private static final Logger LOG = LoggerFactory.getLogger(StorageQueueConsumerController.class);

    private static final String SAS_TOKEN = "?sv=2020-08-04&ss=q&srt=sco&sp=rwdlacup&se=2022-05-17T19:04:58Z&st=2022-04-17T11:04:58Z&spr=https&sig=nCuAS4jDbzVABcXbcKmgcgngdKiVaZsAir985wbhwqM%3D";
    private static final String QUEUE_ENDPOINT = "https://appmessagingstorage.queue.core.windows.net/";
    private static final String QUEUE_NAME = "storagequeue";

    private static final QueueClient STORAGE_ACCOUNT_QUEUE_CONSUMER = createStorageAccountQueueConsumer();

    @PostMapping("/pull")
    public String pull(@RequestParam(required = false) Integer numberOfMessages) {
        LOG.info("Pulling messages");

        List<String> messages;
        if (numberOfMessages != null) {
            messages = STORAGE_ACCOUNT_QUEUE_CONSUMER.receiveMessages(numberOfMessages)
                    .stream()
                    .map(item -> item.getBody().toString())
                    .collect(Collectors.toList());
        } else {
            String singleValue = STORAGE_ACCOUNT_QUEUE_CONSUMER.receiveMessage().getBody().toString();
            messages = List.of(singleValue);
        }

        return "Messages: " + messages;

    }

    private static QueueClient createStorageAccountQueueConsumer() {
        return new QueueClientBuilder()
                .endpoint(QUEUE_ENDPOINT)
                .sasToken(SAS_TOKEN)
                .queueName(QUEUE_NAME)
                .buildClient();
    }
}
