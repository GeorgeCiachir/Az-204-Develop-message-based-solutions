package com.georgeciachir.storagequeue.producer;

import com.azure.storage.queue.QueueClient;
import com.azure.storage.queue.QueueClientBuilder;
import com.azure.storage.queue.models.QueueStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Objects;
import java.util.UUID;

import static java.util.Objects.requireNonNullElseGet;

@RestController
@RequestMapping("/storagequeue/producer")
public class StorageQueueProducerController {

    private static final Logger LOG = LoggerFactory.getLogger(StorageQueueProducerController.class);

    private static final String SAS_TOKEN = "?sv=2020-08-04&ss=q&srt=sco&sp=rwdlacup&se=2022-05-17T19:04:58Z&st=2022-04-17T11:04:58Z&spr=https&sig=nCuAS4jDbzVABcXbcKmgcgngdKiVaZsAir985wbhwqM%3D";
    private static final String QUEUE_ENDPOINT = "https://appmessagingstorage.queue.core.windows.net/";
    private static final String QUEUE_NAME = "storagequeue";

    private static final QueueClient STORAGE_ACCOUNT_QUEUE_PRODUCER = createStorageAccountQueueProducer();

    @PostMapping("/send")
    public void send(@RequestParam String message,
                     @RequestParam(required = false) String messageId) {
        LOG.info("Sending a message");

        try {
            String messageWithId;
            messageWithId = message + " ; id: " + requireNonNullElseGet(messageId, UUID::randomUUID);
            STORAGE_ACCOUNT_QUEUE_PRODUCER.sendMessage(messageWithId);
        } catch (QueueStorageException e) {
            LOG.error("Failed to create a queue with error code: " + e.getErrorCode());
        }
    }

    private static QueueClient createStorageAccountQueueProducer() {
        return new QueueClientBuilder()
                .endpoint(QUEUE_ENDPOINT)
                .sasToken(SAS_TOKEN)
                .queueName(QUEUE_NAME)
                .buildClient();

    }
}
