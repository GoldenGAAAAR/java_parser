package web_parser.ipiad.services.runnable;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import web_parser.ipiad.entity.NewsEntity;
import web_parser.ipiad.services.storage.ElasticsearchStorage;
import web_parser.ipiad.utils.Requests;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static java.nio.charset.StandardCharsets.UTF_8;

public class NewsLoader extends Thread {
    private final ConnectionFactory rmqFactory;
    private final ElasticsearchStorage elasticsearchStorage;
    private static final Logger logger = LoggerFactory.getLogger(NewsLoader.class);

    public NewsLoader(ConnectionFactory factory, ElasticsearchStorage elasticsearchStorage) {
        this.rmqFactory = factory;
        this.elasticsearchStorage = elasticsearchStorage;
    }

    @Override
    public void run() {
        try {
            Connection connection = rmqFactory.newConnection();
            Channel channel = connection.createChannel();
            logger.info("PageLoader connected to RabbitMQ");
            while (true) {
                // try to basic get
                GetResponse rmqResp = channel.basicGet(Requests.QUEUE_PAGE, true);
                if (rmqResp == null) {
                    continue;
                }

                String newsJson = new String(rmqResp.getBody(),UTF_8);
                NewsEntity newsEntity = new NewsEntity();
                logger.info("Got parsed data to insert" + newsJson);
                newsEntity.objectFromStrJson(newsJson);
                if (!elasticsearchStorage.checkExistence(newsEntity.getHash())) {
                    elasticsearchStorage.insertData(newsEntity);
                    logger.info("Inserted data from " + newsEntity.getURL() + " into Elastic");
                } else {
                    logger.info("[!] URL: " + newsEntity.getURL() + " was found in Elastic. Hash: " + newsEntity.getHash());
                }
            }
        } catch (IOException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}
