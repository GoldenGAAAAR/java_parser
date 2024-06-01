package web_parser.ipiad.services.runnable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.rabbitmq.client.*;
import web_parser.ipiad.entity.NewsEntity;
import web_parser.ipiad.entity.UrlEntity;
import web_parser.ipiad.services.storage.ElasticsearchStorage;
import web_parser.ipiad.utils.Requests;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

public class NewsParser extends Thread {
    private final ConnectionFactory connectionFactory;
    private static final Logger logger = LoggerFactory.getLogger(NewsParser.class);
    private final ElasticsearchStorage elasticsearchStorage;

    public NewsParser(ConnectionFactory factory, ElasticsearchStorage bridge) {
        this.connectionFactory = factory;
        this.elasticsearchStorage = bridge;
    }

    @Override
    public void run() {
        try {
            Connection connection = connectionFactory.newConnection();
            Channel channel = connection.createChannel();
            logger.info("Connected to RabbitMQ page queue for publishing");
            while (true) {
                try {
                    if (channel.messageCount(Requests.QUEUE_LINK) == 0) continue;
                    channel.basicConsume(Requests.QUEUE_LINK, false, "pagesTag", new DefaultConsumer(channel) {
                        @Override
                        public void handleDelivery(String consumerTag,
                                                   Envelope envelope,
                                                   AMQP.BasicProperties properties,
                                                   byte[] body)
                                throws IOException {
                            long deliveryTag = envelope.getDeliveryTag();
                            String message = new String(body, StandardCharsets.UTF_8);
                            UrlEntity url = new UrlEntity();
                            url.objectFromStrJson(message);
                            try {
                                parseAndPutToQueue(url, channel);
                            } catch (InterruptedException e) {
                                logger.info(e.getMessage());
                            }
                            channel.basicAck(deliveryTag, false);
                        }
                    });
                } catch (IndexOutOfBoundsException e) {
                    logger.info(e.getMessage());
                }
            }
        } catch (IOException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    void parseAndPutToQueue(UrlEntity url, Channel channel) throws InterruptedException, JsonProcessingException {
        if (elasticsearchStorage.checkExistence(url.getHash())) {
            logger.info("[!] URL: " + url.getUrl() + " was found in ElasticSearch. Hash: " + url.getHash());
            return;
        }
        String urlString = url.getUrl();
        Optional<Document> document = Requests.requestWithRetry(urlString);
        if (document.isPresent()) {
            Document doc = document.get();
            String header = doc.select("h1.di3-body__title").first().text();
            String time = doc.select("div.di3-meta__date_hidden-xs").text();
            String theme = "";
            Element themeElement = doc.select("a.di3-meta__topic-item").first();
            if (themeElement != null) {
                // это опциональное поле и не в каждой новости оно есть, но решил все же сохранять о нем информацию
                theme = themeElement.text();
            }

            StringBuilder textContent = new StringBuilder();
            Element divElement = doc.select("div.js-mediator-article").first();
            for (Element pElement : divElement.select("p")) {
                textContent.append(pElement.text()).append("\n");
            }

            NewsEntity news = new NewsEntity(
                    header,
                    textContent.toString(),
                    urlString,
                    time,
                    theme,
                    url.getHash()
            );
            logger.debug(news.toJsonString().toString());
            try {
                channel.basicPublish("", Requests.QUEUE_PAGE, null, news.toJsonString().getBytes());
                logger.info("Published page in the page queue");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
