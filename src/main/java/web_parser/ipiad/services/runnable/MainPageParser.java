package web_parser.ipiad.services.runnable;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import web_parser.ipiad.entity.UrlEntity;
import web_parser.ipiad.utils.Requests;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

public class MainPageParser extends Thread {
    private final String initialLink;
    private final ConnectionFactory connectionFactory;
    private static final Logger logger = LoggerFactory.getLogger(MainPageParser.class);

    public MainPageParser(String initialLink, ConnectionFactory factory) {
        this.initialLink = initialLink;
        this.connectionFactory = factory;
    }

    @Override
    public void run() {
        try {
            Connection connection = connectionFactory.newConnection();
            Channel channel = connection.createChannel();
            logger.info("Connected to RabbitMQ link queue");
            crawlAndExtractLinks(initialLink, channel);
            channel.close();
            connection.close();
        } catch (IOException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    private void crawlAndExtractLinks(String url, Channel channel) throws IOException {
        logger.info("Starting parsing: " + url);
        String baseUrl = "https://newslab.ru";
        Optional<Document> documentOpt = Requests.requestWithRetry(url);
        if (documentOpt.isPresent()) {
            Document originalDocument = documentOpt.get();
            for (Element publicationItem : originalDocument.select("li.n-list__item")) {
                Element anchorElement = publicationItem.children().select("a.n-list__item__link").first();
                if (anchorElement == null) {
                    continue; // иногда могут быть путстые list-item на странице
                }

                try {
                    String href = baseUrl + anchorElement.attributes().get("href");
                    String title = anchorElement.text();
                    UrlEntity extractedUrl = new UrlEntity(href, title);
                    logger.debug(extractedUrl.toJsonString());
                    channel.basicPublish("", Requests.QUEUE_LINK, null, extractedUrl.toJsonString().getBytes());
                } catch (Exception e) {
                    System.out.println(e);
                    logger.info(e.getMessage());
                }
            }
        }
        logger.info("Finished parsing:" + url);
    }
}