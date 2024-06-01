package web_parser.ipiad.run;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import web_parser.ipiad.services.runnable.MainPageParser;
import web_parser.ipiad.services.runnable.NewsLoader;
import web_parser.ipiad.services.runnable.NewsParser;
import web_parser.ipiad.services.storage.ElasticsearchStorage;
import web_parser.ipiad.utils.Requests;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;


public class Main {
    private static final String url = "https://newslab.ru/news/";
    private static final String INDEX_NAME = "news";
    private static final String EL_URL = "http://localhost:9200";
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    public static void main(String[] args) throws InterruptedException, IOException, TimeoutException {
        logger.info("Start service");
        ConnectionFactory rmqConFactory = new ConnectionFactory();
        rmqConFactory.setHost("127.0.0.1");
        rmqConFactory.setPort(5672);
        rmqConFactory.setVirtualHost("/");
        rmqConFactory.setUsername("rabbitmq");
        rmqConFactory.setPassword("rabbitmq");

        Connection connection = rmqConFactory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(Requests.QUEUE_LINK, false, false, false, null);
        channel.queueDeclare(Requests.QUEUE_PAGE, false, false, false, null);
        channel.close();
        connection.close();

        ElasticsearchStorage elasticsearchStorage = new ElasticsearchStorage(EL_URL, INDEX_NAME);
        elasticsearchStorage.createIndexIfNotExists();

        // запускаем парсинг главной страницы, этот поток необходимо запускать
        // только в одном экземпляре ( иначе не избежать дублирования при покладке тасков с ссылками ) или
        // можно использовать кэш ( чтобы синкать эти потоки), но кажется это лишнее
        MainPageParser mainPageParser = new MainPageParser(url, rmqConFactory);
        mainPageParser.start();

        // запускаем получения страниц по их ссылкам и парсинг новости находящейся на странице ( 2 потока консюмера )
        NewsParser newsParserThread1 = new NewsParser(rmqConFactory, elasticsearchStorage);
        NewsParser newsParserThread2 = new NewsParser(rmqConFactory, elasticsearchStorage);
        newsParserThread1.start();
        newsParserThread2.start();

        // запускаем получение результатов парсинга и сохранение их в бд (elascticsearch)
        NewsLoader newsLoaderThread1 = new NewsLoader(rmqConFactory, elasticsearchStorage);
        NewsLoader newsLoaderThread2 = new NewsLoader(rmqConFactory, elasticsearchStorage);
        newsLoaderThread1.start();
        newsLoaderThread2.start();

        // ожидание пока все потоки отдадут управление
        mainPageParser.join();

        newsParserThread1.join();
        newsParserThread2.join();

        newsLoaderThread1.join();
        newsLoaderThread2.join();
    }

}