package web_parser.ipiad.utils;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

public class Requests {
    private static final int TIME_RETRY_MS = 3000;
    private static final int MAX_RETRY_ATTEMPTS = 3;
    public static final String QUEUE_LINK = "crawler_link";
    public static final String QUEUE_PAGE = "crawler_news";
    private static final Logger logger = LoggerFactory.getLogger(Requests.class);

    public static Optional<Document> requestWithRetry(String url) {
        Optional<Document> doc = Optional.empty();
        for (int attempt = 0; attempt < MAX_RETRY_ATTEMPTS; attempt++) {
            try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
                final HttpGet httpGet = new HttpGet(url);
                try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
                    int statusCode = response.getStatusLine().getStatusCode();
                    switch (statusCode) {
                        case 200: {
                            HttpEntity entity = response.getEntity();
                            if (entity != null) {
                                doc = Optional.ofNullable(Jsoup.parse(entity.getContent(), "UTF-8", url));
                                logger.info("[*] Thread ID: " + Thread.currentThread().getId() + " - Got page for: " + url);
                                return doc;
                            }
                            break;

                        }
                        case 403, 401, 429: {
                            logger.info("[*] Thread ID: " + Thread.currentThread().getId() +
                                    " - Error at " + url + " with status code " + statusCode);
                            response.close();
                            httpClient.close();
                            logger.info("[!] Got " + statusCode+ "code, sleep for " + TIME_RETRY_MS +" ms");
                            Thread.sleep(TIME_RETRY_MS);
                            break;
                        }
                        case 404:
                            logger.info("[*] Thread ID: " + Thread.currentThread().getId() + " - Received 404 for " + url);
                            break;
                        default:
                            break;
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return doc;
    }
}