package club.charliefeng.appengine.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class AppConfig {

    public static String stockTopic;

    @Value("${app.stock.pubsub.topic}")
    public void setStockTopic(String topic) {
        stockTopic = topic;
    }
}
