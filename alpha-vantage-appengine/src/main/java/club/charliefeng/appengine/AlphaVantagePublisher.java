package club.charliefeng.appengine;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.gcp.pubsub.core.PubSubTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@RestController
public class AlphaVantagePublisher {

    public static void main(String[] args) {
        SpringApplication.run(AlphaVantagePublisher.class, args);
    }

    @GetMapping("/")
    public String hello() {return "Alpha Vantage App Engine Publisher"; }

}
