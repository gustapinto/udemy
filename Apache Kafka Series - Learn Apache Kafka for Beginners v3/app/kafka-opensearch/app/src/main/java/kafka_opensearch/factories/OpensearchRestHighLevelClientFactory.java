package kafka_opensearch.factories;

import java.net.URI;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;

public class OpensearchRestHighLevelClientFactory {
    public static RestHighLevelClient make(String connString) {
        URI connUri = URI.create(connString);
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // Conecta em um cliente HTTP
            HttpHost httpHost = new HttpHost(connUri.getHost(), connUri.getPort(), "http");
            RestClientBuilder restClientBuilder = RestClient.builder(httpHost);

            // Cria um cliente REST a partir de uma url "aberta"
            return new RestHighLevelClient(restClientBuilder);
        }

        // Conecta em um client HTTPS ou com login
        String[] auth = userInfo.split(":");
        String username = auth[0];
        String password = auth[1];

        Credentials credentials = new UsernamePasswordCredentials(username, password);
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, credentials);

        HttpHost httpHost = new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme());
        RestClientBuilder restClientBuilder = RestClient.builder(httpHost)
                .setHttpClientConfigCallback(builder -> builder
                        .setDefaultCredentialsProvider(credentialsProvider)
                        .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy()));

        return new RestHighLevelClient(restClientBuilder);
    }
}
