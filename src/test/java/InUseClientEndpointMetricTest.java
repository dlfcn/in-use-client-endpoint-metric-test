
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.dropwizard.DropwizardMetricsOptions;
import io.vertx.ext.dropwizard.Match;
import io.vertx.ext.dropwizard.MetricsService;
import io.vertx.ext.dropwizard.impl.VertxMetricsFactoryImpl;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class InUseClientEndpointMetricTest {

    private static final String LOCALHOST = "localhost";
    private static final int PORT = getFreePort();
    private static Vertx VERTX = null;

    @Test
    public void resetStreamTest() throws InterruptedException, ExecutionException, TimeoutException {

        AtomicBoolean sendResponseFlag = new AtomicBoolean(true);

        HttpServer httpServer = VERTX.createHttpServer()
                .requestHandler(handler -> {
                    if (sendResponseFlag.get()) {
                        handler.response().end();
                    } else {
                        // do not respond
                    }
                });

        httpServer.listen(PORT, LOCALHOST)
                .toCompletionStage()
                .toCompletableFuture()
                .get(5, TimeUnit.SECONDS);

        MetricsService metricsService = MetricsService.create(VERTX);
        String metricsName = "resetStreamTest";

        WebClientOptions clientOptions = new WebClientOptions();
        clientOptions.setMetricsName(metricsName);

        WebClient client = WebClient.create(VERTX, clientOptions);
        HttpRequest<Buffer> request = client.get(PORT, LOCALHOST, "/reset/stream/test");
        request.timeout(2_000);

        // assert there are no endpoint metrics
        assertInUseCount(metricsService, metricsName, -1L);

        // send request and assert OK response
        HttpResponse<Buffer> response = request.send()
                .toCompletionStage()
                .toCompletableFuture()
                .get(5, TimeUnit.SECONDS);

        // sanity check
        assertEquals(response.statusCode(), 200);

        // assert zero streams in-use
        assertInUseCount(metricsService, metricsName, 0L);

        // send request that is expected to timeout triggering stream reset
        try {
            sendResponseFlag.set(false);

            request.send()
                    .toCompletionStage()
                    .toCompletableFuture()
                    .get(5, TimeUnit.SECONDS);

            fail("Request timeout expected");
        } catch (ExecutionException ex) {
            assertTrue(ex.getMessage().contains("timeout"));
        }

        // assert in-use counter has been decremented back to zero
        // this assert will fail with an actual in-use value of 2
        // the in-use counter was incremented once when the request was sent
        // and incremented again due to timeout and request reset logic
        assertInUseCount(metricsService, metricsName, 0L);
    }

    private static void assertInUseCount(MetricsService metricsService, String metricsName, Long expectedCount) {

        JsonObject metricsSnapshot = metricsService.getMetricsSnapshot(VERTX);
        assertNotNull(metricsSnapshot);
        assertFalse(metricsSnapshot.isEmpty());

        String fieldName = String.format("vertx.http.clients.%s.endpoint.%s:%s.in-use", metricsName, LOCALHOST, PORT);
        if (expectedCount == -1) {
            assertFalse(metricsSnapshot.containsKey(fieldName));
        } else {
            assertTrue(metricsSnapshot.containsKey(fieldName));
            JsonObject inUseCounter = metricsSnapshot.getJsonObject(fieldName);
            assertEquals(inUseCounter.getLong("count"), expectedCount);
        }
    }

    @BeforeClass
    public static void setUpClass() throws Exception {

        DropwizardMetricsOptions metricsOptions = new DropwizardMetricsOptions();
        metricsOptions.setEnabled(true);
        metricsOptions.addMonitoredHttpClientEndpoint(new Match().setValue(String.format("%s:%s", LOCALHOST, PORT)));
        metricsOptions.setFactory(new VertxMetricsFactoryImpl());

        VertxOptions vertxOptions = new VertxOptions();
        vertxOptions.setMetricsOptions(metricsOptions);

        VERTX = Vertx.vertx(vertxOptions);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        VERTX.close();
    }

    private static int getFreePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        } catch (IOException ex) {
            throw new RuntimeException("Error getting free port", ex);
        }
    }
}
