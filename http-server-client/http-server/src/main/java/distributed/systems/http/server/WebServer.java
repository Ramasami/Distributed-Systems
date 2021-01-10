package distributed.systems.http.server;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.concurrent.Executors;

public class WebServer {
    private static final String TASK_ENDPOINT = "/task";
    private static final String STATUS_ENDPOINT = "/status";

    private final int port;

    public static void main(String[] args) {
        int serverPort = 8081;
        if (args.length == 1) {
            serverPort = Integer.parseInt(args[0]);
        }
        WebServer webServer = new WebServer(serverPort);
        webServer.startServer();
    }

    public WebServer(int port) {
        this.port = port;
    }

    public void startServer() {
        HttpServer server;
        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        HttpContext statusContext = server.createContext(STATUS_ENDPOINT);
        HttpContext taskContext = server.createContext(TASK_ENDPOINT);

        statusContext.setHandler(this::handleStatusCheckRequest);
        taskContext.setHandler(this::handleTaskCheckRequest);

        server.setExecutor(Executors.newFixedThreadPool(8));
        server.start();
    }

    private void handleTaskCheckRequest(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equalsIgnoreCase("post")) {
            exchange.close();
            return;
        }

        Headers headers = exchange.getRequestHeaders();
        if (headers.containsKey("X-Test") && headers.get("X-Test").get(0).equalsIgnoreCase("true")) {
            String dummyResponse = "dummy";
            sendResponse(dummyResponse.getBytes(), exchange);
            return;
        }

        boolean isDebugMode = false;
        if (headers.containsKey("X-Debug") && headers.get("X-Debug").get(0).equalsIgnoreCase("true")) {
            isDebugMode = true;
        }
        long startTime = System.nanoTime();

        byte[] requestBytes = new byte[exchange.getRequestBody().available()];
        exchange.getRequestBody().read(requestBytes);
        byte[] responseBytes = calculateResponse(requestBytes);

        long finishedTime = System.nanoTime();

        if (isDebugMode) {
            String debugMessage = String.format("Operation took %d ns \n", finishedTime - startTime);
            exchange.getResponseHeaders().put("X-Debug-Info", Collections.singletonList(debugMessage));
        }

        sendResponse(responseBytes, exchange);
    }

    private byte[] calculateResponse(byte[] requestBytes) {
        String bodyString = new String(requestBytes);
        String[] stringNumber = bodyString.split(",");

        BigInteger result = BigInteger.ONE;

        for (String number : stringNumber) {
            BigInteger bigInteger = new BigInteger(number);
            result = result.multiply(bigInteger);
        }
        return result.toString().getBytes();
    }

    private void handleStatusCheckRequest(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equalsIgnoreCase("get")) {
            exchange.close();
            return;
        }
        String responseMessage = "Server is alive";
        sendResponse(responseMessage.getBytes(), exchange);
    }

    private void sendResponse(byte[] responseBytes, HttpExchange exchange) throws IOException {
        exchange.sendResponseHeaders(200, responseBytes.length);
        OutputStream outputStream = exchange.getResponseBody();
        outputStream.write(responseBytes);
        outputStream.flush();
        outputStream.close();
    }


}
