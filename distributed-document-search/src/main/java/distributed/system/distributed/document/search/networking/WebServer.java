package distributed.system.distributed.document.search.networking;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.concurrent.Executors;

public class WebServer {
    private static final String STATUS_ENDPOINT = "/status";

    private final int port;
    private final OnRequestCallBack onRequestCallBack;
    private HttpServer server;

    public WebServer(int port, OnRequestCallBack onRequestCallBack) {
        this.port = port;
        this.onRequestCallBack = onRequestCallBack;
    }

    public void startServer() {
        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        HttpContext statusContext = server.createContext(STATUS_ENDPOINT);
        HttpContext taskContext = server.createContext(onRequestCallBack.getEndPoint());

        statusContext.setHandler(this::handleStatusCheckRequest);
        taskContext.setHandler(this::handleTaskRequest);

        server.setExecutor(Executors.newFixedThreadPool(8));
        server.start();
    }

    private void handleTaskRequest(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equalsIgnoreCase("post")) {
            exchange.close();
            return;
        }

        byte [] responseBytes = onRequestCallBack.handleRequest(readAllBytes(exchange.getRequestBody()));
        sendResponse(responseBytes, exchange);
    }

    private byte[] readAllBytes(InputStream requestBody) throws IOException {
        byte message[] = new byte[requestBody.available()];
        requestBody.read(message);
        return message;
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


    public void stop() {
        server.stop(0);
    }
}
