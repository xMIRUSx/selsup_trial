package org.example;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.net.URIBuilder;

import java.io.IOException;
import java.net.*;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.*;
import java.util.concurrent.Semaphore;

@Slf4j
public abstract class CrptApi {

    private static final long TOKEN_LIFESPAN_MINUTES = 10*60;

    protected String authToken;
    protected Instant lastTokenRequest;
    protected int requestsPerInterval;
    protected SemaphoreWithTimer semaphore;
    protected ObjectMapper objectMapper;
    protected Map<String, String> methodUrlMap;

    public CrptApi(TemporalUnit interval, int requestsPerInterval, Map<String, String> methodUrlMap) {
        if (requestsPerInterval < 1) {
            throw new IllegalArgumentException("Кол-во запросов должно быть больше 0.");
        }
        this.requestsPerInterval = requestsPerInterval;
        this.methodUrlMap = methodUrlMap;
        this.objectMapper = new ObjectMapper();
        this.semaphore = new SemaphoreWithTimer(requestsPerInterval, interval.getDuration().toMillis());
    }

    // В ТЗ не указано, откуда берётся токен.
    // Реализовывать его получение не надо, поэтому сделаем тут заглушку.
    abstract public String getAuthToken();

    /*
        по документации не понятно, отличаются ли по параметрам запросы
        /api/v3/lk/documents/create и /api/v3/lk/documents/commissioning/contract/create
        будем считать, что не отличаются.
        Подробно описан только /api/v3/lk/documents/create, делаем запрос согласно его описанию.
     */
    public CreateDocResponse introduceGoods(IntroduceGoodsDto doc, CreateDocRequest.ProductGroup pg, String signature) throws JsonProcessingException, HttpRequestException {

        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            log.warn("Прерван поток с ожидающим выполнения запросом.", e);
            return null;
        }
        String requestBody;
        var params = new HashMap<String, String>();
        params.put("pg", pg.name().toLowerCase());
        String docStr = objectMapper.writeValueAsString(doc);
        var request = CreateDocRequest.builder()
                .documentFormat(CreateDocRequest.DocumentFormat.MANUAL)
                .productDocument(docStr)
                .productGroup(pg)
                .signature(signature)
                .type(CreateDocRequest.DocumentType.LP_INTRODUCE_GOODS)
                .build();
        requestBody = objectMapper.writeValueAsString(request);

        String responseStr = callApi(methodUrlMap.get("introduceGoods"), params, requestBody);
        return objectMapper.readValue(responseStr, CreateDocResponse.class);
    }

    private String callApi(String urlStr, Map<String, String> params, String body) throws HttpRequestException {
        updateAuthToken();

        URI uri;
        try {
            var uriBuilder = new URIBuilder(urlStr);
            for (var entry : params.entrySet()) {
                uriBuilder.addParameter(entry.getKey(), entry.getValue());
            }
            uri = uriBuilder.build();
        } catch (URISyntaxException e) {
            throw new HttpRequestException("Ошибка при формировании URL.", e);
        }

        String responseBody;
        int responseCode;
        try (var httpClient = HttpClients.createDefault()) {

            var postRequest = new HttpPost(uri);
            postRequest.addHeader("Content-Type", "application/json");
            postRequest.addHeader("Authorization", "Bearer " + authToken);
            postRequest.setEntity(new StringEntity(body));

            try (var response = httpClient.execute(postRequest, classicHttpResponse -> classicHttpResponse)) {
                responseBody = new String(response.getEntity().getContent().readAllBytes());
                responseCode = response.getCode();
            } catch (IOException e) {
                throw new HttpRequestException("Ошибка при чтении ответа от API.", e);
            }
        } catch(IOException e) {
            throw new HttpRequestException("Ошибка при обращении к API.", e);
        }

        if (responseCode != 200) {
            throw new HttpRequestException(String.format("Неожиданный ответ от API. %d : %s", responseCode, responseBody));
        }
        return responseBody;
    }

    private void updateAuthToken() {
        if (lastTokenRequest == null || lastTokenRequest.plus(TOKEN_LIFESPAN_MINUTES, ChronoUnit.SECONDS).isBefore(Instant.now())) {
            authToken = getAuthToken();
            lastTokenRequest = Instant.now();
        }
    }

    /**
     * Семафор, который очищается по таймеру.
     * Метод release отсутствует, чтобы не превысить лимит запросов в единицу времени.
     */
    @Slf4j
    public static class SemaphoreWithTimer {

        Semaphore semaphore;
        long period;

        Timer timer;

        public SemaphoreWithTimer(int limit, long period) {
            this.semaphore = new Semaphore(limit);
            this.period = period;
            this.timer = new Timer("Clear api queue");
            TimerTask tt = new TimerTask() {
                @Override
                public void run() {
                    log.info("Clearing api queue");
                    semaphore.release(semaphore.getQueueLength());
                }
            };
            timer.scheduleAtFixedRate(tt, 0, period);
        }

        public void acquire() throws InterruptedException {
            semaphore.acquire();
        }

    }

    public static class HttpRequestException extends Exception {
        public HttpRequestException() {
            super();
        }

        public HttpRequestException(String message) {
            super(message);
        }

        public HttpRequestException(String message, Throwable cause) {
            super(message, cause);
        }

        public HttpRequestException(Throwable cause) {
            super(cause);
        }
    }

    //Далее идут DTO и перечисления

    @Getter
    @Setter
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static class IntroduceGoodsDto {
        private Description description;
        private String docId;
        private String docStatus;
        private String docType;
        @JsonAlias("importRequest")
        private Boolean importRequest;
        private String ownerInn;
        private String participantInn;
        private String producerInn;
        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd", timezone = "Europe/Moscow")
        private Date productionDate;
        private String productionType;
        private List<Product> products;
        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd", timezone = "Europe/Moscow")
        private Date regDate;
        private String regNumber;

        @Getter
        @Setter
        public static class Description {
            private String participantInn;
        }

        @Getter
        @Setter
        @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
        public static class Product {
            private String certificateDocument;
            @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd", timezone = "Europe/Moscow")
            private Date certificateDocumentDate;
            private String certificateDocumentNumber;
            private String ownerInn;
            private String producerInn;
            @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd", timezone = "Europe/Moscow")
            private Date productionDate;
            private String tnvedCode;
            private String uitCode;
            private String uituCode;

        }
    }

    @Getter
    @Setter
    @Builder
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CreateDocRequest {

        DocumentFormat documentFormat;
        String productDocument;
        ProductGroup productGroup;
        String signature;
        DocumentType type;

        public enum DocumentFormat {
            MANUAL,
            XML,
            CSV;
        }

        public enum DocumentType {
            LP_INTRODUCE_GOODS,
            LP_INTRODUCE_GOODS_CSV,
            LP_INTRODUCE_GOODS_XML;
        }

        public enum ProductGroup {
            CLOTHES(1, "Предметы одежды, белье постельное, столовое, туалетное и кухонное"),
            SHOES(2, "Обувные товары"),
            TOBACCO(3, "Табачная продукция"),
            PERFUMERY(4, "Духи и туалетная вода"),
            TIRES(5, "Шины и покрышки пневматические резиновые новые"),
            ELECTRONICS(6, "Фотокамеры (кроме кинокамер), фотовспышки и лампы-вспышки"),
            PHARMA(7, "Лекарственные препараты для медицинского применения"),
            MILK(8, "Молочная продукция"),
            BICYCLE(9, "Велосипеды и велосипедные рамы"),
            WHEELCHAIRS(10, "Кресла-коляски");

            private final int code;
            private final String description;

            ProductGroup(int code, String description) {
                this.code = code;
                this.description = description;
            }

            public int getCode() {
                return code;
            }

            public String getDescription() {
                return description;
            }

            @JsonCreator
            public static ProductGroup fromCode(int code) {
                var result = Arrays.stream(ProductGroup.values()).filter(
                            pg -> pg.getCode() == code
                        )
                        .findFirst();
                return result.get();
            }
        }
    }

    @Getter
    @Setter
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static class CreateDocResponse {
        private String value;
        private String code;
        private String errorMessage;
        private String description;
    }
}
