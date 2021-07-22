package ru.vtb.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import ru.vtb.producer.PartitionUtils;
import ru.vtb.common.Constants;
import ru.vtb.zf.dto.*;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static ru.vtb.common.Constants.DEALS_TOPIC;

public class Main {

    public static void main(String[] args) throws JsonProcessingException, ExecutionException, InterruptedException {
        final Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_SERVER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        try (var producer = new KafkaProducer<Long, String>(props)) {
            ObjectMapper mapper = getMapper();
            //setPartitions(3);
            for (int i = 0; i < 2_000_000; i++) {
                //var producerRecord = new ProducerRecord<Long, String>(Constants.TOPIC_NAME, random.nextInt(3) + 1, null, mapper.writeValueAsString(message));
                var producerRecord = new ProducerRecord<Long, String>(DEALS_TOPIC, mapper.writeValueAsString(getNewExchangeDeal()));
                Future<RecordMetadata> recordMetadataFuture = producer.send(producerRecord);
                RecordMetadata recordMetadata = recordMetadataFuture.get();
                System.out.println(recordMetadata.offset());
            }
        }

    }

    private static ObjectMapper getMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        return objectMapper;
    }


    static void setPartitions(int count) {
        PartitionUtils partitionUtils = new PartitionUtils();
        partitionUtils.setPartitions(count);
    }

    private static ExchangeDeal getNewExchangeDeal() {
        ExchangeDeal exchangeDeal = new ExchangeDeal();
        exchangeDeal.setGuid(UUID.randomUUID());
        exchangeDeal.setDealDateTime(LocalDateTime.now());
        DealType dealType = new DealType()
                .setCode("DealTypeCode")
                .setName("DealTypeName");
        exchangeDeal.setType(dealType);

        Account account = new Account();
        account.setGuid(UUID.randomUUID());
        exchangeDeal.setAccount(account);

        exchangeDeal.setDirection(DealDirection.B);
        Place place = new Place();
        place.setCode("PlaceCode");
        exchangeDeal.setPlace(place);

        TradeSession tradeSession = new TradeSession();
        tradeSession.setGuid(UUID.randomUUID());
        exchangeDeal.setTradeSession(tradeSession);

        ExchangeOrder order = new ExchangeOrder();
        order.setGuid(UUID.randomUUID());
        exchangeDeal.setOrder(order);


        AbstractInstrument instrument = new AbstractInstrument();
        instrument.setGuid(UUID.randomUUID());
        exchangeDeal.setInstrument(instrument);

        Currency currency = new Currency();
        currency.setGuid(UUID.randomUUID());
        exchangeDeal.setCurrency(currency);

        Random random = new Random();
        exchangeDeal.setQuantity(new BigDecimal(random.nextInt(20)));
        exchangeDeal.setPrice(new BigDecimal(Math.random()).setScale(2, RoundingMode.HALF_UP));

        exchangeDeal.setVolume(new BigDecimal(random.nextInt(40)));

        exchangeDeal.setCouponCurrency(currency);
        exchangeDeal.setCouponVolume(new BigDecimal(random.nextInt(100)));

        exchangeDeal.setPlanDeliveryDate(LocalDate.now().plusDays(2));
        exchangeDeal.setPlanPaymentDate(LocalDate.now().plusDays(10));

        return exchangeDeal;
    }

}
