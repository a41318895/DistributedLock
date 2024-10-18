package com.akichou.distributedlock;

import com.akichou.distributedlock.config.RedissonConfig;
import com.akichou.distributedlock.service.StockService;
import com.akichou.distributedlock.service.StockServiceImpl;
import lombok.RequiredArgsConstructor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.TestPropertySource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

@SpringBootTest(classes = {
        DistributedLockApplicationTests.TestConfig.class,
        RedissonConfig.class})
@TestPropertySource(properties = {
        "spring.data.redis.host=localhost",
        "spring.data.redis.port=6379"
})
@RequiredArgsConstructor
class DistributedLockApplicationTests {

    private static final Logger log = LoggerFactory.getLogger(DistributedLockApplicationTests.class);

    @Configuration
    static class TestConfig {

        // Offer instance of StockServiceImpl
        @Bean
        public StockService stockService(RedissonClient redissonClient) {

            return new StockServiceImpl(redissonClient) ;
        }
    }

    @Autowired
    private RedissonClient redissonClient ;

    private static final int SERVICE_INSTANCE_NUMBER = 5 ;   // Mock the distributed environment
    private static final int THREAD_NUMBER_PER_INSTANCE = 20 ;      // Concurrent requests from different service instances
    private static final int TOTAL_THREAD_NUMBER = THREAD_NUMBER_PER_INSTANCE * SERVICE_INSTANCE_NUMBER ;
    private static final int THREAD_NUMBER_UNDER_HIGH_CONCURRENCY_1000 = 1000 ;
    private static final int THREAD_NUMBER_UNDER_HIGH_CONCURRENCY_2000 = 2000 ;

    private static final Long PRODUCT_MOCK_ID = 1L ;
    private static final int QUANTITY_MOCK = 2 ;

    // Store all instances of StockServiceImpl
    private List<StockService> stockServices ;

    private final AtomicInteger requestSuccessCounterForAllInstances = new AtomicInteger(0) ;
    private final AtomicInteger requestSuccessCounterForFirstInstance = new AtomicInteger(0) ;
    private final AtomicInteger requestFailCounterForAllInstances = new AtomicInteger(0) ;
    private final AtomicInteger requestFailCounterForFirstInstance = new AtomicInteger(0) ;

    @BeforeEach
    public void setupInstances() {

        stockServices = new ArrayList<>(SERVICE_INSTANCE_NUMBER) ;

        for(int i = 0 ; i < SERVICE_INSTANCE_NUMBER ; i ++) {

            StockService stockService = new StockServiceImpl(redissonClient) ;  // All instances use the same RedissonClient instance
            stockServices.add(stockService) ;
            stockServices.get(i).initProductStock() ;
        }
    }

    @Test
    public void testReduceStockInDistributedEnv() throws InterruptedException {

        ExecutorService executorService = Executors.newFixedThreadPool(TOTAL_THREAD_NUMBER) ;
        CountDownLatch latch = new CountDownLatch(TOTAL_THREAD_NUMBER) ;

        for(int i = 0 ; i < TOTAL_THREAD_NUMBER * 2 ; i ++) {

            final int serviceIndex = i % SERVICE_INSTANCE_NUMBER ;      // Send request to instances in rotation to mock load balancing effect

            executorService.execute(() -> {

                try {

                    callReduceStockMethod(serviceIndex, PRODUCT_MOCK_ID, QUANTITY_MOCK) ;

                } finally {

                    latch.countDown() ;
                }
            });
        }

        latch.await() ;     // Wait for all threads finished their tasks
        executorService.shutdown() ;


        // Assertions : The times of reduceStock() method being called in all stockServices
        Assertions.assertEquals(TOTAL_THREAD_NUMBER, requestSuccessCounterForAllInstances.get(), "It should totally reduce stock of product 100 times.");
        log.info("Assertion passed: It actually reduced stock of product totally 100 times.") ;

        // Assertions : The times of reduceStock() method being called in stockServices[0]
        Assertions.assertEquals(THREAD_NUMBER_PER_INSTANCE, requestSuccessCounterForFirstInstance.get(), "It should reduce stock of product 20 times.");
        log.info("Assertion passed: It actually reduced stock of product 20 times.") ;

        // Assertions : The final product stock in every StockServiceImpl instance
        for(StockService stockService : stockServices) {

            Assertions.assertEquals(
                    0,
                    ((StockServiceImpl) stockService).getProductStockMockingMap().get(PRODUCT_MOCK_ID),
                    "The final product [ID=" + PRODUCT_MOCK_ID + "] stock quantity of every StockServiceImpl instance should be 0.") ;
        }
        log.info("Assertion passed: The final product stock quantity of every StockServiceImpl instance actually was 0.") ;
    }

    @Test
    public void testReduceStockWithInsufficientStockInFirstInstance() throws InterruptedException {

        final int REASSIGN_STOCK_QUANTITY = 10 ;
        final int QUANTITY_TRY_TO_REDUCE = 15 ;

        // Reassign stock quantity (40 -> 10)
        ((StockServiceImpl)stockServices.get(0)).getProductStockMockingMap().put(PRODUCT_MOCK_ID, REASSIGN_STOCK_QUANTITY) ;

        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_NUMBER_PER_INSTANCE);
        CountDownLatch latch = new CountDownLatch(THREAD_NUMBER_PER_INSTANCE);

        for (int i = 0 ; i < THREAD_NUMBER_PER_INSTANCE ; i ++) {

            executorService.execute(() -> {

                try {

                    // Try to reduce 15 stock quantity to trigger "The Stock is not enough for the product"
                    stockServices.get(0).reduceStock(PRODUCT_MOCK_ID, QUANTITY_TRY_TO_REDUCE) ;
                } finally {

                    latch.countDown();
                }
            });
        }

        latch.await();      // Wait for all threads finished their tasks
        executorService.shutdown();

        // Due to the failure of reducing operation, the final stock quantity should be still 10
        Assertions.assertEquals(
                REASSIGN_STOCK_QUANTITY,
                ((StockServiceImpl)stockServices.get(0)).getProductStockMockingMap().get(PRODUCT_MOCK_ID)) ;
    }

    @Test
    public void testPerformanceUnderHighConcurrency() throws InterruptedException {

        // Reassign stock quantity (40 -> 2000)
        int initialStockQuantity = THREAD_NUMBER_UNDER_HIGH_CONCURRENCY_1000 * QUANTITY_MOCK ;
        ((StockServiceImpl)stockServices.get(0)).getProductStockMockingMap().put(PRODUCT_MOCK_ID, initialStockQuantity);

        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_NUMBER_UNDER_HIGH_CONCURRENCY_1000);
        CountDownLatch latch = new CountDownLatch(THREAD_NUMBER_UNDER_HIGH_CONCURRENCY_1000);

        long startTime = System.currentTimeMillis();

        for(int i = 0 ; i < THREAD_NUMBER_UNDER_HIGH_CONCURRENCY_1000 ; i ++) {

            // Take the first instance in stockServices for doing high-concurrency-testing
            final int FIRST_INSTANCE_SERVICE_INDEX = 0;

            executorService.execute(() -> {

                try {

                    callReduceStockMethod(FIRST_INSTANCE_SERVICE_INDEX, PRODUCT_MOCK_ID, QUANTITY_MOCK);

                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executorService.shutdown();

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        int finalStockQuantity = ((StockServiceImpl)stockServices.get(0)).getProductStockMockingMap().get(PRODUCT_MOCK_ID);
        int totalQuantityReduced = initialStockQuantity - finalStockQuantity ;

        log.info("High Concurrency Test Results:");

        // Stock
        log.info("Initial stock quantity: {}", initialStockQuantity);
        log.info("Final stock quantity: {}", finalStockQuantity);
        log.info("Total quantity reduced: {}", totalQuantityReduced);

        // isSuccess
        log.info("Successful \"reduce stock\" operations: {}", requestSuccessCounterForAllInstances.get());
        log.info("Failed \"reduce stock\" operations: {}", requestFailCounterForAllInstances.get());

        // Take time
        log.info("Total duration: {} milliseconds", duration);

        Assertions.assertEquals(0, finalStockQuantity, "Final stock quantity should be 0");
        Assertions.assertEquals(THREAD_NUMBER_UNDER_HIGH_CONCURRENCY_1000, requestSuccessCounterForAllInstances.get(), "All operations should succeed");
        Assertions.assertTrue(duration < 2000, "Operation had better take less than 2000 ms") ;
    }

    private void callReduceStockMethod(int serviceInstanceIndex, Long productId, int quantity) {

        if(stockServices.get(serviceInstanceIndex).reduceStock(productId, quantity)) {

            if(serviceInstanceIndex == 0) requestSuccessCounterForFirstInstance.incrementAndGet() ;
            requestSuccessCounterForAllInstances.incrementAndGet() ;
        } else {

            if(serviceInstanceIndex == 0) requestFailCounterForFirstInstance.incrementAndGet() ;
            requestFailCounterForAllInstances.incrementAndGet() ;
        }
    }
}
