package com.akichou.distributedlock.service;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
@Slf4j
public class StockServiceImpl implements StockService{

    private final RedissonClient redissonClient ;

    // Mocking data
    @Getter
    private final Map<Long, Integer> productStockMockingMap = new ConcurrentHashMap<>() ;

    // Open to Unit Test to init stock quantity
    @Override
    public void initProductStock() {

        productStockMockingMap.put(1L, 40) ;
        productStockMockingMap.put(2L, 60) ;
    }

    @Override
    public boolean reduceStock(Long productId, int quantity) {

        RLock distributedLock = getDistributedLock(productId) ;

        try {

            // Redisson Distributed Lock Feature 1 - Avoid DeadLock
            // Set the maximum waiting time (5 sec) to get lock.
            // If the current thread can't get lock within 5 sec, it will return false.

            // Redisson Distributed Lock Feature 2 - Automatic Renewal
            // Set the time (30 sec) to hold lock.
            // If the thread which is holding lock has not finished the task,
            // Redisson will automatically prolong the time (30 sec each time).
            boolean isLocked = distributedLock.tryLock(5, 30, TimeUnit.SECONDS) ;

            if(isLocked) {

                try {

                    // Redisson Distributed Lock Feature 3 - Reentrant
                    // Even though the current thread is holding lock, it still can call lock() again.
                    return processStockReduction(productId, quantity);
                } finally {

                    // Redisson Distributed Lock Feature 4 - Accidental Deletion Prevention
                    // Only the thread which is holding lock can unlock.
                    if (distributedLock.isHeldByCurrentThread()) distributedLock.unlock() ;
                }

            } else {

                log.error("Failed to get lock for product: {}", productId) ;
                return false ;
            }

        } catch (InterruptedException e) {

            Thread.currentThread().interrupt() ;
            return false ;
        }
    }

    private boolean processStockReduction(Long productId, int quantity) {

        // Redisson Distributed Lock Feature 3 - Reentrant
        // Redisson admits the same thread can get the same lock many times.
        RLock distributedLock = getDistributedLock(productId) ;

        try {
            // Mock the situation for Redisson Distributed Lock Feature 3 : call lock() again
            distributedLock.lock() ;

            log.info("Reducing the stock for product: {} by {}", productId, quantity) ;

            // Stock Decrease Logic
            productStockMockingMap.compute(productId, (key, value) -> {

                // Handle "has not this id"
                if(value == null) {
                    throw new IllegalArgumentException("No stock found for product: " + productId) ;
                }

                // Handle "stock not enough"
                if(value < quantity) {
                    throw new IllegalArgumentException("The Stock is not enough for product: " + productId) ;
                }

                return value - quantity ;
            }) ;

            log.info("Reduced stock for product: {} by {} successfully", productId, quantity) ;

            return true ;
        } finally {

            distributedLock.unlock() ;
        }
    }

    @Override
    public void demonstrateReentrant(Long productId) {

        RLock distributedLock = getDistributedLock(productId) ;

        try {

            distributedLock.lock();
            log.info("First lock acquired...") ;

            distributedLock.lock();
            log.info("Second lock acquired (reentrant)...") ;

        } finally {

            // Need to unlock 2 times(equals the times of locking) here

            distributedLock.unlock();
            log.info("First lock released...") ;

            distributedLock.unlock();
            log.info("Second lock released...") ;
        }
    }

    private RLock getDistributedLock(Long productId) {

        String distributedLockKey = "distributed:lock:product:" + productId ;

        return redissonClient.getLock(distributedLockKey) ;
    }
}
