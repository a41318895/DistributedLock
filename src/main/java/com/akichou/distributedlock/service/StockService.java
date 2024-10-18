package com.akichou.distributedlock.service;

public interface StockService {

    void initProductStock() ;

    boolean reduceStock(Long productId, int quantity);

    void demonstrateReentrant(Long productId);
}
