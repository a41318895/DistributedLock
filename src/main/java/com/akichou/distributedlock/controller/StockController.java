package com.akichou.distributedlock.controller;

import com.akichou.distributedlock.service.StockService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@Slf4j
@RequestMapping("/stock")
public class StockController {

    private final StockService stockService ;

    @PostMapping(value = "/reduce")
    public void reduceStock(@RequestParam("productId") Long productId,
                              @RequestParam("quantity") int quantity) {

        boolean reduceResult = stockService.reduceStock(productId, quantity) ;

        if(reduceResult) {
            log.info("Stock reduced successfully for product: {}", productId) ;
        } else {
            log.error("Stock reduced failed for product: {}", productId) ;
        }
    }

    @PostMapping(value = "/reentrant")
    public void demonstrateReentrant(@RequestParam("productId") Long productId) {

        stockService.demonstrateReentrant(productId) ;

        log.info("Reentrant lock demonstration completed for product: {}", productId) ;
    }
}
