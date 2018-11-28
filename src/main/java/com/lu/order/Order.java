package com.lu.order;

import java.util.ArrayList;
import java.util.List;

import com.lu.util.concurrent.CannotExecuteOrderException;

public class Order {
    int customerId;
    List<Integer> products;
    
    public Order(int customerId)
    {
        this.customerId = customerId;
        this.products = new ArrayList<>();
    }

    // For the benefit of easy JSON serialization
    public Order()
    {
    }

    public void AddEntry(int sku)
    {
        products.add(sku);
    }

    public int GetNumberOfEntries()
    {
        return products.size();
    }

    public int getCustomerId()
    {
        return customerId;
    }
    
    public List<Integer> getProducts() {
		return products;
	}
    
    public void validate() {
        if (customerId < 0) {
            throw new CannotExecuteOrderException("customerId must be positive: " + customerId, null);
        }
    }
}
