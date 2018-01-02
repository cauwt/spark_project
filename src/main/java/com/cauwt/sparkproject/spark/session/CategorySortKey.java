package com.cauwt.sparkproject.spark.session;

import scala.Serializable;
import scala.math.Ordered;

/**
 * Created by zkpk on 12/28/17.
 */
public class CategorySortKey implements Ordered<CategorySortKey>, Serializable {

    private static final long serialVersionUID = -6951675551287510173L;

    private long clickCount;
    private long orderCount;
    private long payCount;

    public CategorySortKey(long clickCount, long orderCount, long payCount) {
        this.clickCount = clickCount;
        this.orderCount = orderCount;
        this.payCount = payCount;
    }

    public long getClickCount() {
        return clickCount;
    }

    public void setClickCount(long clickCount) {
        this.clickCount = clickCount;
    }

    public long getOrderCount() {
        return orderCount;
    }

    public void setOrderCount(long orderCount) {
        this.orderCount = orderCount;
    }

    public long getPayCount() {
        return payCount;
    }

    public void setPayCount(long payCount) {
        this.payCount = payCount;
    }

    @Override
    public int compare(CategorySortKey that) {
        if(this.clickCount - that.getClickCount() != 0){
            return Long.compare(this.clickCount, that.getClickCount());
        } else if (this.orderCount - that.getOrderCount() != 0){
            return Long.compare(this.orderCount, that.getOrderCount());
        } else if(this.payCount - that.getPayCount() != 0){
            return Long.compare(this.payCount, that.getPayCount());
        }
        return 0;
    }

    @Override
    public boolean $less(CategorySortKey that) {
        if(this.clickCount < that.getClickCount()){
            return true;
        }else if (this.clickCount == that.getClickCount()
                && this.orderCount < that.getOrderCount()){
            return true;
        } else if (this.clickCount == that.getClickCount()
                && this.orderCount == that.getOrderCount()
                && this.payCount < that.getPayCount()){
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater(CategorySortKey that) {
        if(this.clickCount > that.getClickCount()){
            return true;
        }else if (this.clickCount == that.getClickCount()
                && this.orderCount > that.getOrderCount()){
            return true;
        } else if (this.clickCount == that.getClickCount()
                && this.orderCount == that.getOrderCount()
                && this.payCount > that.getPayCount()){
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq(CategorySortKey that) {
        if($less(that)){
            return true;
        }else if (this.clickCount == that.getClickCount()
                && this.orderCount == that.getOrderCount()
                && this.payCount == that.getPayCount()){
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater$eq(CategorySortKey that) {
        if($greater(that)){
            return true;
        }else if (this.clickCount == that.getClickCount()
                && this.orderCount == that.getOrderCount()
                && this.payCount == that.getPayCount()){
            return true;
        }
        return false;
    }

    @Override
    public int compareTo(CategorySortKey that) {
        if(this.clickCount - that.getClickCount() != 0){
            return Long.compare(this.clickCount, that.getClickCount());
        } else if (this.orderCount - that.getOrderCount() != 0){
            return Long.compare(this.orderCount, that.getOrderCount());
        } else if(this.payCount - that.getPayCount() != 0){
            return Long.compare(this.payCount, that.getPayCount());
        }
        return 0;
    }
}
