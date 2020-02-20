package spark.session;
import scala.math.Ordered;
import java.io.Serializable;

/**
 * 二次排序key
 */
public class CategorySortKey implements Ordered<CategorySortKey>, Serializable {

    private static final long serialVersionUID = 9186364039373791258L;
    //主要以clickCount排序，如果相等，排序优先级别：clickCount -> orderCount -> payCount
    private long clickCount;
    private long orderCount;
    private long payCount;

    public CategorySortKey(long clickCount, long orderCount, long payCount) {
        this.clickCount = clickCount;
        this.orderCount = orderCount;
        this.payCount = payCount;
    }

    @Override
    public boolean $less(CategorySortKey other) {
        if(clickCount < other.getClickCount()) {
            return true;
        } else if(clickCount == other.getClickCount() &&
                orderCount < other.getOrderCount()) {
            return true;
        } else return clickCount == other.getClickCount() &&
                orderCount == other.getOrderCount() &&
                payCount < other.getPayCount();
    }

    @Override
    public boolean $greater(CategorySortKey other) {
        if (clickCount > other.getClickCount()){
            return true;
        }else if (clickCount == other.getClickCount() && orderCount > other.getOrderCount()){
            return true;
        }else return clickCount == other.getClickCount() && orderCount == other.getOrderCount()
                && payCount > other.getPayCount();
    }

    @Override
    public boolean $less$eq(CategorySortKey other) {
        if($less(other)) {
            return true;
        } else return clickCount == other.getClickCount() &&
                orderCount == other.getOrderCount() &&
                payCount == other.getPayCount();
    }

    @Override
    public boolean $greater$eq(CategorySortKey other) {
        if ($greater(other)){
            return true;
        }else return clickCount == other.getClickCount() &&
                orderCount == other.getOrderCount() &&
                payCount == other.getPayCount();
    }

    @Override
    public int compare(CategorySortKey other) {
        if(clickCount - other.getClickCount() != 0) {
            return (int) (clickCount - other.getClickCount());
        } else if(orderCount - other.getOrderCount() != 0) {
            return (int) (orderCount - other.getOrderCount());
        } else if(payCount - other.getPayCount() != 0) {
            return (int) (payCount - other.getPayCount());
        }
        return 0;
    }

    @Override
    public int compareTo(CategorySortKey other) {
        return compare(other);
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
}
