package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.Arrays;
import java.util.List;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    static class Bucket {
        public final int min;
        public int max;
        public int count;

        public Bucket(int min, int max, int count) {
            this.min = min;
            this.max = max;
            this.count = count;
        }

        @Override
        public String toString() {
            return "Bucket{" + "min=" + min + ", max=" + max + ", count=" + count + '}';
        }
    }

    private final List<Bucket> bucketList;
    private final int max;
    private final int min;
    private final double interval;
    private int count;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        if (buckets > (max - min + 1)) {
            buckets = max - min + 1;
        }
        this.min = min;
        this.max = max;
        this.interval = (max - min + 1) / (double) buckets;
        this.count = 0;

        Bucket[] temp = new Bucket[buckets];
        for (int i = 0; i < buckets; i++) {
            temp[i] = new Bucket(bucketBound(i), bucketBound(i + 1), 0);
        }
        temp[buckets - 1].max = max + 1;
        this.bucketList = Arrays.asList(temp);
    }

    private int bucketBound(int i) {
        return (int)(this.min + this.interval * i);
    }

    private int bucketIndex(int val) {
        return (int)((val - min) /this.interval);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        assert(v >= this.min && v <= this.max);
        int index = bucketIndex(v);
        this.bucketList.get(index).count++;
        this.count++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here

        int index = bucketIndex(v);
        if (op == Predicate.Op.EQUALS) {
            if (v < this.min || v > this.max) {
                return 0;
            }
            Bucket bucket = bucketList.get(index);
            return bucket.count / (double) (bucket.max - bucket.min) / this.count;

        } else if (op == Predicate.Op.NOT_EQUALS) {
            if (v < this.min || v > this.max) {
                return 1;
            }
            Bucket bucket = bucketList.get(index);
            return 1 - bucket.count / (double) (bucket.max - bucket.min) / this.count;
        } else if (op == Predicate.Op.GREATER_THAN) {
            if (v < this.min) {
                return 1;
            }
            if (v > this.max) {
                return 0;
            }
            int greatThanCount = 0;
            for (int i = index + 1; i < this.bucketList.size(); i++) {
                greatThanCount += bucketList.get(i).count;
            }
            Bucket bucket = bucketList.get(index);
            return ((bucket.max - v) * bucket.count / (double) (bucket.max - bucket.min) + greatThanCount) / this.count;
        } else if (op == Predicate.Op.GREATER_THAN_OR_EQ) {
            if (v <= this.min) {
                return 1;
            }
            if (v > this.max) {
                return 0;
            }
            int greatThanOrEqCount = 0;
            for (int i = index + 1; i < this.bucketList.size(); i++) {
                greatThanOrEqCount += bucketList.get(i).count;
            }
            Bucket bucket = bucketList.get(index);
            return ((bucket.max - v + 1) * bucket.count / (double) (bucket.max - bucket.min) + greatThanOrEqCount) / this.count;
        } else if (op == Predicate.Op.LESS_THAN) {
            if (v < this.min) {
                return 0;
            }
            if (v > this.max) {
                return 1;
            }
            int lessThanCount = 0;
            for (int i = index - 1; i >= 0; i--) {
                lessThanCount += bucketList.get(i).count;
            }
            Bucket bucket = bucketList.get(index);
            return ((v - bucket.min) * bucket.count / (double) (bucket.max - bucket.min) + lessThanCount) / this.count;
        } else if (op == Predicate.Op.LESS_THAN_OR_EQ) {
            if (v < this.min) {
                return 0;
            }
            if (v >= this.max) {
                return 1;
            }
            int lessThanOrEqCount = 0;
            for (int i = index - 1; i >= 0; i--) {
                lessThanOrEqCount += bucketList.get(i).count;
            }
            Bucket bucket = bucketList.get(index);
            return ((v - bucket.min + 1) * bucket.count / (double) (bucket.max - bucket.min) + lessThanOrEqCount) / this.count;
        } else {
            throw new UnsupportedOperationException("Unsupported Operation For IntField" + op);
        }
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("max:").append(this.max).append("\n");
        sb.append("min:").append(this.min).append("\n");
        sb.append("interval:").append(this.interval).append("\n");
        sb.append("count:").append(this.count).append("\n");
        for (int i = 0; i < bucketList.size(); i++) {
            sb.append("buckets[").append(i).append("]:").append(this.bucketList.get(i).toString()).append("\n");
        }
        return sb.toString();
    }
}
