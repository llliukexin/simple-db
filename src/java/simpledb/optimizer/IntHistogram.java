package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.Arrays;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.
     * For example, you shouldn't simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    private int[] buckets;
    private int min;
    private int max;
    private int numBuckets;
    private int bucketWidth;
    private int ntups; // 记录元组总数


    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.numBuckets = buckets;
        this.min = min;
        this.max = max;
        this.buckets = new int[numBuckets];
        this.bucketWidth = (int) Math.ceil((double)(max-min+1)/numBuckets);
        this.ntups = 0;

    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        if (v > max || v < min || getBucketIndex(v) == 1)
            return;
        int index = getBucketIndex(v);
        buckets[index]++;
        ntups++;
    }

    public int getBucketIndex(int v){
        int index = (int) ((v - min) / bucketWidth);
        if(index < 0 || index >= buckets.length){
            return -1;
        }
        return index;
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
        if (v < min){
            if (op == Predicate.Op.GREATER_THAN || op == Predicate.Op.GREATER_THAN_OR_EQ ||
                    op == Predicate.Op.NOT_EQUALS)
                return 1.0;
            return 0.0;
        }
        if (v > max){
            if (op == Predicate.Op.LESS_THAN || op == Predicate.Op.LESS_THAN_OR_EQ ||
                    op == Predicate.Op.NOT_EQUALS)
                return 1.0;
            return 0.0;
        }

        double selectivity = 0.0;
        int index = getBucketIndex(v);
        double height = buckets[index];
        double width = bucketWidth;
        switch (op){
            case GREATER_THAN:
                if (v > max)
                    selectivity = 0.0;
                else if (v < min)
                    selectivity = 1.0;
                else {
                    double b_right = min + (index + 1)*width;
                    double b_f = height / ntups;
                    double b_part = (b_right - v) / width;
                    selectivity = b_f * b_part;
                    for (int i = index + 1;i < numBuckets;i++){
                        selectivity += (double) buckets[i] / ntups;
                    }
                }
                break;
            case GREATER_THAN_OR_EQ:
                selectivity = estimateSelectivity(Predicate.Op.GREATER_THAN, v)
                        + estimateSelectivity(Predicate.Op.EQUALS, v);
                break;
            case LESS_THAN:
                selectivity = 1.0 - estimateSelectivity(Predicate.Op.GREATER_THAN_OR_EQ, v);
                break;
            case LESS_THAN_OR_EQ:
                selectivity = estimateSelectivity(Predicate.Op.LESS_THAN, v + 1);
//                        + estimateSelectivity(Predicate.Op.EQUALS, v);
                break;
            case EQUALS:
                selectivity = height / width / ntups;
                break;
            case NOT_EQUALS:
                selectivity = 1.0 - estimateSelectivity(Predicate.Op.EQUALS, v);
                break;

        }
        if (selectivity < 0.0) return  0.0;
        else if (selectivity > 1.0) return 1.0;
        else return selectivity;
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
        double sum = 0;
        for (int bucket : buckets) {
            sum += (1.0 * bucket / ntups);
        }
        return sum / numBuckets;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return "IntHistogram{" +
                "buckets = " + Arrays.toString(buckets) +
                ", min = " + min +
                ", max =" + max +
                ", numBuckets=" + numBuckets +
                ", width =" + bucketWidth +
                ", totalCount=" + ntups +
                "}";
    }
}
