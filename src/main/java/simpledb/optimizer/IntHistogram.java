package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.*;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    private final int min_value;
    private final int max_value;
    private final int bucker_width;
    private int num_tuples;
    private final ArrayList<Integer> buckets;
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
    public IntHistogram(int bucket_num, int min, int max) {
        this.min_value = min;
        this.max_value = max;
        this.bucker_width = (int) ceil((double) (max-min+1)/bucket_num);
        this.buckets = new ArrayList<>();
        for (int i = 0; i < bucket_num; i++) {
            this.buckets.add(0);
        }
        this.num_tuples = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        if(v < min_value || v > max_value) return;
        v -= min_value;
        int bucketId = v / bucker_width;
        buckets.set(bucketId, buckets.get(bucketId)+1);
        ++num_tuples;
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
        if(v < min_value) {
            if(op == Predicate.Op.EQUALS || op == Predicate.Op.LESS_THAN || op == Predicate.Op.LESS_THAN_OR_EQ) return 0;
            else if(op == Predicate.Op.NOT_EQUALS || op == Predicate.Op.GREATER_THAN || op == Predicate.Op.GREATER_THAN_OR_EQ) return 1;
        } else if(v > max_value) {
            if(op == Predicate.Op.EQUALS || op == Predicate.Op.GREATER_THAN || op == Predicate.Op.GREATER_THAN_OR_EQ) return 0;
            else if(op == Predicate.Op.NOT_EQUALS || op == Predicate.Op.LESS_THAN || op == Predicate.Op.LESS_THAN_OR_EQ) return 1;
        }
        v -= min_value;
        int bucketId = v / bucker_width;
        double selectivity = 0.0;
        if(op == Predicate.Op.EQUALS || op == Predicate.Op.NOT_EQUALS ||
                op == Predicate.Op.LESS_THAN_OR_EQ || op == Predicate.Op.GREATER_THAN_OR_EQ) {
            selectivity += (double) buckets.get(bucketId) / bucker_width / num_tuples;
            if(op == Predicate.Op.EQUALS) return selectivity;
            if(op == Predicate.Op.NOT_EQUALS) return 1-selectivity;
        }
        if(op == Predicate.Op.GREATER_THAN || op == Predicate.Op.GREATER_THAN_OR_EQ) {
            double b_f =(double) buckets.get(bucketId) / num_tuples;
            double b_part = (double) ((bucketId + 1) * bucker_width - v) / bucker_width;
            selectivity += b_f * b_part;
            for (int i = bucketId + 1; i < buckets.size(); i++) {
                selectivity += (double) buckets.get(i) / num_tuples;
            }
            return min(selectivity, 1.0);
        } else if(op == Predicate.Op.LESS_THAN || op == Predicate.Op.LESS_THAN_OR_EQ){
            double b_f =(double) buckets.get(bucketId) / num_tuples;
            double b_part = (double) (v - bucketId * bucker_width) / bucker_width;
            selectivity += b_f * b_part;
            for (int i = 0; i < bucketId; i++) {
                selectivity += (double) buckets.get(i) / num_tuples;
            }
            return min(selectivity, 1.0);
        }
        // never happen
        return -1.0;
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
        return (double) buckets.stream().reduce(Integer::sum).get() / buckets.size() / bucker_width / num_tuples;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        return buckets.toString();
    }
}
