package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionId;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;
    private final List<IntHistogram> intHistograms;
    private final List<StringHistogram> stringHistograms;
    private final TupleDesc td;
    private int tupleNum;
    private final double scanCost;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }
    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        this.tupleNum = 0;
        HeapFile hf = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
        DbFileIterator it = hf.iterator(new TransactionId());
        this.td = hf.getTupleDesc();
        this.scanCost = hf.numPages() * ioCostPerPage;
        List<Integer> min_fields = new ArrayList<>();
        List<Integer> max_fields = new ArrayList<>();
        try {
            it.open();
            if(it.hasNext()) {
                Tuple t = it.next();
                Iterator<Field> field_it = t.fields();
                while(field_it.hasNext()) {
                    Field f = field_it.next();
                    if(f.getType() == Type.INT_TYPE) {
                        IntField intField = (IntField) f;
                        min_fields.add(intField.getValue());
                        max_fields.add(intField.getValue());
                    } else {
                        min_fields.add(0);
                        max_fields.add(0);
                    }
                }
            }
            while(it.hasNext()) {
                Tuple t = it.next();
                Iterator<Field> field_it = t.fields();
                int idx = 0;
                while(field_it.hasNext()) {
                    Field f = field_it.next();
                    if(f.getType() == Type.INT_TYPE) {
                        IntField intField = (IntField) f;
                        if(intField.getValue() < min_fields.get(idx)) {
                            min_fields.set(idx, intField.getValue());
                        }
                        if(intField.getValue() > max_fields.get(idx)) {
                            max_fields.set(idx, intField.getValue());
                        }
                    }
                    ++idx;
                }
            }
            it.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        intHistograms = new ArrayList<>();
        stringHistograms = new ArrayList<>();
        Iterator<TupleDesc.TDItem> td_it = hf.getTupleDesc().iterator();
        int idx = 0;
        while(td_it.hasNext()) {
            TupleDesc.TDItem item = td_it.next();
            if(item.fieldType == Type.INT_TYPE) {
                intHistograms.add(new IntHistogram(NUM_HIST_BINS, min_fields.get(idx), max_fields.get(idx)));
                stringHistograms.add(null);
            } else if(item.fieldType == Type.STRING_TYPE){
                intHistograms.add(null);
                stringHistograms.add(new StringHistogram(NUM_HIST_BINS));
            }
            ++idx;
        }
        try {
            it.open();
            while(it.hasNext()) {
                Tuple t = it.next();
                Iterator<Field> field_it = t.fields();
                idx = 0;
                while(field_it.hasNext()) {
                    Field f = field_it.next();
                    if(f.getType() == Type.INT_TYPE) {
                        IntField intField = (IntField) f;
                        IntHistogram intHistogram = intHistograms.get(idx);
                        intHistogram.addValue(intField.getValue());
                    } else if(f.getType() == Type.STRING_TYPE) {
                        StringField stringField = (StringField) f;
                        StringHistogram stringHistogram = stringHistograms.get(idx);
                        stringHistogram.addValue(stringField.getValue());
                    }
                    ++idx;
                }
                ++tupleNum;
            }
            it.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        return this.scanCost;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int) (tupleNum * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        Type type = td.getFieldType(field);
        if(type == Type.INT_TYPE) {
            IntHistogram histogram = intHistograms.get(field);
            return histogram.avgSelectivity();
        } else if(type == Type.STRING_TYPE) {
            StringHistogram histogram = stringHistograms.get(field);
            return histogram.avgSelectivity();
        }
        // never happen
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        Type type = td.getFieldType(field);
        if(type == Type.INT_TYPE) {
            IntHistogram histogram = intHistograms.get(field);
            IntField intField = (IntField) constant;
            return histogram.estimateSelectivity(op, intField.getValue());
        } else if(type == Type.STRING_TYPE) {
            StringHistogram histogram = stringHistograms.get(field);
            StringField stringField = (StringField) constant;
            return histogram.estimateSelectivity(op, stringField.getValue());
        }
        // never happen
        return 0.0;
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        return this.tupleNum;
    }

}
