package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
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
            int tableId = tableIt.next();
            TableStats s = new TableStats(tableId, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableId), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private final Map<Integer, StringHistogram> stringHistogramMap;
    private final Map<Integer, IntHistogram> intHistogramMap;

    private final int tableId;
    private final int ioCostPerPage;

    private int tupleNums;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableId
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableId, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.tableId = tableId;
        this.ioCostPerPage = ioCostPerPage;
        this.stringHistogramMap = new HashMap<>();
        this.intHistogramMap = new HashMap<>();
        this.tupleNums = 0;

        TupleDesc tupleDesc = Database.getCatalog().getTupleDesc(this.tableId);
        int[] min = new int[tupleDesc.numFields()];
        int[] max = new int[tupleDesc.numFields()];
        for (int i = 0; i < tupleDesc.numFields(); i++) {
            min[i] = Integer.MAX_VALUE;
            max[i] = Integer.MIN_VALUE;
        }

        SeqScan scan = new SeqScan(new TransactionId(),this.tableId,"");
        try {
            scan.open();
            while (scan.hasNext()) {
                this.tupleNums++;
                Tuple t = scan.next();
                for (int i = 0; i < tupleDesc.numFields(); i++) {
                    if (tupleDesc.getFieldType(i).equals(Type.STRING_TYPE)) {
                        continue;
                    }
                    min[i] = Math.min(min[i], ((IntField) t.getField(i)).getValue());
                    max[i] = Math.max(max[i], ((IntField) t.getField(i)).getValue());
                }

            }
            scan.close();
        } catch (TransactionAbortedException | DbException e) {
            e.printStackTrace();
        }


        for (int i = 0; i < tupleDesc.numFields(); i++) {
            Type t = tupleDesc.getFieldType(i);
            if (t == Type.INT_TYPE) {
                intHistogramMap.put(i, new IntHistogram(NUM_HIST_BINS, min[i], max[i]));
            } else if (t == Type.STRING_TYPE){
                stringHistogramMap.put(i, new StringHistogram(NUM_HIST_BINS));
            } else {
                throw new IllegalStateException("Unsupported Field Type");
            }
        }

        try {
            scan.rewind();
            scan.open();
            while (scan.hasNext()) {
                Tuple t = scan.next();
                for (int i = 0; i < tupleDesc.numFields(); i++) {
                    Field field = t.getField(i);
                    if(field.getType() == Type.INT_TYPE){
                        this.intHistogramMap.get(i).addValue(((IntField)field).getValue());
                    }else if (field.getType() == Type.STRING_TYPE){
                        this.stringHistogramMap.get(i).addValue(((StringField)field).getValue());
                    } else {
                        throw new IllegalStateException("Unsupported Field Type");
                    }
                }
            }
            scan.close();
        } catch (TransactionAbortedException | DbException e) {
            e.printStackTrace();
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
        // some code goes here
        HeapFile heapFile = (HeapFile)Database.getCatalog().getDatabaseFile(this.tableId);
        return heapFile.numPages() * ioCostPerPage;
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
        // some code goes here
        double cardinality = this.tupleNums * selectivityFactor;
        return (int) cardinality;
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
        // some code goes here
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
        // some code goes here
        if(constant.getType() == Type.INT_TYPE){
            IntField intField = (IntField) constant;
            return intHistogramMap.get(field).estimateSelectivity(op, intField.getValue());
        }else{
            StringField stringField = (StringField) constant;
            return stringHistogramMap.get(field).estimateSelectivity(op, stringField.getValue());
        }

    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return this.tupleNums;
    }

}
