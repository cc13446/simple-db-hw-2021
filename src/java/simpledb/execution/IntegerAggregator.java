package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int groupByFieldIndex;
    private final Type groupByFieldType;
    private final int aggregateFieldIndex;
    private final Op op;

    private final Map<String, Item> map;

    static class Item {
        int max = Integer.MIN_VALUE;
        int min = Integer.MAX_VALUE;
        int sum = 0;
        int count = 0;
        int avg = 0;
    }

    /**
     * Aggregate constructor
     * 
     * @param groupByFieldIndex
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param groupByFieldType
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int groupByFieldIndex, Type groupByFieldType, int afield, Op what) {
        // some code goes here
        this.groupByFieldIndex = groupByFieldIndex;
        this.groupByFieldType = groupByFieldType;
        this.aggregateFieldIndex = afield;
        this.op = what;
        assert(this.groupByFieldIndex != this.NO_GROUPING || this.groupByFieldType == null);
        this.map = new HashMap<>();

    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        assert(tup.getTupleDesc().getFieldType(this.aggregateFieldIndex) == Type.INT_TYPE);
        int val = ((IntField) tup.getField(aggregateFieldIndex)).getValue();
        if (this.groupByFieldType == null) {
            map.put(String.valueOf(this.NO_GROUPING), calculate(map.getOrDefault(String.valueOf(this.NO_GROUPING), new Item()), val));
        } else if (this.groupByFieldType == Type.INT_TYPE) {
            map.put(String.valueOf(((IntField) tup.getField(this.groupByFieldIndex)).getValue()), calculate(map.getOrDefault(String.valueOf(((IntField) tup.getField(this.groupByFieldIndex)).getValue()), new Item()), val));
        } else if (this.groupByFieldType == Type.STRING_TYPE) {
            map.put(String.valueOf(((StringField) tup.getField(this.groupByFieldIndex)).getValue()), calculate(map.getOrDefault(String.valueOf(((StringField) tup.getField(this.groupByFieldIndex)).getValue()), new Item()), val));
        } else {
            throw new UnsupportedOperationException("unsupported Operation");
        }
    }

    private Item calculate(Item oldValue, int newValue) {
        oldValue.max = Math.max(oldValue.max, newValue);
        oldValue.min = Math.min(oldValue.min, newValue);
        oldValue.count = oldValue.count + 1;
        oldValue.sum = oldValue.sum + newValue;
        oldValue.avg = oldValue.sum / oldValue.count;
        return oldValue;
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        if (this.groupByFieldIndex == this.NO_GROUPING) {
            Type[] types = new Type[]{Type.INT_TYPE};
            TupleDesc td = new TupleDesc(types);
            if (map.size() == 0) return new IntegerAggregatorOpIterator(td, new LinkedList<>());
            Tuple t = new Tuple(td);
            t.setField(0, new IntField(getAggregatorResult(map.get(String.valueOf(this.NO_GROUPING)))));
            List<Tuple> list = new ArrayList<>(1);
            list.add(t);
            return new IntegerAggregatorOpIterator(td, list);
        } else {
            Type[] types = new Type[]{this.groupByFieldType, Type.INT_TYPE};
            TupleDesc td = new TupleDesc(types);
            if (map.size() == 0) return new IntegerAggregatorOpIterator(td, new LinkedList<>());
            List<Tuple> list = new ArrayList<>(map.size());
            map.forEach((key, val) -> {
                Tuple t = new Tuple(td);
                Field k = null;
                if (this.groupByFieldType == Type.INT_TYPE) {
                    k = new IntField(Integer.parseInt(key));
                } else if (this.groupByFieldType == Type.STRING_TYPE){
                    k = new StringField(key, Type.STRING_LEN);
                }
                t.setField(0, k);
                t.setField(1, new IntField(getAggregatorResult(val)));
                list.add(t);
            });
            return new IntegerAggregatorOpIterator(td, list);
        }
    }

    private int getAggregatorResult(Item item) {
        switch (this.op) {
            case MIN:return item.min;
            case MAX:return item.max;
            case SUM:return item.sum;
            case AVG:return item.avg;
            case COUNT:return item.count;
            default:throw new UnsupportedOperationException("unsupported Operation");
        }
    }

}
