package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int groupByFieldIndex;
    private final Type groupByFieldType;
    private final int aggregateFieldIndex;
    private final Op op;

    private final Map<String, Integer> map;

    /**
     * Aggregate constructor
     * @param groupByFieldIndex the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param groupByFieldType the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int groupByFieldIndex, Type groupByFieldType, int afield, Op what) {
        // some code goes here
        if (what != Op.COUNT) throw new IllegalArgumentException("String aggregator only supports COUNT");
        this.aggregateFieldIndex = afield;
        this.groupByFieldIndex = groupByFieldIndex;
        this.groupByFieldType = groupByFieldType;
        this.op = what;
        this.map = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (this.op == Op.COUNT) {
            assert(tup.getTupleDesc().getFieldType(this.aggregateFieldIndex) == Type.STRING_TYPE);
            if (this.groupByFieldType == null) {
                map.put(String.valueOf(this.NO_GROUPING), map.getOrDefault(String.valueOf(this.NO_GROUPING), 0) + 1);
            } else if (this.groupByFieldType == Type.INT_TYPE) {
                map.put(String.valueOf(((IntField) tup.getField(this.groupByFieldIndex)).getValue()), map.getOrDefault(String.valueOf(((IntField) tup.getField(this.groupByFieldIndex)).getValue()), 0) + 1);
            } else if (this.groupByFieldType == Type.STRING_TYPE) {
                map.put(String.valueOf(((StringField) tup.getField(this.groupByFieldIndex)).getValue()), map.getOrDefault(String.valueOf(((StringField) tup.getField(this.groupByFieldIndex)).getValue()), 0) + 1);
            } else {
                throw new UnsupportedOperationException("unsupported Operation");
            }
        } else {
            throw new IllegalStateException("String aggregator only supports COUNT");
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        if (this.groupByFieldIndex == this.NO_GROUPING) {
            Type[] types = new Type[]{Type.INT_TYPE};
            TupleDesc td = new TupleDesc(types);
            if (map.size() == 0) return new IntegerAggregatorOpIterator(td, new LinkedList<>());
            Tuple t = new Tuple(td);
            t.setField(0, new IntField(map.get(String.valueOf(this.NO_GROUPING))));
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
                t.setField(1, new IntField(val));
                list.add(t);
            });
            return new IntegerAggregatorOpIterator(td, list);
        }
    }

}
