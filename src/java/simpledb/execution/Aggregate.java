package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private final OpIterator child;
    private final int aggregateFieldIndex;
    private final int groupByFieldIndex;
    private final Aggregator.Op aop;
    private final Aggregator aggregator;
    private OpIterator opIterator;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of aggregateFieldIndex, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param aggregateFieldIndex The column over which we are computing an aggregate.
     * @param groupByFieldIndex The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int aggregateFieldIndex, int groupByFieldIndex, Aggregator.Op aop) {
        // some code goes here
        this.child = child;
        this.aggregateFieldIndex = aggregateFieldIndex;
        this.groupByFieldIndex = groupByFieldIndex;
        this.aop = aop;
        Type aggregateField = child.getTupleDesc().getFieldType(this.aggregateFieldIndex);
        Type groupByType = this.groupByFieldIndex == Aggregator.NO_GROUPING ? null : child.getTupleDesc().getFieldType(this.groupByFieldIndex);
        if (aggregateField == Type.INT_TYPE) {
            this.aggregator = new IntegerAggregator(this.groupByFieldIndex, groupByType, this.aggregateFieldIndex, this.aop);
        } else if (aggregateField == Type.STRING_TYPE) {
            this.aggregator = new StringAggregator(this.groupByFieldIndex, groupByType, this.aggregateFieldIndex, this.aop);
        } else {
            throw new UnsupportedOperationException("unsupported Operation");
        }
        this.opIterator = this.aggregator.iterator();

    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // some code goes here
        return this.groupByFieldIndex;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        // some code goes here
        if (this.groupByFieldIndex == Aggregator.NO_GROUPING) {
            return null;
        }
        return child.getTupleDesc().getFieldName(this.groupByFieldIndex);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
        return this.aggregateFieldIndex;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        // some code goes here
        return child.getTupleDesc().getFieldName(this.aggregateFieldIndex);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        this.child.open();
        while (this.child.hasNext()) {
            this.aggregator.mergeTupleIntoGroup(this.child.next());
        }
        this.opIterator = this.aggregator.iterator();
        this.child.close();
        this.opIterator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (this.opIterator.hasNext()) {
            return this.opIterator.next();
        } else {
            return null;
        }
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.opIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return opIterator.getTupleDesc();
    }

    public void close() {
        // some code goes here
        super.close();
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        OpIterator[] res = new OpIterator[1];
        res[0] = this.opIterator;
        return res;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        assert(children.length > 0);
        this.opIterator = children[0];
    }

}
