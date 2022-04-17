package simpledb.optimizer;

import simpledb.execution.OpIterator;
import simpledb.execution.Predicate;

/** A LogicalSubPlanJoinNode represents the state needed of a join of a
 * table to a subPlan in a LogicalQueryPlan -- inherits state from
 * {@link LogicalJoinNode}; t2 and f2 should always be null
 */
public class LogicalSubPlanJoinNode extends LogicalJoinNode {
    
    /** The subPlan (used on the inner) of the join */
    final OpIterator subPlan;
    
    public LogicalSubPlanJoinNode(String table1, String joinField1, OpIterator sp, Predicate.Op pred) {
        t1Alias = table1;
        String[] tmps = joinField1.split("[.]");
        if (tmps.length>1)
            f1PureName = tmps[tmps.length-1];
        else
            f1PureName=joinField1;
        f1QuantifiedName=t1Alias+"."+f1PureName;
        subPlan = sp;
        p = pred;
    }
    
    @Override public int hashCode() {
        return t1Alias.hashCode() + f1PureName.hashCode() + subPlan.hashCode();
    }
    
    @Override public boolean equals(Object o) {
        LogicalJoinNode j2 =(LogicalJoinNode)o;
        if (!(o instanceof LogicalSubPlanJoinNode))
            return false;
        
        return (j2.t1Alias.equals(t1Alias)  && j2.f1PureName.equals(f1PureName) && ((LogicalSubPlanJoinNode)o).subPlan.equals(subPlan));
    }
    
    public LogicalSubPlanJoinNode swapInnerOuter() {
        return new LogicalSubPlanJoinNode(t1Alias,f1PureName,subPlan, p);
    }

}
