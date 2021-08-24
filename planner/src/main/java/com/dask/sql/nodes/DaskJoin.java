package com.dask.sql.nodes;

import java.util.Set;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;

public class DaskJoin extends Join implements DaskRel {
    private DaskJoin(RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right, RexNode condition,
            Set<CorrelationId> variablesSet, JoinRelType joinType) {
        super(cluster, traitSet, ImmutableList.of(), left, right, condition, variablesSet, joinType);
    }

    @Override
    public Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType,
            boolean semiJoinDone) {
        return new DaskJoin(getCluster(), traitSet, left, right, condition, variablesSet, joinType);
    }

    public static DaskJoin create(RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right,
            RexNode condition, Set<CorrelationId> variablesSet, JoinRelType joinType) {
        assert traitSet.getConvention() == DaskRel.CONVENTION;
        return new DaskJoin(cluster, traitSet, left, right, condition, variablesSet, joinType);
    }
}
