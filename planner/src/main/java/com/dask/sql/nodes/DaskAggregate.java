package com.dask.sql.nodes;

import java.util.List;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.util.ImmutableBitSet;

public class DaskAggregate extends Aggregate implements DaskRel {
    private DaskAggregate(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        super(cluster, traitSet, ImmutableList.of(), input, groupSet, groupSets, aggCalls);
    }

    @Override
    public Aggregate copy(RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        return DaskAggregate.create(getCluster(), traitSet, input, groupSet, groupSets, aggCalls);
    }

    public static DaskAggregate create(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
            ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        assert traitSet.getConvention() == DaskRel.CONVENTION;
        return new DaskAggregate(cluster, traitSet, input, groupSet, groupSets, aggCalls);
    }
}
