package com.dask.sql.nodes;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;

public class DaskFilter extends Filter implements DaskRel {
    private DaskFilter(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RexNode condition) {
        super(cluster, traitSet, child, condition);
    }

    @Override
    public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return DaskFilter.create(getCluster(), traitSet, input, condition);
    }

    public static DaskFilter create(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RexNode condition) {
        assert traitSet.getConvention() == DaskRel.CONVENTION;
        return new DaskFilter(cluster, traitSet, child, condition);
    }
}
