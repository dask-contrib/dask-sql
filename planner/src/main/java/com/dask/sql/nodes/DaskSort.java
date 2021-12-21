package com.dask.sql.nodes;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

public class DaskSort extends Sort implements DaskRel {
    private DaskSort(RelOptCluster cluster, RelTraitSet traits, RelNode child, RelCollation collation) {
        super(cluster, traits, child, collation);
    }

    @Override
    public Sort copy(RelTraitSet traitSet, RelNode newInput, RelCollation newCollation, @Nullable RexNode offset,
            @Nullable RexNode fetch) {
        assert offset == null;
        assert fetch == null;
        return DaskSort.create(getCluster(), traitSet, newInput, newCollation);
    }

    static public DaskSort create(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelCollation collation) {
        assert traitSet.getConvention() == DaskRel.CONVENTION;
        return new DaskSort(cluster, traitSet, input, collation);
    }

}
