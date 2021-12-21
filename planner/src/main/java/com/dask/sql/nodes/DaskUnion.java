package com.dask.sql.nodes;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Union;

public class DaskUnion extends Union implements DaskRel {
    private DaskUnion(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, boolean all) {
        super(cluster, traits, inputs, all);
    }

    @Override
    public SetOp copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        return new DaskUnion(getCluster(), traitSet, inputs, all);
    }

    public static DaskUnion create(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, boolean all) {
        assert traits.getConvention() == DaskRel.CONVENTION;
        return new DaskUnion(cluster, traits, inputs, all);
    }
}
