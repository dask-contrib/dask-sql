package com.dask.sql.nodes;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSamplingParameters;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sample;

public class DaskSample extends Sample implements DaskRel {
    private DaskSample(RelOptCluster cluster, RelTraitSet traits, RelNode child, RelOptSamplingParameters params) {
        super(cluster, child, params);
        this.traitSet = traits;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return DaskSample.create(getCluster(), traitSet, sole(inputs), getSamplingParameters());
    }

    public static DaskSample create(RelOptCluster cluster, RelTraitSet traits, RelNode input,
            RelOptSamplingParameters params) {
        assert traits.getConvention() == DaskRel.CONVENTION;
        return new DaskSample(cluster, traits, input, params);
    }
}
