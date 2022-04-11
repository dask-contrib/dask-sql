package com.dask.sql.nodes;

import java.util.List;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

public class DaskProject extends Project implements DaskRel {
    private DaskProject(RelOptCluster cluster, RelTraitSet traits, RelNode input, List<? extends RexNode> projects,
            RelDataType rowType) {
        super(cluster, traits, ImmutableList.of(), input, projects, rowType);
    }

    @Override
    public Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
        return DaskProject.create(getCluster(), traitSet, input, projects, rowType);
    }

    public static DaskProject create(RelOptCluster cluster, RelTraitSet traits, RelNode input,
            List<? extends RexNode> projects, RelDataType rowType) {
        assert traits.getConvention() == DaskRel.CONVENTION;
        return new DaskProject(cluster, traits, input, projects, rowType);
    }
}
