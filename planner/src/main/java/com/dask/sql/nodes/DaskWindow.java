package com.dask.sql.nodes;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;

public class DaskWindow extends Window implements DaskRel {
    public DaskWindow(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, List<RexLiteral> constants,
            RelDataType rowType, List<Group> groups) {
        super(cluster, traitSet, input, constants, rowType, groups);
    }

    public List<Group> getGroups() {
        return this.groups;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return copy(traitSet, sole(inputs), this.getConstants(), this.getRowType(), this.getGroups());
    }

    public Window copy(RelTraitSet traitSet, RelNode input, List<RexLiteral> constants, RelDataType rowType,
            List<Group> groups) {
        return DaskWindow.create(getCluster(), traitSet, input, constants, rowType, groups);
    }

    public static DaskWindow create(RelOptCluster cluster, RelTraitSet traits, RelNode input,
            List<RexLiteral> constants, RelDataType rowType, List<Group> groups) {
        assert traits.getConvention() == DaskRel.CONVENTION;
        return new DaskWindow(cluster, traits, input, constants, rowType, groups);
    }
}
