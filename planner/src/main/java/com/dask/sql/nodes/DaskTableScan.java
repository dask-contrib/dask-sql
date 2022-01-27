package com.dask.sql.nodes;

import java.util.List;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;

public class DaskTableScan extends TableScan implements DaskRel {
    private DaskTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
        super(cluster, traitSet, ImmutableList.of(), table);
    }

    public static DaskTableScan create(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
        assert traitSet.getConvention() == DaskRel.CONVENTION;
        return new DaskTableScan(cluster, traitSet, table);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return copy(traitSet, this.getTable());
    }

    private RelNode copy(RelTraitSet traitSet, RelOptTable table) {
        return new DaskTableScan(getCluster(), traitSet, table);
    }
}
