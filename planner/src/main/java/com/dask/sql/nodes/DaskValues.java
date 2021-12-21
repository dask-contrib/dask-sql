package com.dask.sql.nodes;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import com.google.common.collect.ImmutableList;

public class DaskValues extends Values implements DaskRel {
    private DaskValues(RelOptCluster cluster, RelDataType rowType, ImmutableList<ImmutableList<RexLiteral>> tuples,
            RelTraitSet traits) {
        super(cluster, rowType, tuples, traits);
    }

    public static DaskValues create(RelOptCluster cluster, RelDataType rowType,
            ImmutableList<ImmutableList<RexLiteral>> tuples, RelTraitSet traits) {
        assert traits.getConvention() == DaskRel.CONVENTION;
        return new DaskValues(cluster, rowType, tuples, traits);
    }
}
