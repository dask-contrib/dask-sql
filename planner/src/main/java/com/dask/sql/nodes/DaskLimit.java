package com.dask.sql.nodes;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

public class DaskLimit extends SingleRel implements DaskRel {
    private final @Nullable RexNode offset;
    private final @Nullable RexNode fetch;

    private DaskLimit(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, @Nullable RexNode offset,
            @Nullable RexNode fetch) {
        super(cluster, traitSet, input);
        this.offset = offset;
        this.fetch = fetch;
    }

    public DaskLimit copy(RelTraitSet traitSet, RelNode input, @Nullable RexNode offset, @Nullable RexNode fetch) {
        return new DaskLimit(getCluster(), traitSet, input, offset, fetch);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return copy(traitSet, sole(inputs), this.getOffset(), this.getFetch());
    }

    public @Nullable RexNode getFetch() {
        return this.fetch;
    }

    public @Nullable RexNode getOffset() {
        return this.offset;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("offset", this.getOffset()).item("fetch", this.getFetch());
    }

    public static DaskLimit create(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, @Nullable RexNode offset,
            @Nullable RexNode fetch) {
        assert traitSet.getConvention() == DaskRel.CONVENTION;
        return new DaskLimit(cluster, traitSet, input, offset, fetch);
    }
}
