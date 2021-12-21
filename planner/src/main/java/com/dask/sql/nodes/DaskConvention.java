package com.dask.sql.nodes;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

public class DaskConvention extends Convention.Impl {
    static public DaskConvention INSTANCE = new DaskConvention();

    private DaskConvention() {
        super("DASK", DaskRel.class);
    }

    @Override
    public RelNode enforce(final RelNode input, final RelTraitSet required) {
        RelNode rel = input;
        if (input.getConvention() != INSTANCE) {
            rel = ConventionTraitDef.INSTANCE.convert(input.getCluster().getPlanner(), input, INSTANCE, true);
        }
        return rel;
    }

    public boolean canConvertConvention(Convention toConvention) {
        return false;
    }

    public boolean useAbstractConvertersForConversion(RelTraitSet fromTraits, RelTraitSet toTraits) {
        // Note: there seems to be two possibilities how to handle traits
        // during optimization.
        // One: set planner.setTopDownOpt(false) (the default).
        // In this mode, we need to return true here and let Calcite include
        // abstract converters whenever needed. We then need rules to
        // turn the abstract converters into actual rels (like Exchange and Sort).
        // Two: set planner.setTopDownOpt(true)
        // Here, Calcite will propagate the needed traits from the top to the bottom.
        // Each rel can decide on its own whether it can propagate the traits
        // (example: a project will keep the collation and distribution traits,
        // so it can propagate them). Whenever calcite sees that traits can not be
        // propagated, it will call the enforce method of this convention.
        // Here, we want to return false in this function.
        return true;
    }
}
