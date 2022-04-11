package com.dask.sql.rules;

import com.dask.sql.nodes.DaskRel;
import com.dask.sql.nodes.DaskFilter;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalFilter;

public class DaskFilterRule extends ConverterRule {
    public static final DaskFilterRule INSTANCE = Config.INSTANCE.withConversion(LogicalFilter.class,
            f -> !f.containsOver(), Convention.NONE, DaskRel.CONVENTION, "DaskFilterRule")
            .withRuleFactory(DaskFilterRule::new).toRule(DaskFilterRule.class);

    DaskFilterRule(Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        final LogicalFilter filter = (LogicalFilter) rel;
        final RelTraitSet traitSet = filter.getTraitSet().replace(out);
        RelNode transformedInput = convert(filter.getInput(),
                filter.getInput().getTraitSet().replace(DaskRel.CONVENTION));
        return DaskFilter.create(filter.getCluster(), traitSet, transformedInput, filter.getCondition());
    }
}
