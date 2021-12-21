package com.dask.sql.rules;

import com.dask.sql.nodes.DaskRel;
import com.dask.sql.nodes.DaskAggregate;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalAggregate;

public class DaskAggregateRule extends ConverterRule {
    public static final DaskAggregateRule INSTANCE = Config.INSTANCE
            .withConversion(LogicalAggregate.class, Convention.NONE, DaskRel.CONVENTION, "DaskAggregateRule")
            .withRuleFactory(DaskAggregateRule::new).toRule(DaskAggregateRule.class);

    DaskAggregateRule(Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        final LogicalAggregate agg = (LogicalAggregate) rel;
        final RelTraitSet traitSet = agg.getTraitSet().replace(out);
        RelNode transformedInput = convert(agg.getInput(), agg.getInput().getTraitSet().replace(DaskRel.CONVENTION));

        return DaskAggregate.create(agg.getCluster(), traitSet, transformedInput, agg.getGroupSet(), agg.getGroupSets(),
                agg.getAggCallList());
    }
}
