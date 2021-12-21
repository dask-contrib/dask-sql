package com.dask.sql.rules;

import com.dask.sql.nodes.DaskLimit;
import com.dask.sql.nodes.DaskRel;
import com.dask.sql.nodes.DaskSort;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalSort;

public class DaskSortLimitRule extends ConverterRule {
    public static final DaskSortLimitRule INSTANCE = Config.INSTANCE
            .withConversion(LogicalSort.class, Convention.NONE, DaskRel.CONVENTION, "DaskSortLimitRule")
            .withRuleFactory(DaskSortLimitRule::new).toRule(DaskSortLimitRule.class);

    DaskSortLimitRule(Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        final LogicalSort sort = (LogicalSort) rel;
        final RelTraitSet traitSet = sort.getTraitSet().replace(out);
        RelNode transformedInput = convert(sort.getInput(), sort.getInput().getTraitSet().replace(DaskRel.CONVENTION));

        if (!sort.getCollation().getFieldCollations().isEmpty()) {
            // Create a sort with the same sort key, but no offset or fetch.
            transformedInput = DaskSort.create(transformedInput.getCluster(), traitSet, transformedInput,
                    sort.getCollation());
        }
        if (sort.fetch == null && sort.offset == null) {
            return transformedInput;
        }

        return DaskLimit.create(sort.getCluster(), traitSet, transformedInput, sort.offset, sort.fetch);
    }
}
