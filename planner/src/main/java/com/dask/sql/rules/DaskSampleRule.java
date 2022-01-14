package com.dask.sql.rules;

import com.dask.sql.nodes.DaskRel;
import com.dask.sql.nodes.DaskSample;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Sample;

public class DaskSampleRule extends ConverterRule {
    public static final DaskSampleRule INSTANCE = Config.INSTANCE
            .withConversion(Sample.class, Convention.NONE, DaskRel.CONVENTION, "DaskSampleRule")
            .withRuleFactory(DaskSampleRule::new).toRule(DaskSampleRule.class);

    DaskSampleRule(Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        final Sample sample = (Sample) rel;
        final RelTraitSet traitSet = sample.getTraitSet().replace(out);
        RelNode transformedInput = convert(sample.getInput(),
                sample.getInput().getTraitSet().replace(DaskRel.CONVENTION));

        return DaskSample.create(sample.getCluster(), traitSet, transformedInput, sample.getSamplingParameters());
    }
}
