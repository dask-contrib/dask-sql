package com.dask.sql.rules;

import com.dask.sql.nodes.DaskRel;
import com.dask.sql.nodes.DaskTableScan;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalTableScan;

public class DaskTableScanRule extends ConverterRule {
    public static final DaskTableScanRule INSTANCE = Config.INSTANCE
            .withConversion(LogicalTableScan.class, Convention.NONE, DaskRel.CONVENTION, "DaskTableScanRule")
            .withRuleFactory(DaskTableScanRule::new).toRule(DaskTableScanRule.class);

    DaskTableScanRule(Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        final LogicalTableScan scan = (LogicalTableScan) rel;
        final RelTraitSet traitSet = scan.getTraitSet().replace(out);
        return DaskTableScan.create(scan.getCluster(), traitSet, scan.getTable());
    }
}
