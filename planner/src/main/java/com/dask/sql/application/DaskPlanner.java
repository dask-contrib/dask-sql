package com.dask.sql.application;

import com.dask.sql.rules.DaskAggregateRule;
import com.dask.sql.rules.DaskFilterRule;
import com.dask.sql.rules.DaskJoinRule;
import com.dask.sql.rules.DaskProjectRule;
import com.dask.sql.rules.DaskSampleRule;
import com.dask.sql.rules.DaskSortLimitRule;
import com.dask.sql.rules.DaskTableScanRule;
import com.dask.sql.rules.DaskUnionRule;
import com.dask.sql.rules.DaskValuesRule;
import com.dask.sql.rules.DaskWindowRule;

import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.DateRangeRules;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;

/**
 * DaskPlanner is a cost-based optimizer based on the Calcite VolcanoPlanner.
 *
 * Its only difference to the raw Volcano planner, are the predefined rules (for
 * converting logical into dask nodes and some basic core rules so far), as well
 * as the null executor.
 */
public class DaskPlanner extends VolcanoPlanner {
    public DaskPlanner() {
        // Allow transformation between logical and dask nodes
        addRule(DaskAggregateRule.INSTANCE);
        addRule(DaskFilterRule.INSTANCE);
        addRule(DaskJoinRule.INSTANCE);
        addRule(DaskProjectRule.INSTANCE);
        addRule(DaskSampleRule.INSTANCE);
        addRule(DaskSortLimitRule.INSTANCE);
        addRule(DaskTableScanRule.INSTANCE);
        addRule(DaskUnionRule.INSTANCE);
        addRule(DaskValuesRule.INSTANCE);
        addRule(DaskWindowRule.INSTANCE);

        // Set of core rules
        addRule(PruneEmptyRules.UNION_INSTANCE);
        addRule(PruneEmptyRules.INTERSECT_INSTANCE);
        addRule(PruneEmptyRules.MINUS_INSTANCE);
        addRule(PruneEmptyRules.PROJECT_INSTANCE);
        addRule(PruneEmptyRules.FILTER_INSTANCE);
        addRule(PruneEmptyRules.SORT_INSTANCE);
        addRule(PruneEmptyRules.AGGREGATE_INSTANCE);
        addRule(PruneEmptyRules.JOIN_LEFT_INSTANCE);
        addRule(PruneEmptyRules.JOIN_RIGHT_INSTANCE);
        addRule(PruneEmptyRules.SORT_FETCH_ZERO_INSTANCE);
        addRule(DateRangeRules.FILTER_INSTANCE);
        addRule(CoreRules.INTERSECT_TO_DISTINCT);
        addRule(CoreRules.PROJECT_FILTER_TRANSPOSE);
        addRule(CoreRules.FILTER_PROJECT_TRANSPOSE);
        addRule(CoreRules.FILTER_INTO_JOIN);
        addRule(CoreRules.JOIN_CONDITION_PUSH);
        addRule(CoreRules.JOIN_PUSH_EXPRESSIONS);
        addRule(CoreRules.FILTER_AGGREGATE_TRANSPOSE);
        addRule(CoreRules.PROJECT_WINDOW_TRANSPOSE);
        addRule(CoreRules.JOIN_COMMUTE);
        addRule(CoreRules.FILTER_INTO_JOIN);
        addRule(CoreRules.PROJECT_JOIN_TRANSPOSE);
        addRule(JoinPushThroughJoinRule.RIGHT);
        addRule(JoinPushThroughJoinRule.LEFT);
        addRule(CoreRules.SORT_PROJECT_TRANSPOSE);
        addRule(CoreRules.SORT_JOIN_TRANSPOSE);
        addRule(CoreRules.SORT_UNION_TRANSPOSE);

        // Enable conventions to turn from logical to dask
        addRelTraitDef(ConventionTraitDef.INSTANCE);

        // We do not want to execute any SQL
        setExecutor(null);
    }
}
