package com.dask.sql.application;

import com.dask.sql.rules.DaskAggregateRule;
import com.dask.sql.rules.DaskFilterRule;
import com.dask.sql.rules.DaskJoinRule;
import com.dask.sql.rules.DaskProjectRule;
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

public class DaskPlanner extends VolcanoPlanner {
    public DaskPlanner() {
        addRule(DaskAggregateRule.INSTANCE);
        addRule(DaskFilterRule.INSTANCE);
        addRule(DaskJoinRule.INSTANCE);
        addRule(DaskProjectRule.INSTANCE);
        addRule(DaskSortLimitRule.INSTANCE);
        addRule(DaskTableScanRule.INSTANCE);
        addRule(DaskUnionRule.INSTANCE);
        addRule(DaskValuesRule.INSTANCE);
        addRule(DaskWindowRule.INSTANCE);

        // TODO
        addRule(CoreRules.AGGREGATE_ANY_PULL_UP_CONSTANTS);
        addRule(CoreRules.UNION_PULL_UP_CONSTANTS);
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
        addRule(CoreRules.UNION_MERGE);
        addRule(CoreRules.INTERSECT_MERGE);
        addRule(CoreRules.MINUS_MERGE);
        addRule(CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW);
        addRule(CoreRules.FILTER_MERGE);
        addRule(DateRangeRules.FILTER_INSTANCE);
        addRule(CoreRules.INTERSECT_TO_DISTINCT);
        addRule(CoreRules.AGGREGATE_STAR_TABLE);
        addRule(CoreRules.AGGREGATE_PROJECT_STAR_TABLE);
        addRule(CoreRules.PROJECT_MERGE);
        addRule(CoreRules.FILTER_SCAN);
        addRule(CoreRules.PROJECT_FILTER_TRANSPOSE);
        addRule(CoreRules.FILTER_PROJECT_TRANSPOSE);
        addRule(CoreRules.FILTER_INTO_JOIN);
        addRule(CoreRules.JOIN_PUSH_EXPRESSIONS);
        addRule(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES);
        // addRule(CoreRules.AGGREGATE_CASE_TO_FILTER);
        addRule(CoreRules.AGGREGATE_REDUCE_FUNCTIONS);
        addRule(CoreRules.FILTER_AGGREGATE_TRANSPOSE);
        addRule(CoreRules.PROJECT_WINDOW_TRANSPOSE);
        addRule(CoreRules.MATCH);
        addRule(CoreRules.JOIN_COMMUTE);
        addRule(JoinPushThroughJoinRule.RIGHT);
        addRule(JoinPushThroughJoinRule.LEFT);
        addRule(CoreRules.SORT_PROJECT_TRANSPOSE);
        addRule(CoreRules.SORT_JOIN_TRANSPOSE);
        addRule(CoreRules.SORT_REMOVE_CONSTANT_KEYS);
        addRule(CoreRules.SORT_UNION_TRANSPOSE);
        addRule(CoreRules.EXCHANGE_REMOVE_CONSTANT_KEYS);
        addRule(CoreRules.SORT_EXCHANGE_REMOVE_CONSTANT_KEYS);
        addRule(CoreRules.PROJECT_TABLE_SCAN);
        addRule(CoreRules.PROJECT_INTERPRETER_TABLE_SCAN);
        addRule(CoreRules.FILTER_REDUCE_EXPRESSIONS);

        // Enable conventions to turn from logical to dask
        addRelTraitDef(ConventionTraitDef.INSTANCE);

        // We do not want to execute any SQL
        setExecutor(null);
    }
}
