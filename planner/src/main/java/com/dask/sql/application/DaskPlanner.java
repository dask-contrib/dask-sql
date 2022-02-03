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

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.DateRangeRules;
import org.apache.calcite.rel.rules.PruneEmptyRules;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * DaskPlanner is a cost-based optimizer based on the Calcite VolcanoPlanner.
 *
 * Its only difference to the raw Volcano planner, are the predefined rules (for
 * converting logical into dask nodes and some basic core rules so far), as well
 * as the null executor.
 */
public class DaskPlanner extends VolcanoPlanner {

    private final Context defaultContext;

    private final ArrayList<RelOptRule> ALL_RULES = new ArrayList<>(
            Arrays.asList(
                    // Allow transformation between logical and dask nodes
                    DaskAggregateRule.INSTANCE,
                    DaskFilterRule.INSTANCE,
                    DaskJoinRule.INSTANCE,
                    DaskProjectRule.INSTANCE,
                    DaskSampleRule.INSTANCE,
                    DaskSortLimitRule.INSTANCE,
                    DaskTableScanRule.INSTANCE,
                    DaskUnionRule.INSTANCE,
                    DaskValuesRule.INSTANCE,
                    DaskWindowRule.INSTANCE,

                    // Set of core rules
                    PruneEmptyRules.UNION_INSTANCE,
                    PruneEmptyRules.INTERSECT_INSTANCE,
                    PruneEmptyRules.MINUS_INSTANCE,
                    PruneEmptyRules.PROJECT_INSTANCE,
                    PruneEmptyRules.FILTER_INSTANCE,
                    PruneEmptyRules.SORT_INSTANCE,
                    PruneEmptyRules.AGGREGATE_INSTANCE,
                    PruneEmptyRules.JOIN_LEFT_INSTANCE,
                    PruneEmptyRules.JOIN_RIGHT_INSTANCE,
                    PruneEmptyRules.SORT_FETCH_ZERO_INSTANCE,
                    DateRangeRules.FILTER_INSTANCE,
                    CoreRules.INTERSECT_TO_DISTINCT,
                    CoreRules.PROJECT_FILTER_TRANSPOSE,
                    CoreRules.FILTER_PROJECT_TRANSPOSE,
                    CoreRules.FILTER_INTO_JOIN,
                    CoreRules.JOIN_PUSH_EXPRESSIONS,
                    CoreRules.FILTER_AGGREGATE_TRANSPOSE,
                    CoreRules.PROJECT_WINDOW_TRANSPOSE,
                    CoreRules.JOIN_COMMUTE,
                    CoreRules.PROJECT_JOIN_TRANSPOSE,
                    CoreRules.SORT_PROJECT_TRANSPOSE,
                    CoreRules.SORT_JOIN_TRANSPOSE,
                    CoreRules.SORT_UNION_TRANSPOSE)
    );

    private ArrayList<RelOptRule> disabledRules = new ArrayList<>();

    public DaskPlanner(ArrayList<RelOptRule> disabledRules) {

        this.disabledRules = disabledRules;

        // Iterate through all rules and only add the ones not disabled
        for (RelOptRule rule : ALL_RULES) {
            if (!disabledRules.contains(rule)) {
                addRule(rule);
            }
        }

        // Enable conventions to turn from logical to dask
        addRelTraitDef(ConventionTraitDef.INSTANCE);

        // We do not want to execute any SQL
        setExecutor(null);

        // Use our defined type system and create a default CalciteConfigContext
        defaultContext = Contexts.of(CalciteConnectionConfig.DEFAULT.set(
                CalciteConnectionProperty.TYPE_SYSTEM, "com.dask.sql.application.DaskSqlDialect#DASKSQL_TYPE_SYSTEM"));
    }

    public Context getContext() {
        return defaultContext;
    }

    public ArrayList<RelOptRule> getAllRules() {
        return this.ALL_RULES;
    }

    public ArrayList<RelOptRule> getDisabledRules() {
        return this.disabledRules;
    }

    public ArrayList<RelOptRule> getEnabledRules() {
        ArrayList<RelOptRule> enabledRules = this.ALL_RULES;
        enabledRules.removeAll(this.disabledRules);
        return enabledRules;
    }
}
