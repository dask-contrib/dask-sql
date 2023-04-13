//! Optimizer rule for dynamic partition pruning (DPP)
//!
//! DPP refers to a query optimization rule in which distinct values in an inner join are used as
//! filters in a table scan. This allows us to eliminate all other rows which do not fit the join
//! condition from being read at all.
//!
//! Furthermore, a table involved in a join may be filtered during a scan, which allows us to
//! further prune the values to be read.

use std::{
    collections::{HashMap, HashSet},
    fs,
};

use datafusion::parquet::{
    basic::Type as BasicType,
    file::reader::{FileReader, SerializedFileReader},
    record::{reader::RowIter, RowAccessor},
    schema::{parser::parse_message_type, types::Type},
};
use datafusion_common::{Column, Result, ScalarValue};
use datafusion_expr::{
    logical_plan::LogicalPlan,
    utils::from_plan,
    Expr,
    JoinType,
    Operator,
    TableScan,
};
use datafusion_optimizer::{OptimizerConfig, OptimizerRule};
use log::warn;

use crate::sql::table::DaskTableSource;

// Optimizer rule for dynamic partition pruning
// General TODOs:
// - Should only run the rule once
// - Add more explanatory comments
// - Replace repeating code with functions
// - Remove unnecessary `clone()`s and consider using references instead
// - Remove unncessary `Option`s and `Result`s
// - Check against all queries

pub struct DynamicPartitionPruning {}

impl DynamicPartitionPruning {
    #[allow(missing_docs)]
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for DynamicPartitionPruning {
    fn name(&self) -> &str {
        "dynamic_partition_pruning"
    }

    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        // Parse the LogicalPlan and store tables and columns being (inner) joined upon.
        // We do this by creating a HashSet of all InnerJoins' join.on and join.filters
        let join_conds = gather_joins(plan);
        let tables = gather_tables(plan);
        let aliases = gather_aliases(plan);

        if join_conds.is_empty() || tables.is_empty() {
            // No InnerJoins to optimize with
            Ok(None)
        } else {
            // Find the size of the largest table in the query
            let mut largest_size = 1_f64;
            for table in &tables {
                let table_size = table.1.size.unwrap_or(0) as f64;
                if table_size > largest_size {
                    largest_size = table_size;
                }
            }

            let mut join_values = vec![];
            let mut join_tables = vec![];
            let mut join_fields = vec![];
            let mut fact_tables = HashSet::new();

            // Iterate through all inner joins in the query
            for join_cond in &join_conds {
                let on = &join_cond.on;
                if on.len() == 1 {
                    // Obtain tables and columns (fields) involved in join
                    let left_on = &on[0].0;
                    let right_on = &on[0].1;
                    let mut left_table: Option<String> = None;
                    let mut left_field: Option<String> = None;
                    let mut right_table: Option<String> = None;
                    let mut right_field: Option<String> = None;

                    if let Expr::Column(c) = left_on {
                        left_table = Some(c.relation.clone().unwrap().to_string().clone());
                        left_field = Some(c.name.clone());
                    }
                    if let Expr::Column(c) = right_on {
                        right_table = Some(c.relation.clone().unwrap().to_string().clone());
                        right_field = Some(c.name.clone());
                    }

                    let mut left_table = left_table.unwrap();
                    let left_field = left_field.unwrap();
                    let mut right_table = right_table.unwrap();
                    let right_field = right_field.unwrap();

                    let fact_dimension_ratio = 0.3;
                    let mut left_filtered_table = None;
                    let mut right_filtered_table = None;

                    // Check if join uses an alias instead of the table name itself.
                    // Need to use the actual table name to obtain its filepath
                    let left_alias = aliases.get(&left_table.clone());
                    if let Some(t) = left_alias {
                        left_table = t.to_string()
                    }
                    let right_alias = aliases.get(&right_table.clone());
                    if let Some(t) = right_alias {
                        right_table = t.to_string()
                    }

                    // Determine whether a table is a fact or dimension table
                    // If it's a dimension table, we should read it in and use the rule
                    if tables
                        .get(&left_table.clone())
                        .unwrap()
                        .size
                        .unwrap_or(largest_size as usize) as f64
                        / largest_size
                        < fact_dimension_ratio
                    {
                        left_filtered_table =
                            read_table(left_table.clone(), left_field.clone(), tables.clone());
                    } else {
                        fact_tables.insert(left_table.clone());
                    }
                    if tables
                        .get(&right_table.clone())
                        .unwrap()
                        .size
                        .unwrap_or(largest_size as usize) as f64
                        / largest_size
                        < fact_dimension_ratio
                    {
                        right_filtered_table =
                            read_table(right_table.clone(), right_field.clone(), tables.clone());
                    } else {
                        fact_tables.insert(right_table.clone());
                    }

                    join_values.push((left_filtered_table, right_filtered_table));
                    join_tables.push((left_table, right_table));
                    join_fields.push((left_field, right_field));
                } else {
                    // TODO: Need to reason about more than one join condition
                }
            }
            // Creates HashMap of all tables and fields
            // with their unique values to be set in the TableScan
            let filter_values = combine_sets(join_values, join_tables, join_fields, fact_tables);
            // Optimize and return the plan
            parse_and_optimize(plan, filter_values)
        }
    }
}

/// Represents relevant information in an InnerJoin
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct JoinInfo {
    /// Equijoin clause expressed as pairs of (left, right) join expressions
    on: Vec<(Expr, Expr)>,
    /// Filters applied during join (non-equi conditions)
    filter: Option<Expr>,
}

fn gather_joins(plan: &LogicalPlan) -> HashSet<JoinInfo> {
    let mut current_plan = plan.clone();
    let mut join_info = HashSet::new();
    loop {
        if current_plan.inputs().is_empty() {
            break;
        } else if current_plan.inputs().len() > 1 {
            match current_plan {
                LogicalPlan::Join(ref j) => {
                    if j.join_type == JoinType::Inner {
                        // Store tables and columns that are being (inner) joined upon
                        let info = JoinInfo {
                            on: j.on.clone(),
                            filter: j.filter.clone(),
                        };
                        join_info.insert(info);

                        // Recurse on left and right inputs of Join
                        let left_joins = gather_joins(&j.left);
                        let right_joins = gather_joins(&j.right);

                        // Add left_joins and right_joins to HashSet
                        join_info.extend(left_joins);
                        join_info.extend(right_joins);
                    } else {
                        // We don't run the rule if there are non-inner joins in the query
                        return HashSet::new();
                    }
                }
                LogicalPlan::CrossJoin(ref c) => {
                    // Recurse on left and right inputs of CrossJoin
                    let left_joins = gather_joins(&c.left);
                    let right_joins = gather_joins(&c.right);

                    // Add left_joins and right_joins to HashSet
                    join_info.extend(left_joins);
                    join_info.extend(right_joins);
                }
                LogicalPlan::Union(ref u) => {
                    // Recurse on inputs vector of Union
                    for input in &u.inputs {
                        let joins = gather_joins(input);

                        // Add joins to HashSet
                        join_info.extend(joins);
                    }
                }
                _ => {
                    warn!("Skipping optimizer rule 'DynamicPartitionPruning'");
                    return HashSet::new();
                }
            }
            break;
        } else {
            // Move on to next step
            current_plan = current_plan.inputs()[0].clone();
        }
    }
    join_info
}

/// Represents relevant information in a TableScan
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct TableInfo {
    /// The name of the table
    table_name: String,
    /// The path and filename of the table
    filepath: String,
    /// The number of rows in the table
    size: Option<usize>,
    /// Optional expressions to be used as filters by the table provider
    filters: Vec<Expr>,
}

fn gather_tables(plan: &LogicalPlan) -> HashMap<String, TableInfo> {
    let mut current_plan = plan.clone();
    let mut tables = HashMap::new();
    loop {
        if current_plan.inputs().is_empty() {
            if let LogicalPlan::TableScan(ref t) = current_plan {
                // Use TableScan to get the filepath and/or size
                let filepath = get_filepath(&current_plan);
                let size = get_table_size(&current_plan);
                match filepath {
                    Some(f) => {
                        tables.insert(
                            t.table_name.to_string(),
                            TableInfo {
                                table_name: t.table_name.to_string(),
                                filepath: f.clone(),
                                size,
                                filters: t.filters.clone(),
                            },
                        );
                        break;
                    }
                    None => return HashMap::new(),
                }
            }
            break;
        } else if current_plan.inputs().len() > 1 {
            match current_plan {
                LogicalPlan::Join(ref j) => {
                    // Recurse on left and right inputs of Join
                    let left_tables = gather_tables(&j.left);
                    let right_tables = gather_tables(&j.right);

                    // Add left_tables and right_tables to HashMap
                    tables.extend(left_tables);
                    tables.extend(right_tables);
                }
                LogicalPlan::CrossJoin(ref c) => {
                    // Recurse on left and right inputs of CrossJoin
                    let left_tables = gather_tables(&c.left);
                    let right_tables = gather_tables(&c.right);

                    // Add left_tables and right_tables to HashMap
                    tables.extend(left_tables);
                    tables.extend(right_tables);
                }
                LogicalPlan::Union(ref u) => {
                    // Recurse on inputs vector of Union
                    for input in &u.inputs {
                        let union_tables = gather_tables(input);

                        // Add union_tables to HashMap
                        tables.extend(union_tables);
                    }
                }
                _ => {
                    warn!("Skipping optimizer rule 'DynamicPartitionPruning'");
                    return HashMap::new();
                }
            }
            break;
        } else {
            // Move on to next step
            current_plan = current_plan.inputs()[0].clone();
        }
    }
    tables
}

fn get_filepath(plan: &LogicalPlan) -> Option<&String> {
    match plan {
        LogicalPlan::TableScan(scan) => scan
            .source
            .as_any()
            .downcast_ref::<DaskTableSource>()?
            .filepath(),
        _ => None,
    }
}

fn get_table_size(plan: &LogicalPlan) -> Option<usize> {
    match plan {
        LogicalPlan::TableScan(scan) => scan
            .source
            .as_any()
            .downcast_ref::<DaskTableSource>()?
            .statistics()
            .map(|stats| stats.get_row_count() as usize),
        _ => None,
    }
}

fn gather_aliases(plan: &LogicalPlan) -> HashMap<String, String> {
    let mut current_plan = plan.clone();
    let mut aliases = HashMap::new();
    loop {
        if current_plan.inputs().is_empty() {
            break;
        } else if current_plan.inputs().len() > 1 {
            match current_plan {
                LogicalPlan::Join(ref j) => {
                    // Recurse on left and right inputs of Join
                    let left_aliases = gather_aliases(&j.left);
                    let right_aliases = gather_aliases(&j.right);

                    // Add left_aliases and right_aliases to HashMap
                    aliases.extend(left_aliases);
                    aliases.extend(right_aliases);
                }
                LogicalPlan::CrossJoin(ref c) => {
                    // Recurse on left and right inputs of CrossJoin
                    let left_aliases = gather_aliases(&c.left);
                    let right_aliases = gather_aliases(&c.right);

                    // Add left_aliases and right_aliases to HashMap
                    aliases.extend(left_aliases);
                    aliases.extend(right_aliases);
                }
                LogicalPlan::Union(ref u) => {
                    // Recurse on inputs vector of Union
                    for input in &u.inputs {
                        let union_aliases = gather_aliases(input);

                        // Add union_aliases to HashMap
                        aliases.extend(union_aliases);
                    }
                }
                _ => {
                    return HashMap::new();
                }
            }
            break;
        } else {
            if let LogicalPlan::SubqueryAlias(ref s) = current_plan {
                match *s.input {
                    LogicalPlan::TableScan(ref t) => {
                        aliases.insert(s.alias.clone(), t.table_name.to_string().clone());
                    }
                    LogicalPlan::Projection(ref p) => {
                        if let LogicalPlan::TableScan(ref t) = *p.input {
                            aliases.insert(s.alias.clone(), t.table_name.to_string().clone());
                        }
                    }
                    _ => (),
                }
            }
            // Move on to next step
            current_plan = current_plan.inputs()[0].clone();
        }
    }
    aliases
}

fn read_table(
    table_string: String,
    field_string: String,
    tables: HashMap<String, TableInfo>,
) -> Option<HashSet<RowValue>> {
    // Obtain filepaths to all relevant Parquet files
    // e.g., in a directory of Parquet files
    let paths = fs::read_dir(tables.get(&table_string).unwrap().filepath.clone()).unwrap();
    let mut files = vec![];
    for path in paths {
        files.push(path.unwrap().path().display().to_string())
    }

    // Using the filepaths to the Parquet tables, obtain the schemas of the relevant tables
    let schema: &Type = &SerializedFileReader::try_from(files[0].clone())
        .unwrap()
        .metadata()
        .file_metadata()
        .schema()
        .clone();

    // Use the schemas of the relevant tables to obtain the physical type of the relevant columns
    let physical_type = get_physical_type(schema, field_string.clone());

    let filters = tables.get(&table_string).unwrap().filters.clone();
    let filtered_fields = get_filtered_fields(&filters, schema, field_string.clone());
    let filtered_string = filtered_fields.0;
    let filtered_types = filtered_fields.1;
    let filtered_names = filtered_fields.2;
    // TODO: Add more logic so that this check isn't necessary
    if filters.len() != filtered_names.len() {
        return None;
    }

    // Specify which column to include in the reader, then read in the rows
    let repetition = get_repetition(schema, field_string.clone());
    let physical_type = physical_type.unwrap().to_string();
    let projection_schema = "message schema { ".to_owned()
        + &filtered_string
        + &repetition.unwrap()
        + " "
        + &physical_type
        + " "
        + &field_string
        + "; }";
    let projection = parse_message_type(&projection_schema).ok();
    let rows = files
        .iter()
        .map(|p| SerializedFileReader::try_from(&*p.clone()).unwrap())
        .flat_map(|r| {
            RowIter::from_file_into(Box::new(r))
                .project(projection.clone())
                .unwrap()
        });

    // Create HashSets for the join column values
    let mut value_set: HashSet<RowValue> = HashSet::new();
    for row in rows {
        // Since a TableScan may have its own filters, we want to ensure that
        // the values in value_set satisfy the TableScan filters
        let mut satisfies_filters = true;
        let mut row_index = 0;
        for index in 0..filters.len() {
            if filtered_names[index] != field_string {
                let current_type = &filtered_types[index];
                match current_type.as_str() {
                    "BYTE_ARRAY" => {
                        let string_value = row.get_string(row_index);
                        // TODO: Handle Err(_) and/or Null case?
                        if let Ok(s) = string_value {
                            if !satisfies_string(s, filters[index].clone()) {
                                satisfies_filters = false;
                            }
                        }
                    }
                    "INT64" => {
                        let long_value = row.get_long(row_index);
                        // TODO: Handle Err(_) case?
                        if let Ok(l) = long_value {
                            if !satisfies_long(l, filters[index].clone()) {
                                satisfies_filters = false;
                            }
                        }
                    }
                    "INT32" => {
                        let int_value = row.get_int(row_index);
                        if let Ok(i) = int_value {
                            if !satisfies_long(i as i64, filters[index].clone()) {
                                satisfies_filters = false;
                            }
                        }
                    }
                    _ => panic!("Unknown PhysicalType"),
                }
                row_index += 1;
            }
        }
        if satisfies_filters {
            match physical_type.as_str() {
                "BYTE_ARRAY" => {
                    let r = row.get_string(row_index).unwrap();
                    value_set.insert(RowValue::String(r.to_string()));
                }
                "INT64" => {
                    let r = row.get_long(row_index).unwrap();
                    value_set.insert(RowValue::Int64(r));
                }
                "INT32" => {
                    let r = row.get_int(row_index).unwrap();
                    value_set.insert(RowValue::Int32(r));
                }
                _ => panic!("Unknown PhysicalType"),
            }
        }
    }

    Some(value_set)
}

fn get_physical_type(schema: &Type, field: String) -> Option<BasicType> {
    match schema {
        Type::GroupType {
            basic_info: _,
            fields,
        } => {
            for f in fields {
                let match_field = &*f.clone();
                match match_field {
                    Type::PrimitiveType {
                        basic_info,
                        physical_type,
                        ..
                    } => {
                        if basic_info.name() == field {
                            return Some(*physical_type);
                        }
                    }
                    _ => return None,
                }
            }
            None
        }
        _ => None,
    }
}

fn get_repetition(schema: &Type, field: String) -> Option<String> {
    match schema {
        Type::GroupType {
            basic_info: _,
            fields,
        } => {
            for f in fields {
                let match_field = &*f.clone();
                match match_field {
                    Type::PrimitiveType { basic_info, .. } => {
                        if basic_info.name() == field {
                            return Some(basic_info.repetition().to_string());
                        }
                    }
                    _ => return None,
                }
            }
            None
        }
        _ => None,
    }
}

fn get_filtered_fields(
    filters: &Vec<Expr>,
    schema: &Type,
    field: String,
) -> (String, Vec<String>, Vec<String>) {
    // Used to create a string representation of the projection
    // for the TableScan filters to be read
    let mut filtered_fields = vec![];
    // All columns involved in TableScan filters
    let mut filtered_columns = vec![];
    // All physical types involved in TableScan filters
    let mut filtered_types = vec![];
    for filter in filters {
        match filter {
            Expr::BinaryExpr(b) => {
                // TODO: Handle nested BinaryExprs
                if let Expr::Column(c) = &*b.left {
                    let current_field = c.name.clone();
                    let physical_type = get_physical_type(schema, c.name.clone())
                        .unwrap()
                        .to_string();
                    if current_field != field {
                        let repetition = get_repetition(schema, c.name.clone());
                        filtered_fields.push(repetition.unwrap());
                        filtered_fields.push(" ".to_string());

                        filtered_fields.push(physical_type.clone());
                        filtered_fields.push(" ".to_string());

                        filtered_fields.push(current_field.clone());
                        filtered_fields.push("; ".to_string());
                    }
                    filtered_columns.push(current_field);
                    filtered_types.push(physical_type);
                }
            }
            Expr::IsNotNull(e) => {
                if let Expr::Column(c) = &**e {
                    let current_field = c.name.clone();
                    let physical_type = get_physical_type(schema, c.name.clone())
                        .unwrap()
                        .to_string();
                    if current_field != field {
                        let repetition = get_repetition(schema, c.name.clone());
                        filtered_fields.push(repetition.unwrap());
                        filtered_fields.push(" ".to_string());

                        filtered_fields.push(physical_type.clone());
                        filtered_fields.push(" ".to_string());

                        filtered_fields.push(current_field.clone());
                        filtered_fields.push("; ".to_string());
                    }
                    filtered_columns.push(current_field);
                    filtered_types.push(physical_type);
                }
            }
            _ => (),
        }
    }
    (filtered_fields.join(""), filtered_types, filtered_columns)
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum RowValue {
    String(String),
    Int64(i64),
    Int32(i32),
}

fn satisfies_string(string_value: &String, filter: Expr) -> bool {
    match filter {
        Expr::BinaryExpr(b) => match b.op {
            Operator::Eq => {
                Expr::Literal(ScalarValue::Utf8(Some(string_value.to_string()))) == *b.right
            }
            Operator::NotEq => {
                Expr::Literal(ScalarValue::Utf8(Some(string_value.to_string()))) != *b.right
            }
            _ => {
                panic!("Unknown satisfies_string operator");
            }
        },
        _ => {
            panic!("Unknown satisfies_string Expr");
        }
    }
}

fn satisfies_long(long_value: i64, filter: Expr) -> bool {
    match filter {
        Expr::BinaryExpr(b) => match b.op {
            Operator::Eq => Expr::Literal(ScalarValue::Int64(Some(long_value))) == *b.right,
            Operator::NotEq => Expr::Literal(ScalarValue::Int64(Some(long_value))) != *b.right,
            Operator::Gt => Expr::Literal(ScalarValue::Int64(Some(long_value))) > *b.right,
            Operator::Lt => Expr::Literal(ScalarValue::Int64(Some(long_value))) < *b.right,
            Operator::GtEq => Expr::Literal(ScalarValue::Int64(Some(long_value))) >= *b.right,
            Operator::LtEq => Expr::Literal(ScalarValue::Int64(Some(long_value))) <= *b.right,
            _ => {
                panic!("Unknown satisfies_long operator");
            }
        },
        _ => {
            panic!("Unknown satisfies_long Expr");
        }
    }
}

type RowHashSet = HashSet<RowValue>;
type RowOptionHashSet = Option<RowHashSet>;
type RowTuple = (RowOptionHashSet, RowOptionHashSet);
type RowVec = Vec<RowTuple>;

fn combine_sets(
    join_values: RowVec,
    join_tables: Vec<(String, String)>,
    join_fields: Vec<(String, String)>,
    fact_tables: HashSet<String>,
) -> HashMap<(String, String), HashSet<RowValue>> {
    let mut sets: HashMap<(String, String), HashSet<RowValue>> = HashMap::new();
    for i in 0..join_values.len() {
        if let (Some(set1), Some(set2)) = (&join_values[i].0, &join_values[i].1) {
            let set_intersection = set1.intersection(set2);
            let mut values = HashSet::new();
            for value in set_intersection {
                values.insert(value.clone());
            }

            let current_table = join_tables[i].0.clone();
            if fact_tables.contains(&current_table) {
                let current_field = join_fields[i].0.clone();
                add_to_existing_set(&mut sets, values.clone(), current_table, current_field);
            }

            let current_table = join_tables[i].1.clone();
            if fact_tables.contains(&current_table) {
                let current_field = join_fields[i].1.clone();
                add_to_existing_set(&mut sets, values.clone(), current_table, current_field);
            }
        } else if let Some(values) = &join_values[i].0 {
            let current_table = join_tables[i].0.clone();
            if fact_tables.contains(&current_table) {
                let current_field = join_fields[i].0.clone();
                add_to_existing_set(&mut sets, values.clone(), current_table, current_field);
            }

            let current_table = join_tables[i].1.clone();
            if fact_tables.contains(&current_table) {
                let current_field = join_fields[i].1.clone();
                add_to_existing_set(&mut sets, values.clone(), current_table, current_field);
            }
        } else if let Some(values) = &join_values[i].1 {
            let current_table = join_tables[i].0.clone();
            if fact_tables.contains(&current_table) {
                let current_field = join_fields[i].0.clone();
                add_to_existing_set(&mut sets, values.clone(), current_table, current_field);
            }

            let current_table = join_tables[i].1.clone();
            if fact_tables.contains(&current_table) {
                let current_field = join_fields[i].1.clone();
                add_to_existing_set(&mut sets, values.clone(), current_table, current_field);
            }
        }
    }
    sets
}

fn add_to_existing_set(
    sets: &mut HashMap<(String, String), HashSet<RowValue>>,
    values: HashSet<RowValue>,
    current_table: String,
    current_field: String,
) {
    let existing_set = sets.get(&(current_table.clone(), current_field.clone()));
    match existing_set {
        Some(s) => {
            let mut s = s.clone();
            // TODO: intersect instead of extend
            s.extend(values.iter().cloned().collect::<HashSet<RowValue>>());
            sets.insert((current_table, current_field), s.clone());
        }
        None => {
            sets.insert((current_table, current_field), values);
        }
    }
}

fn parse_and_optimize(
    plan: &LogicalPlan,
    filter_values: HashMap<(String, String), HashSet<RowValue>>,
) -> Result<Option<LogicalPlan>> {
    // Replaces existing TableScan with a new TableScan which includes
    // the new binary expression filter created from reading in the join columns
    match plan {
        LogicalPlan::TableScan(t) => {
            let table_name = t.table_name.to_string();
            let table_filters: HashMap<(String, String), HashSet<RowValue>> = filter_values
                .iter()
                .filter(|(key, _value)| key.0 == table_name)
                .map(|(key, value)| ((key.0.to_owned(), key.1.to_owned()), value.clone()))
                .collect();
            let mut updated_filters = t.filters.clone();
            for (key, value) in table_filters.iter() {
                let current_expr =
                    format_inlist_expr(value.clone(), key.0.to_owned(), key.1.to_owned());
                updated_filters.push(current_expr);
            }
            let scan = LogicalPlan::TableScan(TableScan {
                table_name: t.table_name.clone(),
                source: t.source.clone(),
                projection: t.projection.clone(),
                projected_schema: t.projected_schema.clone(),
                filters: updated_filters,
                fetch: t.fetch,
            });
            Ok(Some(scan))
        }
        _ => optimize_children(plan, filter_values),
    }
}

fn format_inlist_expr(
    value_set: HashSet<RowValue>,
    join_table: String,
    join_field: String,
) -> Expr {
    let expr = Box::new(Expr::Column(Column::new(
        Some(join_table),
        join_field,
    )));
    let mut list: Vec<Expr> = vec![];

    for value in value_set {
        if let RowValue::String(s) = value {
            let v = Expr::Literal(ScalarValue::Utf8(Some(s.to_string())));
            list.push(v);
        } else if let RowValue::Int64(l) = value {
            let v = Expr::Literal(ScalarValue::Int64(Some(l)));
            list.push(v);
        } else if let RowValue::Int32(i) = value {
            let v = Expr::Literal(ScalarValue::Int32(Some(i)));
            list.push(v);
        }
    }

    Expr::InList {
        expr,
        list,
        negated: false,
    }
}

fn optimize_children(
    plan: &LogicalPlan,
    filter_values: HashMap<(String, String), HashSet<RowValue>>,
) -> Result<Option<LogicalPlan>> {
    // Similar to DataFusion's `optimize_children` function,
    // but recurses on the `parse_and_optimize` function instead
    let new_exprs = plan.expressions();
    let mut new_inputs = Vec::with_capacity(plan.inputs().len());
    let mut plan_is_changed = false;
    for input in plan.inputs() {
        let new_input = parse_and_optimize(input, filter_values.clone())?;
        plan_is_changed = plan_is_changed || new_input.is_some();
        new_inputs.push(new_input.unwrap_or_else(|| input.clone()))
    }
    if plan_is_changed {
        Ok(Some(from_plan(plan, &new_exprs, &new_inputs)?))
    } else {
        Ok(None)
    }
}

// TODO: Add Rust tests
