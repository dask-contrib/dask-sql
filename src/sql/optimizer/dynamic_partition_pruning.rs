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
    hash::{Hash, Hasher},
};

use datafusion_python::{
    datafusion::parquet::{
        basic::Type as BasicType,
        file::reader::{FileReader, SerializedFileReader},
        record::{reader::RowIter, RowAccessor},
        schema::{parser::parse_message_type, types::Type},
    },
    datafusion_common::{Column, Result, ScalarValue},
    datafusion_expr::{
        expr::InList,
        logical_plan::LogicalPlan,
        Expr,
        JoinType,
        Operator,
        TableScan,
    },
    datafusion_optimizer::{OptimizerConfig, OptimizerRule},
};
use log::warn;

use crate::sql::table::DaskTableSource;

// Optimizer rule for dynamic partition pruning
pub struct DynamicPartitionPruning {}

impl DynamicPartitionPruning {
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
        // Parse the LogicalPlan and store tables and columns being (inner) joined upon. We do this
        // by creating a HashSet of all InnerJoins' join.on and join.filters
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
                let join_on = &join_cond.on;
                for on_i in join_on {
                    // Obtain tables and columns (fields) involved in join
                    let (left_on, right_on) = (&on_i.0, &on_i.1);
                    let (mut left_table, mut right_table) = (None, None);
                    let (mut left_field, mut right_field) = (None, None);

                    if let Expr::Column(c) = left_on {
                        left_table = Some(c.relation.clone().unwrap().to_string().clone());
                        left_field = Some(c.name.clone());
                    }
                    if let Expr::Column(c) = right_on {
                        right_table = Some(c.relation.clone().unwrap().to_string().clone());
                        right_field = Some(c.name.clone());
                    }

                    // For now, if it is not a join between columns then we skip the rule
                    // TODO: https://github.com/dask-contrib/dask-sql/issues/1121
                    if left_table.is_none() || right_table.is_none() {
                        continue;
                    }

                    let (mut left_table, mut right_table) =
                        (left_table.unwrap(), right_table.unwrap());
                    let (left_field, right_field) = (left_field.unwrap(), right_field.unwrap());

                    // TODO: Consider allowing the fact_dimension_ratio to be configured by the
                    // user. See issue: https://github.com/dask-contrib/dask-sql/issues/1121
                    let fact_dimension_ratio = 0.3;
                    let (mut left_filtered_table, mut right_filtered_table) = (None, None);

                    // Check if join uses an alias instead of the table name itself. Need to use
                    // the actual table name to obtain its filepath
                    let left_alias = aliases.get(&left_table.clone());
                    if let Some(t) = left_alias {
                        left_table = t.to_string()
                    }
                    let right_alias = aliases.get(&right_table.clone());
                    if let Some(t) = right_alias {
                        right_table = t.to_string()
                    }

                    // A more complicated alias, e.g. an alias for a nested select, means it's not
                    // obvious which file(s) should be read
                    if !tables.contains_key(&left_table) || !tables.contains_key(&right_table) {
                        continue;
                    }

                    // Determine whether a table is a fact or dimension table. If it's a dimension
                    // table, we should read it in and use the rule
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
                }
            }
            // Creates HashMap of all tables and field with their unique values to be set in the
            // TableScan
            let filter_values = combine_sets(join_values, join_tables, join_fields, fact_tables);
            // Optimize and return the plan
            optimize_table_scans(plan, filter_values)
        }
    }
}

/// Represents relevant information in an InnerJoin
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct JoinInfo {
    /// Equijoin clause expressed as pairs of (left, right) join expressions
    on: Vec<(Expr, Expr)>,
    /// Filters applied during join (non-equi conditions)
    /// TODO: https://github.com/dask-contrib/dask-sql/issues/1121
    filter: Option<Expr>,
}

// This function parses through the LogicalPlan, grabs relevant information from an InnerJoin, and
// adds them to a HashSet
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
                        let (left_joins, right_joins) =
                            (gather_joins(&j.left), gather_joins(&j.right));

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
                    let (left_joins, right_joins) = (gather_joins(&c.left), gather_joins(&c.right));

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

// This function parses through the LogicalPlan, grabs relevant information from a TableScan, and
// adds them to a HashMap where the key is the table name
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
                        // TODO: Add better handling for when a table is read in more than once
                        // https://github.com/dask-contrib/dask-sql/issues/1121
                        if tables.contains_key(&t.table_name.to_string()) {
                            return HashMap::new();
                        }

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
                    let (left_tables, right_tables) =
                        (gather_tables(&j.left), gather_tables(&j.right));

                    if check_table_overlaps(&tables, &left_tables, &right_tables) {
                        return HashMap::new();
                    }

                    // Add left_tables and right_tables to HashMap
                    tables.extend(left_tables);
                    tables.extend(right_tables);
                }
                LogicalPlan::CrossJoin(ref c) => {
                    // Recurse on left and right inputs of CrossJoin
                    let (left_tables, right_tables) =
                        (gather_tables(&c.left), gather_tables(&c.right));

                    if check_table_overlaps(&tables, &left_tables, &right_tables) {
                        return HashMap::new();
                    }

                    // Add left_tables and right_tables to HashMap
                    tables.extend(left_tables);
                    tables.extend(right_tables);
                }
                LogicalPlan::Union(ref u) => {
                    // Recurse on inputs vector of Union
                    for input in &u.inputs {
                        let union_tables = gather_tables(input);

                        // TODO: Add better handling for when a table is read in more than once
                        // https://github.com/dask-contrib/dask-sql/issues/1121
                        if tables.keys().any(|k| union_tables.contains_key(k))
                            || union_tables.keys().any(|k| tables.contains_key(k))
                        {
                            return HashMap::new();
                        }

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

// TODO: Add better handling for when a table is read in more than once
// https://github.com/dask-contrib/dask-sql/issues/1121
fn check_table_overlaps(
    m1: &HashMap<String, TableInfo>,
    m2: &HashMap<String, TableInfo>,
    m3: &HashMap<String, TableInfo>,
) -> bool {
    m1.keys().any(|k| m2.contains_key(k))
        || m2.keys().any(|k| m1.contains_key(k))
        || m1.keys().any(|k| m3.contains_key(k))
        || m3.keys().any(|k| m1.contains_key(k))
        || m2.keys().any(|k| m3.contains_key(k))
        || m3.keys().any(|k| m2.contains_key(k))
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

// This function parses through the LogicalPlan, grabs any aliases, and adds them to a HashMap
// where the key is the alias name and the value is the table name
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
                    let (left_aliases, right_aliases) =
                        (gather_aliases(&j.left), gather_aliases(&j.right));

                    // Add left_aliases and right_aliases to HashMap
                    aliases.extend(left_aliases);
                    aliases.extend(right_aliases);
                }
                LogicalPlan::CrossJoin(ref c) => {
                    // Recurse on left and right inputs of CrossJoin
                    let (left_aliases, right_aliases) =
                        (gather_aliases(&c.left), gather_aliases(&c.right));

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
                        aliases.insert(s.alias.to_string(), t.table_name.to_string().clone());
                    }
                    // Sometimes a TableScan is immediately followed by a Projection, so we can
                    // still use the alias for the table
                    LogicalPlan::Projection(ref p) => {
                        if let LogicalPlan::TableScan(ref t) = *p.input {
                            aliases.insert(s.alias.to_string(), t.table_name.to_string().clone());
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

// Wrapper for floats, since they are not hashable
#[derive(Clone, Copy, Debug, PartialEq, PartialOrd)]
struct FloatWrapper(f64);

impl Eq for FloatWrapper {}

impl Hash for FloatWrapper {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Convert the f64 to a u64 using transmute
        let bits: u64 = self.0.to_bits();
        // Use the u64's hash implementation
        bits.hash(state);
    }
}

// Wrapper for possible row value types
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum RowValue {
    String(Option<String>),
    Int64(Option<i64>),
    Int32(Option<i32>),
    Double(Option<FloatWrapper>),
}

// This function uses the table name, column name, and filters to read in the relevant columns,
// filter out row values, and construct a HashSet of relevant row values for the specified column,
// i.e., the column involved in the join
fn read_table(
    table_string: String,
    field_string: String,
    tables: HashMap<String, TableInfo>,
) -> Option<HashSet<RowValue>> {
    // Obtain filepaths to all relevant Parquet files, e.g., in a directory of Parquet files
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

    // A TableScan may include existing filters. These conditions should be used to filter the data
    // after being read. Therefore, the columns involved in these filters should be read in as well
    let filters = tables.get(&table_string).unwrap().filters.clone();
    let filtered_fields = get_filtered_fields(&filters, schema, field_string.clone());
    let filtered_string = filtered_fields.0;
    let filtered_types = filtered_fields.1;
    let filtered_names = filtered_fields.2;

    if filters.len() != filtered_names.len() {
        warn!("Unable to check existing filters for optimizer rule 'DynamicPartitionPruning'");
        return None;
    }

    // Specify which columns to include in the reader, then read in the rows
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

    let mut rows = Vec::new();
    for file in files {
        let reader_result = SerializedFileReader::try_from(&*file.clone());
        if let Ok(reader) = reader_result {
            let row_iter_result = RowIter::from_file_into(Box::new(reader))
                .project(projection.clone())
                .ok();
            if let Some(row_iter) = row_iter_result {
                rows.extend(row_iter.map(|r| r.expect("Parquet error encountered")));
            } else {
                // TODO: Investigate cases when this would happen
                rows.clear();
                break;
            }
        } else {
            rows.clear();
            break;
        }
    }
    if rows.is_empty() {
        return None;
    }

    // Create HashSets for the join column values
    let mut value_set: HashSet<RowValue> = HashSet::new();
    for row in rows {
        // Since a TableScan may have its own filters, we want to ensure that the values in
        // value_set satisfy the TableScan filters
        let mut satisfies_filters = true;
        let mut row_index = 0;
        for index in 0..filters.len() {
            if filtered_names[index] != field_string {
                let current_type = &filtered_types[index];
                match current_type.as_str() {
                    "BYTE_ARRAY" => {
                        let string_value = row.get_string(row_index).ok();
                        if !satisfies_string(string_value, filters[index].clone()) {
                            satisfies_filters = false;
                        }
                    }
                    "INT64" => {
                        let long_value = row.get_long(row_index).ok();
                        if !satisfies_int64(long_value, filters[index].clone()) {
                            satisfies_filters = false;
                        }
                    }
                    "INT32" => {
                        let int_value = row.get_int(row_index).ok();
                        if !satisfies_int32(int_value, filters[index].clone()) {
                            satisfies_filters = false;
                        }
                    }
                    "DOUBLE" => {
                        let double_value = row.get_double(row_index).ok();
                        if !satisfies_float(double_value, filters[index].clone()) {
                            satisfies_filters = false;
                        }
                    }
                    u => panic!("Unknown PhysicalType {u}"),
                }
                row_index += 1;
            }
        }
        // After verifying that the row satisfies all existing filters, we add the column value to
        // the HashSet
        if satisfies_filters {
            match physical_type.as_str() {
                "BYTE_ARRAY" => {
                    let r = row.get_string(row_index).ok();
                    value_set.insert(RowValue::String(r.cloned()));
                }
                "INT64" => {
                    let r = row.get_long(row_index).ok();
                    value_set.insert(RowValue::Int64(r));
                }
                "INT32" => {
                    let r = row.get_int(row_index).ok();
                    value_set.insert(RowValue::Int32(r));
                }
                "DOUBLE" => {
                    let r = row.get_double(row_index).ok();
                    if let Some(f) = r {
                        value_set.insert(RowValue::Double(Some(FloatWrapper(f))));
                    } else {
                        value_set.insert(RowValue::Double(None));
                    }
                }
                _ => panic!("Unknown PhysicalType"),
            }
        }
    }

    Some(value_set)
}

// A column has a physical_type (INT64, etc.) that needs to be included when specifying which
// columns to read in. To get the physical_type, we grab it from the schema
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

// A column has a repetition (i.e., REQUIRED or OPTIONAL) that needs to be included when specifying
// which columns to read in. To get the repetition, we grab it from the schema
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

// This is a helper function to deal with TableScan filters for reading in the data. The first
// value returned is a string representation of the projection used to read in the relevant
// columns. The second value returned is a vector of the physical_type of each column that has has
// a filter, in the order that they are being read. The third value returned is a vector of the
// column names, in the order that they are being read.
fn get_filtered_fields(
    filters: &Vec<Expr>,
    schema: &Type,
    field: String,
) -> (String, Vec<String>, Vec<String>) {
    // Used to create a string representation of the projection
    // for the TableScan filters to be read
    let mut filtered_fields = vec![];
    // All physical types involved in TableScan filters
    let mut filtered_types = vec![];
    // All columns involved in TableScan filters
    let mut filtered_columns = vec![];
    for filter in filters {
        match filter {
            Expr::BinaryExpr(b) => {
                if let Expr::Column(column) = &*b.left {
                    push_filtered_fields(
                        column,
                        schema,
                        field.clone(),
                        &mut filtered_fields,
                        &mut filtered_columns,
                        &mut filtered_types,
                    );
                }
            }
            Expr::IsNotNull(e) => {
                if let Expr::Column(column) = &**e {
                    push_filtered_fields(
                        column,
                        schema,
                        field.clone(),
                        &mut filtered_fields,
                        &mut filtered_columns,
                        &mut filtered_types,
                    );
                }
            }
            _ => (),
        }
    }
    (filtered_fields.join(""), filtered_types, filtered_columns)
}

// Helper function for get_filtered_fields
fn push_filtered_fields(
    column: &Column,
    schema: &Type,
    field: String,
    filtered_fields: &mut Vec<String>,
    filtered_columns: &mut Vec<String>,
    filtered_types: &mut Vec<String>,
) {
    let current_field = column.name.clone();
    let physical_type = get_physical_type(schema, current_field.clone())
        .unwrap()
        .to_string();
    if current_field != field {
        let repetition = get_repetition(schema, current_field.clone());
        filtered_fields.push(repetition.unwrap());
        filtered_fields.push(" ".to_string());

        filtered_fields.push(physical_type.clone());
        filtered_fields.push(" ".to_string());

        filtered_fields.push(current_field.clone());
        filtered_fields.push("; ".to_string());
    }
    filtered_types.push(physical_type);
    filtered_columns.push(current_field);
}

// Returns a boolean representing whether a string satisfies a given filter
fn satisfies_string(string_value: Option<&String>, filter: Expr) -> bool {
    match filter {
        Expr::BinaryExpr(b) => match b.op {
            Operator::Eq => Expr::Literal(ScalarValue::Utf8(string_value.cloned())) == *b.right,
            Operator::NotEq => Expr::Literal(ScalarValue::Utf8(string_value.cloned())) != *b.right,
            _ => {
                panic!("Unknown satisfies_string operator");
            }
        },
        Expr::IsNotNull(_) => string_value.is_some(),
        _ => {
            panic!("Unknown satisfies_string Expr");
        }
    }
}

// Returns a boolean representing whether an Int64 satisfies a given filter
fn satisfies_int64(long_value: Option<i64>, filter: Expr) -> bool {
    match filter {
        Expr::BinaryExpr(b) => {
            let filter_value = *b.right;
            let int_value: i64 = match filter_value {
                Expr::Literal(ScalarValue::Int64(i)) => i.unwrap(),
                Expr::Literal(ScalarValue::Int32(i)) => i64::from(i.unwrap()),
                Expr::Literal(ScalarValue::Float64(i)) => i.unwrap() as i64,
                Expr::Literal(ScalarValue::TimestampNanosecond(i, None)) => i.unwrap(),
                Expr::Literal(ScalarValue::Date32(i)) => i64::from(i.unwrap()),
                // TODO: Add logic to check if the string can be converted to a timestamp
                Expr::Literal(ScalarValue::Utf8(_)) => return false,
                _ => {
                    panic!("Unknown ScalarValue type {filter_value}");
                }
            };
            let filter_value = Expr::Literal(ScalarValue::Int64(Some(int_value)));
            match b.op {
                Operator::Eq => Expr::Literal(ScalarValue::Int64(long_value)) == filter_value,
                Operator::NotEq => Expr::Literal(ScalarValue::Int64(long_value)) != filter_value,
                Operator::Gt => Expr::Literal(ScalarValue::Int64(long_value)) > filter_value,
                Operator::Lt => Expr::Literal(ScalarValue::Int64(long_value)) < filter_value,
                Operator::GtEq => Expr::Literal(ScalarValue::Int64(long_value)) >= filter_value,
                Operator::LtEq => Expr::Literal(ScalarValue::Int64(long_value)) <= filter_value,
                _ => {
                    panic!("Unknown satisfies_int64 operator");
                }
            }
        }
        Expr::IsNotNull(_) => long_value.is_some(),
        _ => {
            panic!("Unknown satisfies_int64 Expr");
        }
    }
}

// Returns a boolean representing whether an Int32 satisfies a given filter
fn satisfies_int32(long_value: Option<i32>, filter: Expr) -> bool {
    match filter {
        Expr::BinaryExpr(b) => {
            let filter_value = *b.right;
            let int_value: i32 = match filter_value {
                Expr::Literal(ScalarValue::Int64(i)) => i.unwrap() as i32,
                Expr::Literal(ScalarValue::Int32(i)) => i.unwrap(),
                Expr::Literal(ScalarValue::Float64(i)) => i.unwrap() as i32,
                _ => {
                    panic!("Unknown ScalarValue type {filter_value}");
                }
            };
            let filter_value = Expr::Literal(ScalarValue::Int32(Some(int_value)));
            match b.op {
                Operator::Eq => Expr::Literal(ScalarValue::Int32(long_value)) == filter_value,
                Operator::NotEq => Expr::Literal(ScalarValue::Int32(long_value)) != filter_value,
                Operator::Gt => Expr::Literal(ScalarValue::Int32(long_value)) > filter_value,
                Operator::Lt => Expr::Literal(ScalarValue::Int32(long_value)) < filter_value,
                Operator::GtEq => Expr::Literal(ScalarValue::Int32(long_value)) >= filter_value,
                Operator::LtEq => Expr::Literal(ScalarValue::Int32(long_value)) <= filter_value,
                _ => {
                    panic!("Unknown satisfies_int32 operator");
                }
            }
        }
        Expr::IsNotNull(_) => long_value.is_some(),
        _ => {
            panic!("Unknown satisfies_int32 Expr");
        }
    }
}

// Returns a boolean representing whether an Float64 satisfies a given filter
fn satisfies_float(long_value: Option<f64>, filter: Expr) -> bool {
    match filter {
        Expr::BinaryExpr(b) => {
            let filter_value = *b.right;
            let float_value: f64 = match filter_value {
                Expr::Literal(ScalarValue::Int64(i)) => i.unwrap() as f64,
                Expr::Literal(ScalarValue::Int32(i)) => i.unwrap() as f64,
                Expr::Literal(ScalarValue::Float64(i)) => i.unwrap(),
                _ => {
                    panic!("Unknown ScalarValue type {filter_value}");
                }
            };
            let filter_value = Expr::Literal(ScalarValue::Float64(Some(float_value)));
            match b.op {
                Operator::Eq => Expr::Literal(ScalarValue::Float64(long_value)) == filter_value,
                Operator::NotEq => Expr::Literal(ScalarValue::Float64(long_value)) != filter_value,
                Operator::Gt => Expr::Literal(ScalarValue::Float64(long_value)) > filter_value,
                Operator::Lt => Expr::Literal(ScalarValue::Float64(long_value)) < filter_value,
                Operator::GtEq => Expr::Literal(ScalarValue::Float64(long_value)) >= filter_value,
                Operator::LtEq => Expr::Literal(ScalarValue::Float64(long_value)) <= filter_value,
                _ => {
                    panic!("Unknown satisfies_float operator");
                }
            }
        }
        Expr::IsNotNull(_) => long_value.is_some(),
        _ => {
            panic!("Unknown satisfies_float Expr");
        }
    }
}

// Used to simplify the signature of combine_sets
type RowHashSet = HashSet<RowValue>;
type RowOptionHashSet = Option<RowHashSet>;
type RowTuple = (RowOptionHashSet, RowOptionHashSet);
type RowVec = Vec<RowTuple>;

// Given a vector of hashsets to be set as TableScan filters, a vector of tuples representing the
// tables involved in a join, a vector of tuples representing the columns involved in a join, and
// a hashset of fact tables in the query; return a hashmap where the key is a tuple of the table
// and column names, and the value is the hashset representing the INLIST filter specified in the
// TableScan.
fn combine_sets(
    join_values: RowVec,
    join_tables: Vec<(String, String)>,
    join_fields: Vec<(String, String)>,
    fact_tables: HashSet<String>,
) -> HashMap<(String, String), HashSet<RowValue>> {
    let mut sets: HashMap<(String, String), HashSet<RowValue>> = HashMap::new();
    for i in 0..join_values.len() {
        // Case when we were able to read in both tables involved in the join
        if let (Some(set1), Some(set2)) = (&join_values[i].0, &join_values[i].1) {
            // The INLIST vector will be the intersection of both hashsets
            let set_intersection = set1.intersection(set2);
            let mut values = HashSet::new();
            for value in set_intersection {
                values.insert(value.clone());
            }

            let current_table = join_tables[i].0.clone();
            // We only create INLIST filters for fact tables
            if fact_tables.contains(&current_table) {
                let current_field = join_fields[i].0.clone();
                add_to_existing_set(&mut sets, values.clone(), current_table, current_field);
            }

            let current_table = join_tables[i].1.clone();
            // We only create INLIST filters for fact tables
            if fact_tables.contains(&current_table) {
                let current_field = join_fields[i].1.clone();
                add_to_existing_set(&mut sets, values.clone(), current_table, current_field);
            }
        // Case when we were only able to read in the left table of the join
        } else if let Some(values) = &join_values[i].0 {
            let current_table = join_tables[i].0.clone();
            // We only create INLIST filters for fact tables
            if fact_tables.contains(&current_table) {
                let current_field = join_fields[i].0.clone();
                add_to_existing_set(&mut sets, values.clone(), current_table, current_field);
            }

            let current_table = join_tables[i].1.clone();
            // We only create INLIST filters for fact tables
            if fact_tables.contains(&current_table) {
                let current_field = join_fields[i].1.clone();
                add_to_existing_set(&mut sets, values.clone(), current_table, current_field);
            }
        // Case when we were only able to read in the right table of the join
        } else if let Some(values) = &join_values[i].1 {
            let current_table = join_tables[i].0.clone();
            // We only create INLIST filters for fact tables
            if fact_tables.contains(&current_table) {
                let current_field = join_fields[i].0.clone();
                add_to_existing_set(&mut sets, values.clone(), current_table, current_field);
            }

            let current_table = join_tables[i].1.clone();
            // We only create INLIST filters for fact tables
            if fact_tables.contains(&current_table) {
                let current_field = join_fields[i].1.clone();
                add_to_existing_set(&mut sets, values.clone(), current_table, current_field);
            }
        }
    }
    sets
}

// Given a mutable hashmap (the hashmap which will eventually be returned by the `combine_sets`
// function), a hashset of values, a table name, and a column name; insert the hashset of values
// into the hashmap, where the key is a tuple of the table and column names.
fn add_to_existing_set(
    sets: &mut HashMap<(String, String), HashSet<RowValue>>,
    values: HashSet<RowValue>,
    current_table: String,
    current_field: String,
) {
    let existing_set = sets.get(&(current_table.clone(), current_field.clone()));
    match existing_set {
        // If the tuple for (current_table, current_field) already exists, then we want to combine
        // the existing set with the new hashset being inserted; to do this, we take the
        // intersection of both sets.
        Some(s) => {
            let s = s.clone();
            let v = values.iter().cloned().collect::<HashSet<RowValue>>();
            let s = s.intersection(&v);
            let mut set_intersection = HashSet::new();
            for i in s {
                set_intersection.insert(i.clone());
            }
            sets.insert((current_table, current_field), set_intersection.clone());
        }
        // If the tuple for (current_table, current_field) does not already exist as a key in the
        // hashmap, then simply create it and set the hashset as the value
        None => {
            sets.insert((current_table, current_field), values);
        }
    }
}

// Given a LogicalPlan and a hashmap where the key is a tuple containing a table name and column
// and the value is a hashset of unique row values, parse the LogicalPlan and insert INLIST filters
// at the TableScan level.
fn optimize_table_scans(
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
                if let Some(e) = current_expr {
                    updated_filters.push(e);
                }
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

// Given a hashset of values, a table name, and a column name, return a DataFusion INLIST Expr
fn format_inlist_expr(
    value_set: HashSet<RowValue>,
    join_table: String,
    join_field: String,
) -> Option<Expr> {
    let expr = Box::new(Expr::Column(Column::new(Some(join_table), join_field)));
    let mut list: Vec<Expr> = vec![];

    // Need to correctly format the ScalarValue type
    for value in value_set {
        if let RowValue::String(s) = value {
            if s.is_some() {
                let v = Expr::Literal(ScalarValue::Utf8(s));
                list.push(v);
            }
        } else if let RowValue::Int64(l) = value {
            if l.is_some() {
                let v = Expr::Literal(ScalarValue::Int64(l));
                list.push(v);
            }
        } else if let RowValue::Int32(i) = value {
            if i.is_some() {
                let v = Expr::Literal(ScalarValue::Int32(i));
                list.push(v);
            }
        } else if let RowValue::Double(Some(f)) = value {
            let v = Expr::Literal(ScalarValue::Float64(Some(f.0)));
            list.push(v);
        }
    }

    if list.is_empty() {
        None
    } else {
        Some(Expr::InList(InList {
            expr,
            list,
            negated: false,
        }))
    }
}

// Given a LogicalPlan and the same hashmap as the `optimize_table_scans` function, correctly
// iterate through the LogicalPlan nodes. Similar to DataFusion's `optimize_children` function, but
// recurses on the `optimize_table_scans` function instead.
fn optimize_children(
    plan: &LogicalPlan,
    filter_values: HashMap<(String, String), HashSet<RowValue>>,
) -> Result<Option<LogicalPlan>> {
    let new_exprs = plan.expressions();
    let mut new_inputs = Vec::with_capacity(plan.inputs().len());
    let mut plan_is_changed = false;
    for input in plan.inputs() {
        let new_input = optimize_table_scans(input, filter_values.clone())?;
        plan_is_changed = plan_is_changed || new_input.is_some();
        new_inputs.push(new_input.unwrap_or_else(|| input.clone()))
    }
    if plan_is_changed {
        Ok(Some(plan.with_new_exprs(new_exprs, &new_inputs)?))
    } else {
        Ok(None)
    }
}
