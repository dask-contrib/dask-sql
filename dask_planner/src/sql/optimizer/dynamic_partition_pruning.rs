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
    file::reader::{FileReader, SerializedFileReader},
    record::{reader::RowIter, RowAccessor},
    schema::{parser::parse_message_type, types::Type},
};
use datafusion_common::{Column, Result};
use datafusion_expr::{
    logical_plan::LogicalPlan,
    utils::from_plan,
    BinaryExpr,
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
// - Add more explanatory comments
// - Change variable names starting with `my_`
// - Replace repeating code with functions
// - Use imports instead of long abc::def::ghi::jkl
// - Remove unnecessary `clone()`s and consider using references instead
// - Remove unncessary `Option`s and `Result`s
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
        // Parse the LogicalPlan and store tables and columns are being (inner) joined upon.
        // We do this by creating a HashSet of all InnerJoins' join.on and join.filters
        let join_conds = gather_joins(plan);
        let tables = gather_tables(plan);
        let aliases = gather_aliases(plan);

        if join_conds.is_empty() || tables.is_empty() {
            // No InnerJoins to optimize with
            Ok(None)
        } else {
            let mut largest_size = 1_f64;
            for table in &tables {
                let table_size = table.1.size.unwrap_or(0) as f64;
                if table_size > largest_size {
                    largest_size = table_size;
                }
            }

            for join_cond in &join_conds {
                let on = &join_cond.on;
                if on.len() == 1 {
                    // Obtain tables and columns involved in join
                    let left_on = &on[0].0;
                    let right_on = &on[0].1;
                    let mut left_table: Option<String> = None;
                    let mut left_field: Option<String> = None;
                    let mut right_table: Option<String> = None;
                    let mut right_field: Option<String> = None;

                    if let Expr::Column(c) = left_on {
                        left_table = c.relation.clone();
                        left_field = Some(c.name.clone());
                    }
                    if let Expr::Column(c) = right_on {
                        right_table = c.relation.clone();
                        right_field = Some(c.name.clone());
                    }

                    let fact_dimension_ratio = 0.3;
                    let mut left_filtered_table = None;
                    let mut right_filtered_table = None;

                    let left_alias = aliases.get(&left_table.clone().unwrap());
                    if let Some(t) = left_alias { left_table = Some(t.to_string()) }
                    let right_alias = aliases.get(&right_table.clone().unwrap());
                    if let Some(t) = right_alias { right_table = Some(t.to_string()) }

                    // Determine whether a table is a fact or dimension table
                    // If it's a dimension table, we should read it in and use the rule
                    if tables
                        .get(&left_table.clone().unwrap())
                        .unwrap()
                        .size
                        .unwrap_or(largest_size as usize) as f64
                        / largest_size
                        < fact_dimension_ratio
                    {
                        left_filtered_table =
                            read_table(left_table.clone(), left_field.clone(), tables.clone());
                    }
                    if tables
                        .get(&right_table.clone().unwrap())
                        .unwrap()
                        .size
                        .unwrap_or(largest_size as usize) as f64
                        / largest_size
                        < fact_dimension_ratio
                    {
                        right_filtered_table =
                            read_table(right_table.clone(), right_field.clone(), tables);
                    }

                    let mut left_read = false;
                    let mut right_read = false;
                    if let Some(_) = left_filtered_table { left_read = true }
                    if let Some(_) = right_filtered_table { right_read = true }

                    if left_read || right_read {
                        return parse_and_optimize(
                            plan,
                            left_filtered_table,
                            right_filtered_table,
                            left_table,
                            left_field,
                            right_table,
                            right_field,
                        );
                    } else {
                        return Ok(None);
                    }
                } else {
                    // TODO: Need to reason about more than one join condition
                }
            }
            Ok(None)
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
                        // TODO: Decide what to do here
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
                            t.table_name.clone(),
                            TableInfo {
                                table_name: t.table_name.clone(),
                                filepath: f.clone(),
                                size: size,
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
        LogicalPlan::TableScan(scan) => {
            let source = scan
                .source
                .as_any()
                .downcast_ref::<DaskTableSource>()
                .expect("should be a DaskTableSource");

            source.filepath()
        }
        _ => None,
    }
}

fn get_table_size(plan: &LogicalPlan) -> Option<usize> {
    match plan {
        LogicalPlan::TableScan(scan) => {
            let source = scan
                .source
                .as_any()
                .downcast_ref::<DaskTableSource>()
                .expect("should be a DaskTableSource");

            source
                .statistics()
                .map(|stats| stats.get_row_count() as usize)
        }
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
                if let LogicalPlan::TableScan(ref t) = *s.input {
                    aliases.insert(s.alias.clone(), t.table_name.clone());
                }
            }
            // Move on to next step
            current_plan = current_plan.inputs()[0].clone();
        }
    }
    aliases
}

fn read_table(
    my_table: Option<String>,
    my_field: Option<String>,
    tables: HashMap<String, TableInfo>,
) -> Option<HashSet<RowValue>> {
    // Obtain filepaths to all relevant parquet files
    let paths = fs::read_dir(
        tables
            .get(&my_table.clone().unwrap())
            .unwrap()
            .filepath
            .clone()
    )
    .unwrap();
    let mut my_files = vec![];
    for path in paths {
        my_files.push(path.unwrap().path().display().to_string())
    }

    // Using the filepaths to the Parquet tables, obtain the schemas of the relevant tables
    let my_schema: &Type = &SerializedFileReader::try_from(my_files[0].clone())
        .unwrap()
        .metadata()
        .file_metadata()
        .schema()
        .clone();

    // Use the schemas of the relevant tables to obtain the physical type of the relevant columns
    let my_field = my_field.unwrap();
    let my_type = get_physical_type(my_schema, my_field.clone());

    let my_filters = tables.get(&my_table.unwrap()).unwrap().filters.clone();
    let my_filtered_fields = get_filtered_fields(&my_filters, my_schema, my_field.clone());
    let my_filtered_string = my_filtered_fields.0;
    let my_filtered_types = my_filtered_fields.1;
    let my_filtered_names = my_filtered_fields.2;
    // TODO: Add more logic so that this check isn't necessary
    if my_filters.len() != my_filtered_names.len() {
        return None;
    }

    // Specify which column to include in the reader, then read in the rows
    let repetition = get_repetition(my_schema, my_field.clone());
    let my_type = my_type.unwrap().to_string();
    let my_projection_schema = "message schema { ".to_owned()
        + &my_filtered_string
        + &repetition.unwrap()
        + " "
        + &my_type
        + " "
        + &my_field
        + "; }";
    let my_projection = parse_message_type(&my_projection_schema).ok();
    let my_rows = my_files
        .iter()
        .map(|p| SerializedFileReader::try_from(&*p.clone()).unwrap())
        .flat_map(|r| {
            RowIter::from_file_into(Box::new(r))
                .project(my_projection.clone())
                .unwrap()
        });

    // Create HashSets for each column
    let mut my_set: HashSet<RowValue> = HashSet::new();
    for row in my_rows {
        let mut satisfies_filters = true;
        let mut row_index = 0;
        for index in 0..my_filters.len() {
            if my_filtered_names[index] != my_field {
                let current_type = &my_filtered_types[index];
                match current_type.as_str() {
                    "BYTE_ARRAY" => {
                        let string_value = row.get_string(row_index);
                        // TODO: Handle Err(_) case?
                        if let Ok(s) = string_value {
                            if !satisfies_string(s, my_filters[index].clone()) {
                                satisfies_filters = false;
                            }
                        }
                    }
                    "INT64" => {
                        let long_value = row.get_long(row_index);
                        // TODO: Handle Err(_) case?
                        if let Ok(l) = long_value {
                            if !satisfies_long(l, my_filters[index].clone()) {
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
            match my_type.as_str() {
                "BYTE_ARRAY" => {
                    let r = row.get_string(row_index).unwrap();
                    my_set.insert(RowValue::String(r.to_string()));
                }
                "INT64" => {
                    let r = row.get_long(row_index).unwrap();
                    my_set.insert(RowValue::Long(r));
                }
                _ => panic!("Unknown PhysicalType"),
            }
        }
    }

    Some(my_set)
}

fn get_physical_type(
    my_schema: &Type,
    my_field: String,
) -> Option<datafusion::parquet::basic::Type> {
    match my_schema {
        Type::GroupType {
            basic_info: _,
            fields,
        } => {
            for field in fields {
                let match_field = &*field.clone();
                match match_field {
                    Type::PrimitiveType {
                        basic_info,
                        physical_type,
                        ..
                    } => {
                        if basic_info.name() == my_field {
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

fn get_repetition(my_schema: &Type, my_field: String) -> Option<String> {
    match my_schema {
        Type::GroupType {
            basic_info: _,
            fields,
        } => {
            for field in fields {
                let match_field = &*field.clone();
                match match_field {
                    Type::PrimitiveType { basic_info, .. } => {
                        if basic_info.name() == my_field {
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
    my_schema: &Type,
    my_field: String,
) -> (String, Vec<String>, Vec<String>) {
    let mut filtered_fields = vec![];
    let mut filtered_columns = vec![];
    let mut filtered_types = vec![];
    for filter in filters {
        match filter {
            Expr::BinaryExpr(b) => {
                // TODO: Handle nested BinaryExprs
                if let Expr::Column(c) = &*b.left {
                    let current_field = c.name.clone();
                    let physical_type = get_physical_type(my_schema, c.name.clone())
                        .unwrap()
                        .to_string();
                    if current_field != my_field {
                        let repetition = get_repetition(my_schema, c.name.clone());
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
                    let physical_type = get_physical_type(my_schema, c.name.clone()).unwrap().to_string();
                    if current_field != my_field {
                        let repetition = get_repetition(my_schema, c.name.clone());
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
    Long(i64),
}

fn satisfies_string(string_value: &String, filter: Expr) -> bool {
    match filter {
        Expr::BinaryExpr(b) => {
            match b.op {
                Operator::Eq => {
                    Expr::Literal(datafusion_common::ScalarValue::Utf8(Some(
                        string_value.to_string(),
                    ))) == *b.right
                }
                Operator::NotEq => {
                    Expr::Literal(datafusion_common::ScalarValue::Utf8(Some(
                        string_value.to_string(),
                    ))) != *b.right
                }
                _ => {
                    panic!("Unknown satisfies_string operator"); // TODO
                }
            }
        }
        _ => {
            panic!("Unknown satisfies_string Expr"); // TODO
        }
    }
}

fn satisfies_long(long_value: i64, filter: Expr) -> bool {
    match filter {
        Expr::BinaryExpr(b) => {
            match b.op {
                Operator::Eq => {
                    Expr::Literal(datafusion_common::ScalarValue::Int64(Some(long_value)))
                        == *b.right
                }
                Operator::NotEq => {
                    Expr::Literal(datafusion_common::ScalarValue::Int64(Some(long_value)))
                        != *b.right
                }
                _ => {
                    panic!("Unknown satisfies_long operator"); // TODO
                }
            }
        }
        _ => {
            panic!("Unknown satisfies_long Expr"); // TODO
        }
    }
}

fn parse_and_optimize(
    plan: &LogicalPlan,
    left_set: Option<HashSet<RowValue>>,
    right_set: Option<HashSet<RowValue>>,
    left_table: Option<String>,
    left_field: Option<String>,
    right_table: Option<String>,
    right_field: Option<String>,
) -> Result<Option<LogicalPlan>> {
    // Use HashSet to set filters for the relevant TableScan
    if let (Some(set1), Some(set2)) = (left_set.clone(), right_set.clone()) {
        // Create a HashSet of the unique values shared by both of the columns being joined upon
        let set_intersection = set1.intersection(&set2);
        let mut set = HashSet::new();
        for value in set_intersection {
            set.insert(value.clone());
        }
        let binary_exprs = format_binary_exprs(
            set,
            left_table.clone(),
            left_field,
            right_table.clone(),
            right_field,
        );
        return build_plan(&plan, binary_exprs, left_table, right_table);
    } else if let Some(set) = left_set {
        let binary_exprs = format_binary_exprs(
            set,
            left_table.clone(),
            left_field,
            right_table.clone(),
            right_field,
        );
        return build_plan(&plan, binary_exprs, left_table, right_table);
    } else if let Some(set) = right_set {
        let binary_exprs = format_binary_exprs(
            set,
            left_table.clone(),
            left_field,
            right_table.clone(),
            right_field,
        );
        return build_plan(plan, binary_exprs, left_table, right_table);
    } else {
        return Ok(None);
    }
}

fn format_binary_exprs(
    my_set: HashSet<RowValue>,
    left_table: Option<String>,
    left_field: Option<String>,
    right_table: Option<String>,
    right_field: Option<String>,
) -> (Expr, Expr) {
    let mut left_exprs: Vec<Expr> = vec![];
    let mut right_exprs: Vec<Expr> = vec![];

    let left_field = left_field.unwrap();
    let right_field = right_field.unwrap();
    for value in my_set {
        if let RowValue::String(s) = value {
            let right = Box::new(Expr::Literal(datafusion_common::ScalarValue::Utf8(Some(
                s.to_string(),
            ))));

            // Left table
            let left = Box::new(Expr::Column(Column::new(
                left_table.clone(),
                left_field.clone(),
            )));
            let expr = Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::Eq,
                right: right.clone(),
            });
            left_exprs.push(expr);

            // Right table
            let left = Box::new(Expr::Column(Column::new(
                right_table.clone(),
                right_field.clone(),
            )));
            let expr = Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::Eq,
                right,
            });
            right_exprs.push(expr);
        } else if let RowValue::Long(l) = value {
            let right = Box::new(Expr::Literal(datafusion_common::ScalarValue::Int64(Some(
                l,
            ))));

            // Left table
            let left = Box::new(Expr::Column(Column::new(
                left_table.clone(),
                left_field.clone(),
            )));
            let expr = Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::Eq,
                right: right.clone(),
            });
            left_exprs.push(expr);

            // Right table
            let left = Box::new(Expr::Column(Column::new(
                right_table.clone(),
                right_field.clone(),
            )));
            let expr = Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::Eq,
                right,
            });
            right_exprs.push(expr);
        }
    }

    // TODO: Handle case when exprs.len() < 2 or 3
    let mut left_binary_expr = Expr::BinaryExpr(BinaryExpr {
        left: Box::new(left_exprs[0].clone()),
        op: Operator::Or,
        right: Box::new(left_exprs[1].clone()),
    });
    for left_expr in left_exprs.iter().skip(2) {
        left_binary_expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(left_binary_expr),
            op: Operator::Or,
            right: Box::new(left_expr.clone()),
        });
    }

    let mut right_binary_expr = Expr::BinaryExpr(BinaryExpr {
        left: Box::new(right_exprs[0].clone()),
        op: Operator::Or,
        right: Box::new(right_exprs[1].clone()),
    });
    for right_expr in right_exprs.iter().skip(2) {
        right_binary_expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(right_binary_expr),
            op: Operator::Or,
            right: Box::new(right_expr.clone()),
        });
    }

    (left_binary_expr, right_binary_expr)
}

fn build_plan(
    plan: &LogicalPlan,
    binary_exprs: (Expr, Expr),
    left_table: Option<String>,
    right_table: Option<String>,
) -> Result<Option<LogicalPlan>> {
    match plan {
        LogicalPlan::TableScan(t) => {
            if t.table_name == left_table.unwrap() {
                let mut new_filters = t.filters.clone();
                new_filters.push(binary_exprs.0);
                let scan = LogicalPlan::TableScan(TableScan {
                    table_name: t.table_name.clone(),
                    source: t.source.clone(),
                    projection: t.projection.clone(),
                    projected_schema: t.projected_schema.clone(),
                    filters: new_filters,
                    fetch: t.fetch,
                });
                Ok(Some(scan))
            } else if t.table_name == right_table.unwrap() {
                let mut new_filters = t.filters.clone();
                new_filters.push(binary_exprs.1);
                let scan = LogicalPlan::TableScan(TableScan {
                    table_name: t.table_name.clone(),
                    source: t.source.clone(),
                    projection: t.projection.clone(),
                    projected_schema: t.projected_schema.clone(),
                    filters: new_filters,
                    fetch: t.fetch,
                });
                Ok(Some(scan))
            } else {
                Ok(None)
            }
        }
        _ => optimize_children(plan, binary_exprs, left_table, right_table),
    }
}

fn optimize_children(
    plan: &LogicalPlan,
    binary_exprs: (Expr, Expr),
    left_table: Option<String>,
    right_table: Option<String>,
) -> Result<Option<LogicalPlan>> {
    let new_exprs = plan.expressions();
    let mut new_inputs = Vec::with_capacity(plan.inputs().len());
    let mut plan_is_changed = false;
    for input in plan.inputs() {
        let new_input = build_plan(
            input,
            binary_exprs.clone(),
            left_table.clone(),
            right_table.clone(),
        )?;
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
