//! Column-oriented query results that keep the query's column order.
//!
//! The connectors used to hand back `HashMap<String, Vec<String>>`, which has no
//! order at all: `SELECT a, b FROM t` could profile as `["b", "a"]`, and hash
//! iteration is not even stable between processes, so two runs of the same query
//! could disagree. Every other input path reports columns in source order — CSV
//! header order, Parquet schema order, JSON first-seen field order — so the
//! database path was the one place where a format conversion reshuffled a
//! report.
//!
//! [`QueryColumns`] is that map with the order kept: a vector of named columns,
//! built in the order the driver reports them.

use std::collections::HashMap;
use std::ops::Index;

/// A query result as columns, in the order the query selected them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueryColumns {
    columns: Vec<(String, Vec<String>)>,
    /// Per column, by position, the rows whose value was a JSON object or array
    /// or a SQL array, ascending. Always as long as `columns`.
    containers: Vec<Vec<usize>>,
}

impl QueryColumns {
    /// An empty result with no columns.
    pub fn new() -> Self {
        Self::default()
    }

    /// Build empty columns for `names`, in order, each sized for `row_capacity`
    /// values.
    ///
    /// Callers then fill them positionally with [`push_value`](Self::push_value),
    /// which is what ties a value to the column the driver read it from.
    pub fn with_names<I, S>(names: I, row_capacity: usize) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        names
            .into_iter()
            .map(|name| (name.into(), Vec::with_capacity(row_capacity)))
            .collect()
    }

    /// Append a value to the column at `index`.
    ///
    /// Out-of-range indices are ignored; callers iterate the same column list
    /// they built the result from, so there is no in-range/out-of-range decision
    /// for them to get wrong.
    pub fn push_value(&mut self, index: usize, value: String) {
        self.push_decoded(index, value, false);
    }

    /// Append a value to the column at `index`, recording whether it was a
    /// container: a JSON object or array, or a SQL array.
    pub fn push_decoded(&mut self, index: usize, value: String, container: bool) {
        if let Some((_, data)) = self.columns.get_mut(index) {
            if container {
                self.containers[index].push(data.len());
            }
            data.push(value);
        }
    }

    /// Per column name, how many of its values were containers. Columns with
    /// none are left out.
    pub fn container_counts(&self) -> HashMap<String, usize> {
        self.columns
            .iter()
            .zip(&self.containers)
            .filter(|(_, rows)| !rows.is_empty())
            .map(|((name, _), rows)| (name.clone(), rows.len()))
            .collect()
    }

    /// Per column, in query order, the rows whose value was a container,
    /// ascending. Row signatures tag those values apart from a string that
    /// spells the same text (#945).
    pub fn container_rows(&self) -> impl Iterator<Item = &[usize]> {
        self.containers.iter().map(Vec::as_slice)
    }

    /// Number of columns.
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    /// Whether the result has no columns.
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// Rows in the result, read off the first column.
    pub fn row_count(&self) -> usize {
        self.columns.first().map_or(0, |(_, data)| data.len())
    }

    /// Column names, in query order.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.columns.iter().map(|(name, _)| name.as_str())
    }

    /// Column data, in query order.
    pub fn values(&self) -> impl Iterator<Item = &Vec<String>> {
        self.columns.iter().map(|(_, data)| data)
    }

    /// Name/data pairs, in query order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &Vec<String>)> {
        self.columns
            .iter()
            .map(|(name, data)| (name.as_str(), data))
    }

    /// Mutable column data, in query order.
    pub fn values_mut(&mut self) -> impl Iterator<Item = &mut Vec<String>> {
        self.columns.iter_mut().map(|(_, data)| data)
    }

    /// The data for `name`, if the result has such a column.
    pub fn get(&self, name: &str) -> Option<&Vec<String>> {
        self.columns
            .iter()
            .find(|(column, _)| column == name)
            .map(|(_, data)| data)
    }

    /// Retain selected columns while preserving query order.
    pub fn retain_names(&mut self, names: &[String]) {
        let names = names
            .iter()
            .map(String::as_str)
            .collect::<std::collections::HashSet<_>>();
        let (columns, containers) = std::mem::take(&mut self.columns)
            .into_iter()
            .zip(std::mem::take(&mut self.containers))
            .filter(|((name, _), _)| names.contains(name.as_str()))
            .unzip();
        self.columns = columns;
        self.containers = containers;
    }

    /// Drop the order and hand back a plain map.
    ///
    /// For consumers keyed purely by name — quality metrics look every column up
    /// by name and never iterate for presentation.
    pub fn into_map(self) -> HashMap<String, Vec<String>> {
        self.columns.into_iter().collect()
    }
}

impl Index<&str> for QueryColumns {
    type Output = Vec<String>;

    fn index(&self, name: &str) -> &Self::Output {
        self.get(name)
            .unwrap_or_else(|| panic!("no column named {name} in query result"))
    }
}

impl FromIterator<(String, Vec<String>)> for QueryColumns {
    fn from_iter<I: IntoIterator<Item = (String, Vec<String>)>>(iter: I) -> Self {
        let columns: Vec<_> = iter.into_iter().collect();
        let containers = vec![Vec::new(); columns.len()];
        Self {
            columns,
            containers,
        }
    }
}

impl IntoIterator for QueryColumns {
    type Item = (String, Vec<String>);
    type IntoIter = std::vec::IntoIter<(String, Vec<String>)>;

    fn into_iter(self) -> Self::IntoIter {
        self.columns.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_come_back_in_the_order_they_went_in() {
        // Deliberately non-alphabetical: sorting or hashing this yields a
        // different order, so a regression is visible rather than accidental.
        let result = QueryColumns::with_names(["id", "amount", "active"], 0);
        assert_eq!(
            result.names().collect::<Vec<_>>(),
            ["id", "amount", "active"]
        );
    }

    #[test]
    fn values_land_in_the_column_they_were_pushed_to() {
        let mut result = QueryColumns::with_names(["b", "a"], 2);
        result.push_value(0, "b1".to_string());
        result.push_value(1, "a1".to_string());
        result.push_value(0, "b2".to_string());
        result.push_value(1, "a2".to_string());

        assert_eq!(result["b"], vec!["b1", "b2"]);
        assert_eq!(result["a"], vec!["a1", "a2"]);
        assert_eq!(result.row_count(), 2);
    }
}
