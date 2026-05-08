//! Shared query helpers for MySQL stores.

use std::borrow::Cow;

use spark_storage::TableNameRewriter;

/// Provides table-name-aware query helpers for MySQL stores.
pub trait MysqlQueryExt {
    /// Returns the table-name rewriter used by the store.
    fn table_names(&self) -> &TableNameRewriter;

    /// Rewrites SDK-managed storage identifiers in `sql`.
    fn sql<'a>(&self, sql: &'a str) -> Cow<'a, str> {
        self.table_names().sql(sql)
    }
}

/// Rewrites SDK-managed storage identifiers in `sql` using `table_names`.
#[must_use]
pub fn sql<'a>(table_names: &TableNameRewriter, sql: &'a str) -> Cow<'a, str> {
    table_names.sql(sql)
}
