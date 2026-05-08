//! Shared query helpers for PostgreSQL stores.

use std::borrow::Cow;

use deadpool_postgres::GenericClient;
use macros::async_trait;
use spark_storage::TableNameRewriter;
use tokio_postgres::{Row, types::ToSql};

/// Provides table-name-aware query helpers for PostgreSQL stores.
#[async_trait]
pub trait PostgresQueryExt {
    /// Returns the table-name rewriter used by the store.
    fn table_names(&self) -> &TableNameRewriter;

    /// Rewrites SDK-managed storage identifiers in `sql`.
    fn sql<'a>(&self, sql: &'a str) -> Cow<'a, str> {
        self.table_names().sql(sql)
    }

    /// Runs a query after applying storage table-name rewriting.
    async fn query<C>(
        &self,
        client: &C,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, tokio_postgres::Error>
    where
        C: GenericClient + Sync,
    {
        query(self.table_names(), client, sql, params).await
    }

    /// Runs a query expected to return one row after applying storage table-name rewriting.
    async fn query_one<C>(
        &self,
        client: &C,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, tokio_postgres::Error>
    where
        C: GenericClient + Sync,
    {
        query_one(self.table_names(), client, sql, params).await
    }

    /// Runs a query expected to return zero or one row after applying storage table-name rewriting.
    async fn query_opt<C>(
        &self,
        client: &C,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, tokio_postgres::Error>
    where
        C: GenericClient + Sync,
    {
        query_opt(self.table_names(), client, sql, params).await
    }

    /// Runs a statement after applying storage table-name rewriting.
    async fn execute<C>(
        &self,
        client: &C,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, tokio_postgres::Error>
    where
        C: GenericClient + Sync,
    {
        execute(self.table_names(), client, sql, params).await
    }
}

/// Rewrites SDK-managed storage identifiers in `sql` using `table_names`.
#[must_use]
pub fn sql<'a>(table_names: &TableNameRewriter, sql: &'a str) -> Cow<'a, str> {
    table_names.sql(sql)
}

/// Runs a query after applying storage table-name rewriting.
pub async fn query<C>(
    table_names: &TableNameRewriter,
    client: &C,
    sql: &str,
    params: &[&(dyn ToSql + Sync)],
) -> Result<Vec<Row>, tokio_postgres::Error>
where
    C: GenericClient + Sync,
{
    let sql = table_names.sql(sql);
    client.query(sql.as_ref(), params).await
}

/// Runs a query expected to return one row after applying storage table-name rewriting.
pub async fn query_one<C>(
    table_names: &TableNameRewriter,
    client: &C,
    sql: &str,
    params: &[&(dyn ToSql + Sync)],
) -> Result<Row, tokio_postgres::Error>
where
    C: GenericClient + Sync,
{
    let sql = table_names.sql(sql);
    client.query_one(sql.as_ref(), params).await
}

/// Runs a query expected to return zero or one row after applying storage table-name rewriting.
pub async fn query_opt<C>(
    table_names: &TableNameRewriter,
    client: &C,
    sql: &str,
    params: &[&(dyn ToSql + Sync)],
) -> Result<Option<Row>, tokio_postgres::Error>
where
    C: GenericClient + Sync,
{
    let sql = table_names.sql(sql);
    client.query_opt(sql.as_ref(), params).await
}

/// Runs a statement after applying storage table-name rewriting.
pub async fn execute<C>(
    table_names: &TableNameRewriter,
    client: &C,
    sql: &str,
    params: &[&(dyn ToSql + Sync)],
) -> Result<u64, tokio_postgres::Error>
where
    C: GenericClient + Sync,
{
    let sql = table_names.sql(sql);
    client.execute(sql.as_ref(), params).await
}
