//! Generic `PostgreSQL` migration runner with version tracking and concurrency control.

use deadpool_postgres::Pool;
use spark_storage::TableNameRewriter;

use crate::error::PostgresError;
use crate::pool::{map_db_error, map_pool_error};

/// Runs database migrations with version tracking and concurrency control.
///
/// This function:
/// - Acquires an advisory lock (derived from `migration_lock_{table_name}`) to prevent concurrent migrations
/// - Creates a migrations tracking table if it doesn't exist
/// - Applies only new migrations (based on version number)
/// - Commits all changes in a single transaction
///
/// # Arguments
/// * `pool` - The connection pool to use
/// * `migrations_table` - Name of the table to track migration versions (e.g., `schema_migrations`)
/// * `migrations` - List of migrations, where each migration is a list of SQL statements.
///   Statements are owned `String`s so callers can build them at runtime (e.g. inlining a
///   tenant identity into a backfill).
#[allow(clippy::arithmetic_side_effects)]
pub async fn run_migrations(
    pool: &Pool,
    migrations_table: &str,
    migrations: &[Vec<String>],
) -> Result<(), PostgresError> {
    run_migrations_with_table_prefix(pool, migrations_table, migrations, None).await
}

/// Runs database migrations with an optional table prefix applied to all
/// SDK-owned table names.
#[allow(clippy::arithmetic_side_effects)]
pub async fn run_migrations_with_table_prefix(
    pool: &Pool,
    migrations_table: &str,
    migrations: &[Vec<String>],
    table_prefix: Option<&str>,
) -> Result<(), PostgresError> {
    let table_names = TableNameRewriter::new(table_prefix)
        .map_err(|e| PostgresError::Initialization(e.to_string()))?;
    run_migrations_with_table_names(pool, migrations_table, migrations, &table_names).await
}

pub(crate) async fn run_migrations_with_table_names(
    pool: &Pool,
    migrations_table: &str,
    migrations: &[Vec<String>],
    table_names: &TableNameRewriter,
) -> Result<(), PostgresError> {
    let mut client = pool.get().await.map_err(map_pool_error)?;
    let migrations_table = table_names.identifier(migrations_table);

    // Generate a unique advisory lock ID from a descriptive lock name
    let lock_name = format!("migration_lock_{migrations_table}");
    let lock_id = advisory_lock_id(&lock_name);

    // Run all migrations in a single transaction with a transaction-level advisory lock.
    // pg_advisory_xact_lock is automatically released on commit/rollback, making it safe
    // with connection pools (no risk of leaked locks if the task is cancelled or panics).
    let tx = client.transaction().await.map_err(map_db_error)?;

    tx.execute("SELECT pg_advisory_xact_lock($1)", &[&lock_id])
        .await
        .map_err(|e| {
            PostgresError::Initialization(format!("Failed to acquire migration lock: {e}"))
        })?;

    // Create migrations table if it doesn't exist
    // Note: table names cannot be parameterized in PostgreSQL, so we use format!
    let create_table_sql = format!(
        "CREATE TABLE IF NOT EXISTS {migrations_table} (
            version INTEGER PRIMARY KEY,
            applied_at TIMESTAMPTZ DEFAULT NOW()
        )"
    );
    tx.execute(&create_table_sql, &[])
        .await
        .map_err(map_db_error)?;

    // Get current version
    let get_version_sql = format!("SELECT COALESCE(MAX(version), 0) FROM {migrations_table}");
    let current_version: i32 = tx
        .query_opt(&get_version_sql, &[])
        .await
        .map_err(map_db_error)?
        .map_or(0, |row| row.get(0));

    for (i, migration) in migrations.iter().enumerate() {
        let version = i32::try_from(i + 1).unwrap_or(i32::MAX);
        if version > current_version {
            for statement in migration {
                let statement = table_names.sql(statement);
                tx.execute(statement.as_ref(), &[]).await.map_err(|e| {
                    PostgresError::Database(format!("Migration {version} failed: {e}"))
                })?;
            }
            let insert_version_sql =
                format!("INSERT INTO {migrations_table} (version) VALUES ($1)");
            tx.execute(&insert_version_sql, &[&version])
                .await
                .map_err(map_db_error)?;
        }
    }

    tx.commit().await.map_err(map_db_error)?;

    Ok(())
}

fn advisory_lock_id(lock_name: &str) -> i64 {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

    let mut hash = FNV_OFFSET;
    for b in lock_name.bytes() {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    i64::from_be_bytes(hash.to_be_bytes())
}

/// Asserts every SDK schema object referenced by migrations is represented in
/// the shared storage identifier allowlist.
#[doc(hidden)]
pub fn assert_migrations_schema_objects_known(
    migrations: &[Vec<String>],
    extra_identifiers: &[&str],
) {
    let mut identifiers = std::collections::BTreeSet::new();
    identifiers.extend(extra_identifiers.iter().copied().map(str::to_string));

    for statement in migrations.iter().flatten() {
        collect_sql_schema_identifiers(statement, &mut identifiers);
    }

    let unknown: Vec<_> = identifiers
        .into_iter()
        .filter(|identifier| !spark_storage::is_storage_identifier(identifier))
        .collect();
    assert!(
        unknown.is_empty(),
        "migration schema identifiers missing from STORAGE_IDENTIFIERS: {unknown:?}"
    );
}

fn collect_sql_schema_identifiers(sql: &str, identifiers: &mut std::collections::BTreeSet<String>) {
    for keyword in [
        "CREATE TABLE",
        "ALTER TABLE",
        "UPDATE",
        "DELETE FROM",
        "INSERT INTO",
        "REFERENCES",
        "CONSTRAINT",
    ] {
        identifiers.extend(identifiers_after_keyword(sql, keyword));
    }

    for index_pos in keyword_positions(sql, "CREATE INDEX") {
        if let Some(index_name) = identifier_after(sql, index_pos + "CREATE INDEX".len()) {
            identifiers.insert(index_name);
        }

        let upper_tail = sql[index_pos..].to_ascii_uppercase();
        if let Some(on_offset) = upper_tail.find(" ON ")
            && let Some(table_name) = identifier_after(sql, index_pos + on_offset + " ON ".len())
        {
            identifiers.insert(table_name);
        }
    }

    identifiers.extend(identifiers_after_keyword(sql, "DROP INDEX"));
}

#[cfg(test)]
pub(crate) fn assert_migrations_prefix_schema_objects(migrations: &[Vec<String>], prefix: &str) {
    let table_names = TableNameRewriter::new(Some(prefix)).expect("valid test prefix");

    for statement in migrations.iter().flatten() {
        let statement = table_names.sql(statement);
        assert_sql_schema_identifiers_prefixed(statement.as_ref(), prefix);
    }
}

#[cfg(test)]
fn assert_sql_schema_identifiers_prefixed(sql: &str, prefix: &str) {
    for keyword in [
        "CREATE TABLE",
        "ALTER TABLE",
        "UPDATE",
        "DELETE FROM",
        "INSERT INTO",
        "REFERENCES",
        "CONSTRAINT",
    ] {
        for identifier in identifiers_after_keyword(sql, keyword) {
            assert_prefixed_schema_identifier(&identifier, prefix);
        }
    }

    for index_pos in keyword_positions(sql, "CREATE INDEX") {
        let Some(index_name) = identifier_after(sql, index_pos + "CREATE INDEX".len()) else {
            continue;
        };
        assert_prefixed_schema_identifier(&index_name, prefix);

        let upper_tail = sql[index_pos..].to_ascii_uppercase();
        if let Some(on_offset) = upper_tail.find(" ON ")
            && let Some(table_name) = identifier_after(sql, index_pos + on_offset + " ON ".len())
        {
            assert_prefixed_schema_identifier(&table_name, prefix);
        }
    }

    for identifier in identifiers_after_keyword(sql, "DROP INDEX") {
        assert_prefixed_schema_identifier(&identifier, prefix);
    }
}

fn identifiers_after_keyword(sql: &str, keyword: &str) -> Vec<String> {
    keyword_positions(sql, keyword)
        .into_iter()
        .filter(|pos| keyword != "UPDATE" || is_statement_start(sql, *pos))
        .filter_map(|pos| identifier_after(sql, pos + keyword.len()))
        .collect()
}

fn keyword_positions(sql: &str, keyword: &str) -> Vec<usize> {
    let upper = sql.to_ascii_uppercase();
    let mut positions = Vec::new();
    let mut offset = 0;
    while let Some(pos) = upper[offset..].find(keyword) {
        let absolute = offset + pos;
        positions.push(absolute);
        offset = absolute + keyword.len();
    }
    positions
}

fn is_statement_start(sql: &str, pos: usize) -> bool {
    sql[..pos].chars().all(|c| c.is_whitespace() || c == ';')
}

fn identifier_after(sql: &str, mut offset: usize) -> Option<String> {
    let bytes = sql.as_bytes();
    while offset < bytes.len() && bytes[offset].is_ascii_whitespace() {
        offset += 1;
    }

    for optional in ["IF NOT EXISTS", "IF EXISTS"] {
        if sql[offset..].to_ascii_uppercase().starts_with(optional) {
            offset += optional.len();
            while offset < bytes.len() && bytes[offset].is_ascii_whitespace() {
                offset += 1;
            }
        }
    }

    if offset >= bytes.len() {
        return None;
    }

    let quote = match bytes[offset] {
        b'`' | b'"' => {
            offset += 1;
            Some(bytes[offset - 1])
        }
        _ => None,
    };

    let start = offset;
    while offset < bytes.len() {
        let b = bytes[offset];
        if Some(b) == quote {
            break;
        }
        if quote.is_none() && (b.is_ascii_whitespace() || matches!(b, b'(' | b',' | b';')) {
            break;
        }
        offset += 1;
    }

    (offset > start).then(|| sql[start..offset].to_string())
}

#[cfg(test)]
fn assert_prefixed_schema_identifier(identifier: &str, prefix: &str) {
    assert!(
        identifier.starts_with(prefix),
        "schema identifier `{identifier}` was not prefixed with `{prefix}`"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advisory_lock_id_hashes_full_lock_name() {
        assert_ne!(
            advisory_lock_id("migration_lock_schema_migrations"),
            advisory_lock_id("migration_lock_breez_schema_migrations")
        );
    }
}
