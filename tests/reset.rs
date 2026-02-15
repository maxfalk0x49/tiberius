use futures_util::io::{AsyncRead, AsyncWrite};
use once_cell::sync::Lazy;
use std::env;
use std::sync::Once;

use tiberius::Result;

use runtimes_macro::test_on_runtimes;

// This is used in the testing macro :)
#[allow(dead_code)]
static LOGGER_SETUP: Once = Once::new();

static CONN_STR: Lazy<String> = Lazy::new(|| {
    env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap_or_else(|_| {
        "server=tcp:localhost,1433;user=SA;password=<YourStrong@Passw0rd>;IntegratedSecurity=true;TrustServerCertificate=true".to_owned()
    })
});

#[test_on_runtimes]
async fn reset_connection_clears_temp_tables<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    // Create a session-scoped temp table and insert data.
    conn.simple_query("CREATE TABLE #reset_test (id INT)")
        .await?
        .into_results()
        .await?;

    conn.simple_query("INSERT INTO #reset_test (id) VALUES (1)")
        .await?
        .into_results()
        .await?;

    // Verify the temp table exists.
    let row = conn
        .query("SELECT COUNT(*) FROM #reset_test", &[])
        .await?
        .into_row()
        .await?
        .unwrap();
    assert_eq!(Some(1i32), row.get(0));

    // Mark connection for reset.
    conn.reset_connection();

    // The next query triggers the reset on the wire.
    // After reset, the temp table should be gone.
    let result = conn
        .simple_query("SELECT COUNT(*) FROM #reset_test")
        .await;

    assert!(
        result.is_err(),
        "expected an error because #reset_test should have been dropped by the reset"
    );

    Ok(())
}
