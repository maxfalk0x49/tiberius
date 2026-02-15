use futures_util::io::{AsyncRead, AsyncWrite};
use names::{Generator, Name};
use once_cell::sync::Lazy;
use std::cell::RefCell;
use std::env;
use std::sync::Once;
use tiberius::{IntoSql, Result, TokenRow};

#[cfg(all(feature = "tds73", feature = "chrono"))]
use chrono::DateTime;
#[cfg(all(feature = "tds73", feature = "chrono"))]
use chrono::NaiveDateTime;

use runtimes_macro::test_on_runtimes;

// This is used in the testing macro :)
#[allow(dead_code)]
static LOGGER_SETUP: Once = Once::new();

static CONN_STR: Lazy<String> = Lazy::new(|| {
    env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap_or_else(|_| {
        "server=tcp:localhost,1433;IntegratedSecurity=true;TrustServerCertificate=true".to_owned()
    })
});

thread_local! {
    static NAMES: RefCell<Option<Generator<'static>>> =
    RefCell::new(None);
}

async fn random_table() -> String {
    NAMES.with(|maybe_generator| {
        maybe_generator
            .borrow_mut()
            .get_or_insert_with(|| Generator::with_naming(Name::Plain))
            .next()
            .unwrap()
            .replace('-', "")
    })
}

macro_rules! test_bulk_type {
    ($name:ident($sql_type:literal, $total_generated:expr, $generator:expr)) => {
        paste::item! {
            #[test_on_runtimes]
            async fn [< bulk_load_optional_ $name >]<S>(mut conn: tiberius::Client<S>) -> Result<()>
            where
                S: AsyncRead + AsyncWrite + Unpin + Send,
            {
                let table = format!("##{}", random_table().await);

                conn.execute(
                    &format!(
                        "CREATE TABLE {} (id INT IDENTITY PRIMARY KEY, content {} NULL)",
                        table,
                        $sql_type,
                    ),
                    &[],
                )
                    .await?;

                let mut req = conn.bulk_insert(&table).await?;

                for i in $generator {
                    let mut row = TokenRow::new();
                    row.push(i.into_sql());
                    req.send(row).await?;
                }

                let res = req.finalize().await?;

                assert_eq!($total_generated, res.total());

                Ok(())
            }

            #[test_on_runtimes]
            async fn [< bulk_load_required_ $name >]<S>(mut conn: tiberius::Client<S>) -> Result<()>
            where
                S: AsyncRead + AsyncWrite + Unpin + Send,
            {
                let table = format!("##{}", random_table().await);

                conn.execute(
                    &format!(
                        "CREATE TABLE {} (id INT IDENTITY PRIMARY KEY, content {} NOT NULL)",
                        table,
                        $sql_type
                    ),
                    &[],
                )
                    .await?;

                let mut req = conn.bulk_insert(&table).await?;

                for i in $generator {
                    let mut row = TokenRow::new();
                    row.push(i.into_sql());
                    req.send(row).await?;
                }

                let res = req.finalize().await?;

                assert_eq!($total_generated, res.total());

                Ok(())
            }
        }
    };
}

test_bulk_type!(tinyint("TINYINT", 256, 0..=255u8));
test_bulk_type!(smallint("SMALLINT", 2000, 0..2000i16));
test_bulk_type!(int("INT", 2000, 0..2000i32));
test_bulk_type!(bigint("BIGINT", 2000, 0..2000i64));

test_bulk_type!(empty_varchar(
    "VARCHAR(MAX)",
    100,
    vec![""; 100].into_iter()
));
test_bulk_type!(empty_nvarchar(
    "NVARCHAR(MAX)",
    100,
    vec![""; 100].into_iter()
));
test_bulk_type!(empty_varbinary(
    "VARBINARY(MAX)",
    100,
    vec![b""; 100].into_iter()
));

test_bulk_type!(real(
    "REAL",
    1000,
    vec![std::f32::consts::PI; 1000].into_iter()
));

test_bulk_type!(float(
    "FLOAT",
    1000,
    vec![std::f64::consts::PI; 1000].into_iter()
));

test_bulk_type!(varchar_limited(
    "VARCHAR(255)",
    1000,
    vec!["aaaaaaaaaaaaaaaaaaaaaaa"; 1000].into_iter()
));

#[cfg(all(feature = "tds73", feature = "chrono"))]
test_bulk_type!(datetime2(
    "DATETIME2",
    100,
    vec![DateTime::from_timestamp(1658524194, 123456789); 100].into_iter()
));

#[cfg(all(feature = "tds73", feature = "chrono"))]
test_bulk_type!(datetime2_naive("DATETIME2", 100, {
    #[allow(deprecated)]
    let dt = NaiveDateTime::from_timestamp_opt(1658524194, 123456789).unwrap();

    vec![dt; 100].into_iter()
}));

#[cfg(all(feature = "tds73", feature = "chrono"))]
test_bulk_type!(datetime2_0(
    "DATETIME2(0)",
    100,
    vec![DateTime::from_timestamp(1658524194, 123456789); 100].into_iter()
));

#[cfg(all(feature = "tds73", feature = "chrono"))]
test_bulk_type!(datetime2_1(
    "DATETIME2(1)",
    100,
    vec![DateTime::from_timestamp(1658524194, 123456789); 100].into_iter()
));

#[cfg(all(feature = "tds73", feature = "chrono"))]
test_bulk_type!(datetime2_2(
    "DATETIME2(2)",
    100,
    vec![DateTime::from_timestamp(1658524194, 123456789); 100].into_iter()
));

#[cfg(all(feature = "tds73", feature = "chrono"))]
test_bulk_type!(datetime2_3(
    "DATETIME2(3)",
    100,
    vec![DateTime::from_timestamp(1658524194, 123456789); 100].into_iter()
));

#[cfg(all(feature = "tds73", feature = "chrono"))]
test_bulk_type!(datetime2_4(
    "DATETIME2(4)",
    100,
    vec![DateTime::from_timestamp(1658524194, 123456789); 100].into_iter()
));

#[cfg(all(feature = "tds73", feature = "chrono"))]
test_bulk_type!(datetime2_5(
    "DATETIME2(5)",
    100,
    vec![DateTime::from_timestamp(1658524194, 123456789); 100].into_iter()
));

#[cfg(all(feature = "tds73", feature = "chrono"))]
test_bulk_type!(datetime2_6(
    "DATETIME2(6)",
    100,
    vec![DateTime::from_timestamp(1658524194, 123456789); 100].into_iter()
));

#[cfg(all(feature = "tds73", feature = "chrono"))]
test_bulk_type!(datetime2_7(
    "DATETIME2(7)",
    100,
    vec![DateTime::from_timestamp(1658524194, 123456789); 100].into_iter()
));

// --- ZOMBIES integration tests for NVARCHAR(MAX) / VARCHAR(MAX) large strings ---

#[test_on_runtimes]
async fn bulk_load_nvarchar_max_large_string<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let table = format!("##{}", random_table().await);
    let large_string: String = "a".repeat(50_000);

    conn.execute(
        &format!("CREATE TABLE {} (id INT IDENTITY PRIMARY KEY, content NVARCHAR(MAX) NULL)", table),
        &[],
    )
    .await?;

    let mut req = conn.bulk_insert(&table).await?;
    let mut row = TokenRow::new();
    row.push(large_string.as_str().into_sql());
    req.send(row).await?;
    let res = req.finalize().await?;
    assert_eq!(1, res.total());

    let rows = conn
        .query(format!("SELECT content FROM {}", table), &[])
        .await?
        .into_first_result()
        .await?;

    assert_eq!(1, rows.len());
    let val: &str = rows[0].get(0).unwrap();
    assert_eq!(large_string, val);

    Ok(())
}

#[test_on_runtimes]
async fn bulk_load_varchar_max_large_string<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let table = format!("##{}", random_table().await);
    let large_string: String = "b".repeat(50_000);

    conn.execute(
        &format!("CREATE TABLE {} (id INT IDENTITY PRIMARY KEY, content VARCHAR(MAX) NULL)", table),
        &[],
    )
    .await?;

    let mut req = conn.bulk_insert(&table).await?;
    let mut row = TokenRow::new();
    row.push(large_string.as_str().into_sql());
    req.send(row).await?;
    let res = req.finalize().await?;
    assert_eq!(1, res.total());

    let rows = conn
        .query(format!("SELECT content FROM {}", table), &[])
        .await?
        .into_first_result()
        .await?;

    assert_eq!(1, rows.len());
    let val: &str = rows[0].get(0).unwrap();
    assert_eq!(large_string, val);

    Ok(())
}

#[test_on_runtimes]
async fn bulk_load_nvarchar_max_boundary<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let table = format!("##{}", random_table().await);
    // 32767 chars * 2 = 65534 bytes UTF-16 (just under boundary)
    let at_boundary: String = "x".repeat(32767);
    // 33000 chars * 2 = 66000 bytes UTF-16 (over boundary)
    let over_boundary: String = "x".repeat(33000);

    conn.execute(
        &format!("CREATE TABLE {} (id INT IDENTITY PRIMARY KEY, content NVARCHAR(MAX) NULL)", table),
        &[],
    )
    .await?;

    let mut req = conn.bulk_insert(&table).await?;

    let mut row = TokenRow::new();
    row.push(at_boundary.as_str().into_sql());
    req.send(row).await?;

    let mut row = TokenRow::new();
    row.push(over_boundary.as_str().into_sql());
    req.send(row).await?;

    let res = req.finalize().await?;
    assert_eq!(2, res.total());

    let rows = conn
        .query(format!("SELECT content FROM {} ORDER BY id", table), &[])
        .await?
        .into_first_result()
        .await?;

    assert_eq!(2, rows.len());
    let val0: &str = rows[0].get(0).unwrap();
    let val1: &str = rows[1].get(0).unwrap();
    assert_eq!(at_boundary, val0);
    assert_eq!(over_boundary, val1);

    Ok(())
}

#[test_on_runtimes]
async fn bulk_load_nvarchar_max_null_and_values<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let table = format!("##{}", random_table().await);

    conn.execute(
        &format!("CREATE TABLE {} (id INT IDENTITY PRIMARY KEY, content NVARCHAR(MAX) NULL)", table),
        &[],
    )
    .await?;

    let mut req = conn.bulk_insert(&table).await?;

    let mut row = TokenRow::new();
    row.push("hello".into_sql());
    req.send(row).await?;

    let mut row = TokenRow::new();
    row.push(Option::<&str>::None.into_sql());
    req.send(row).await?;

    let mut row = TokenRow::new();
    row.push("world".into_sql());
    req.send(row).await?;

    let res = req.finalize().await?;
    assert_eq!(3, res.total());

    let rows = conn
        .query(format!("SELECT content FROM {} ORDER BY id", table), &[])
        .await?
        .into_first_result()
        .await?;

    assert_eq!(3, rows.len());
    assert_eq!(Some("hello"), rows[0].get(0));
    assert_eq!(None::<&str>, rows[1].get(0));
    assert_eq!(Some("world"), rows[2].get(0));

    Ok(())
}

#[test_on_runtimes]
async fn bulk_load_nvarchar_max_unicode<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let table = format!("##{}", random_table().await);
    let unicode_str = "\u{4e16}\u{754c}\u{3053}\u{3093}\u{306b}\u{3061}\u{306f}\u{1f600}\u{1f680}";

    conn.execute(
        &format!("CREATE TABLE {} (id INT IDENTITY PRIMARY KEY, content NVARCHAR(MAX) NULL)", table),
        &[],
    )
    .await?;

    let mut req = conn.bulk_insert(&table).await?;
    let mut row = TokenRow::new();
    row.push(unicode_str.into_sql());
    req.send(row).await?;
    let res = req.finalize().await?;
    assert_eq!(1, res.total());

    let rows = conn
        .query(format!("SELECT content FROM {}", table), &[])
        .await?
        .into_first_result()
        .await?;

    assert_eq!(1, rows.len());
    let val: &str = rows[0].get(0).unwrap();
    assert_eq!(unicode_str, val);

    Ok(())
}

#[test_on_runtimes]
async fn bulk_load_nvarchar_max_single_row<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let table = format!("##{}", random_table().await);

    conn.execute(
        &format!("CREATE TABLE {} (id INT IDENTITY PRIMARY KEY, content NVARCHAR(MAX) NULL)", table),
        &[],
    )
    .await?;

    let mut req = conn.bulk_insert(&table).await?;
    let mut row = TokenRow::new();
    row.push("single".into_sql());
    req.send(row).await?;
    let res = req.finalize().await?;
    assert_eq!(1, res.total());

    let rows = conn
        .query(format!("SELECT content FROM {}", table), &[])
        .await?
        .into_first_result()
        .await?;

    assert_eq!(1, rows.len());
    assert_eq!(Some("single"), rows[0].get(0));

    Ok(())
}

#[test_on_runtimes]
async fn bulk_load_nvarchar_max_many_rows<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let table = format!("##{}", random_table().await);
    let large_string: String = "z".repeat(40_000);

    conn.execute(
        &format!("CREATE TABLE {} (id INT IDENTITY PRIMARY KEY, content NVARCHAR(MAX) NULL)", table),
        &[],
    )
    .await?;

    let mut req = conn.bulk_insert(&table).await?;
    for _ in 0..10 {
        let mut row = TokenRow::new();
        row.push(large_string.as_str().into_sql());
        req.send(row).await?;
    }
    let res = req.finalize().await?;
    assert_eq!(10, res.total());

    let rows = conn
        .query(format!("SELECT content FROM {}", table), &[])
        .await?
        .into_first_result()
        .await?;

    assert_eq!(10, rows.len());
    for row in &rows {
        let val: &str = row.get(0).unwrap();
        assert_eq!(large_string, val);
    }

    Ok(())
}
