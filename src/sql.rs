macro_rules! fmt_sql {
    (static $single:literal) => {{
        let sql = $single;
        //$crate::log!(fmt_sql: "fmt_sql: {}", sql);
        sql
    }};
    ($($args:tt)*) => {{
        let sql = format!($($args)*);
        //$crate::log!(fmt_sql: "fmt_sql: {}", sql);
        sql
    }}
}