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

pub trait Utils: Sized {
    fn map_it<T>(self, func: impl FnOnce(Self) -> T) -> T {
        func(self)
    }

    fn also(mut self, func: impl FnOnce(&mut Self)) -> Self {
        func(&mut self);
        self
    }

    fn debug_assert(self) -> bool
    where
        Self: Into<bool>,
    {
        let res = self.into();
        debug_assert!(res);
        res
    }

    fn format_rfc3339<'a, 'b>(&'b self) -> String
    where
        &'a time::OffsetDateTime: From<&'b Self>,
    {
        // As for time == 0.3.1 formatting into Rfc3339 may give following errors:
        //
        // 1. InsufficientTypeInformation when there is no date, time or offset.
        //    Not the case for OffsetDateTime.
        // 2. InvalidComponent for years not in range of (0..10_000)
        // 3. InvalidComponent when offset.seconds_past_minute() != 0
        // 4. io::Error — not possible when formatting to the String
        //
        // source: https://docs.rs/time/0.3.1/src/time/formatting/formattable.rs.html#103
        let time: &'a time::OffsetDateTime = self.into();
        time.format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_else(|_| format!("{:?}", time))
    }
}

impl<T> Utils for T {}
