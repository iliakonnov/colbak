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

pub trait Utils {
    fn map_it<T>(self, func: impl FnOnce(Self) -> T) -> T
    where
        Self: Sized,
    {
        func(self)
    }

    fn also(mut self, func: impl FnOnce(&mut Self)) -> Self
    where
        Self: Sized,
    {
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
}

impl<T> Utils for T {}
