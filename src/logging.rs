// FIXME: It's better to make logging non-panicking too
#![allow(clippy::missing_panics_doc)]

use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::Mutex;
use time::OffsetDateTime;

pub struct Logging {
    // Creating json_serde::Serializer is cheap.
    json: BufWriter<File>,
}

#[allow(non_upper_case_globals)]
pub mod groups {
    use super::Logging;
    use once_cell::sync::OnceCell;
    use std::sync::Mutex;

    pub static warn: OnceCell<Mutex<Logging>> = OnceCell::new();
    pub static error: OnceCell<Mutex<Logging>> = OnceCell::new();
    pub static aws: OnceCell<Mutex<Logging>> = OnceCell::new();
    pub static cli: OnceCell<Mutex<Logging>> = OnceCell::new();
    pub static fmt_sql: OnceCell<Mutex<Logging>> = OnceCell::new();
    pub static time: OnceCell<Mutex<Logging>> = OnceCell::new();
}

#[allow(clippy::unwrap_used)]
pub fn get_log(
    source: &'static OnceCell<Mutex<Logging>>,
    name: &'static str,
) -> &'static Mutex<Logging> {
    source.get_or_init(|| {
        // FIXME: Panicking here.
        // This could be fixed later, since currently it fails very soon.
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .append(true)
            .open(PathBuf::from("logs/").join(name).with_extension("json"))
            .unwrap();
        Mutex::new(Logging {
            json: BufWriter::new(file),
        })
    })
}

#[allow(clippy::unwrap_used)]
pub fn write_log(this: &'static Mutex<Logging>, data: &[u8]) {
    let mut this = this.lock().unwrap();
    this.json.write_all(&[b'\n']).unwrap();
    this.json.write_all(data).unwrap();
    this.json.flush().unwrap();
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LogEntry<T> {
    pub func: &'static str,
    pub file: &'static str,
    pub position: (u32, u32),
    pub time: OffsetDateTime,
    pub message: &'static str,
    pub inner: T,
}

#[macro_export]
macro_rules! log {
    (@value $x:ident [$($y:tt)?]) => {log!(@value $x [$($y)?] $x)};
    (@value $x:ident [f] $fmt:literal ) => {format!($fmt, $x)};
    (@value $x:ident [clone] $val:expr ) => {$val.clone()};
    (@value $x:ident [] $val:expr) => {&$val};
    (
        $($group:ident),*: $fmt:literal
            $(, $(&)? $key:ident $(= $val:expr )? $(=> $suffix:tt)? )*
            $(; $( $(&)? $additional:ident $(= $add_val:expr )? $(=> $add_suffix:tt)? ),* )?
    ) => {{
        #[allow(non_camel_case_types)]
        {
            #[derive(::serde::Serialize)]
            struct Log<$($key,)* $($($additional,)*)?> {
                $(
                    $key: $key,
                )*
                $($(
                    $additional: $additional,
                )*)?
            }

            // Get current function name.
            // Based on https://docs.rs/stdext/0.2.1/stdext/macro.function_name.html
            fn f() {}
            fn type_name_of<T>(_: T) -> &'static str {
                std::any::type_name::<T>()
            }
            let func = type_name_of(f);
            // `3` is the length of the `::f`.
            let func = &func[..func.len() - 3];

            let s = $crate::logging::LogEntry {
                message: $fmt,
                func,
                file: file!(),
                position: (line!(), column!()),
                time: ::time::OffsetDateTime::now_utc(),
                inner: Log {
                    $(
                        $key: log!(@value $key [$($suffix)?] $($val)? ),
                    )*
                    $($(
                        $additional: log!(@value $additional [$($add_suffix)?] $($add_val)?),
                    )*)?
                }
            };

            eprintln!(
                concat!("[{__time} @ {__func}:{__line} => {__groups}] ", $fmt),
                $($key = s.inner.$key, )*
                __time=s.time, __func=s.func, __line=s.position.0, __groups=stringify!($($group),*)
            );

            let ser = ::serde_json::to_vec(&s).unwrap();
            for group in [
                $(
                    $crate::logging::get_log(&crate::logging::groups::$group, stringify!($group))
                ),*
            ].iter() {
                $crate::logging::write_log(group, &ser);
            }
        }
    }}
}
