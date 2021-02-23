use once_cell::sync::OnceCell;
use std::fs::File;
use std::io::{Stdout, Write, BufWriter};
use std::sync::Mutex;
use serde::Serialize;
use std::path::PathBuf;

pub struct Logging {
    stdout: Stdout,
    // Creating json_serde::Serializer is cheap.
    json: BufWriter<File>,
}

#[allow(non_upper_case_globals)]
pub mod groups {
    use once_cell::sync::OnceCell;
    use std::sync::Mutex;
    use super::Logging;

    pub static warn: OnceCell<Mutex<Logging>> = OnceCell::new();
}

pub fn get_log(source: &'static OnceCell<Mutex<Logging>>, name: &'static str) -> &'static Mutex<Logging> {
    source.get_or_init(|| {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .append(true)
            .open(PathBuf::from("logs/").join(name).with_extension(".json"))
            .unwrap();
        Mutex::new(Logging {
            stdout: std::io::stdout(),
            json: BufWriter::new(file),
        })
    })
}

pub fn write_log<T: Serialize>(this: &'static Mutex<Logging>, data: &T, pretty: std::fmt::Arguments<'_>) {
    let mut this = this.lock().unwrap();
    this.json.write_all(&[b'\n']).unwrap();
    serde_json::to_writer(&mut this.json, data).unwrap();
    this.json.flush().unwrap();
    this.stdout.write_fmt(pretty).unwrap();
}

macro_rules! log {
    ($group:ident: $fmt:literal $(, $key:ident $(=$val:expr)? )* $(; $($additional:ident $(=$add_val:expr)? )*)? ) => {
        #[allow(non_camel_case_types)]
        {
            #[derive(::serde::Serialize)]
            struct Log<$($key,)* $($($additional,)*)?> {
                __message: &'static str,
                $(
                    $key: $key,
                )*
                $($(
                    $additional: $additional,
                )*)?
            }

            let s = Log {
                __message: $fmt,
                $(
                    $key$(: $val)?,
                )*
                $($(
                    $additional$(: $add_val)?,
                )*)?
            };
            let group = crate::logging::get_log(&crate::logging::groups::$group, stringify!($group));
            crate::logging::write_log(group, &s, format_args!(
                concat!($fmt, '\n'), $(
                    $key = s.$key
                ),*
            ))
        }
    }
}
