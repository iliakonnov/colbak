use std::convert::TryInto;

use serde::{Deserialize, Deserializer, Serializer};

pub fn serialize<T, S>(buffer: &T, serializer: S) -> Result<S::Ok, S::Error>
where
    T: AsRef<[u8]>,
    S: Serializer,
{
    serializer.serialize_str(&base64::encode(buffer))
}

pub fn deserialize<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    Vec<u8>: TryInto<T>,
    <Vec<u8> as TryInto<T>>::Error: std::fmt::Debug,
    D: Deserializer<'de>,
{
    use serde::de::Error;
    String::deserialize(deserializer)
        .and_then(|string| base64::decode(string).map_err(|err| Error::custom(err.to_string())))
        .and_then(|vec| {
            vec.try_into()
                .map_err(|err| Error::custom(format!("{:?}", err)))
        })
}
