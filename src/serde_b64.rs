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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    #[test]
    fn deserialize() {
        let data = "ABCDEFE=";
        let expected = [0x00, 0x10, 0x83, 0x10, 0x51];
        let serialized = serde_json::to_string(data).unwrap();
        let mut deserializer = serde_json::Deserializer::from_str(&serialized);
        let result: Vec<u8> = super::deserialize(&mut deserializer).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn serialize() {
        let data = b"hello";
        let expected = "\"aGVsbG8=\"";
        let mut result = Vec::new();
        let mut serializer = serde_json::Serializer::new(&mut result);
        super::serialize(data, &mut serializer).unwrap();
        assert_eq!(result, expected.as_bytes());
    }
}
