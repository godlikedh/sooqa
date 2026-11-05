use time::OffsetDateTime;
use uuid::Uuid;

pub(super) fn encode(timestamp: OffsetDateTime, id: Uuid) -> String {
    format!("{}:{id}", timestamp.unix_timestamp_nanos())
}

pub(super) fn decode(value: &str) -> Result<(OffsetDateTime, Uuid), ()> {
    let (timestamp, id) = value.split_once(':').ok_or(())?;
    let timestamp = timestamp.parse::<i128>().map_err(|_| ())?;
    let timestamp = OffsetDateTime::from_unix_timestamp_nanos(timestamp).map_err(|_| ())?;
    let id = Uuid::parse_str(id).map_err(|_| ())?;
    Ok((timestamp, id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_wire_format_and_nanosecond_precision() {
        let id = Uuid::from_u128(0x12345678_1234_5678_1234_567812345678);
        for nanos in [-1_234_567_891, 0, 1_234_567_891] {
            let timestamp = OffsetDateTime::from_unix_timestamp_nanos(nanos).unwrap();
            let encoded = encode(timestamp, id);
            assert_eq!(encoded, format!("{nanos}:12345678-1234-5678-1234-567812345678"));
            assert_eq!(decode(&encoded), Ok((timestamp, id)));
        }
    }

    #[test]
    fn round_trips_supported_timestamp_bounds() {
        let first = time::Date::MIN.midnight().assume_utc();
        let last = time::Date::MAX.with_hms_nano(23, 59, 59, 999_999_999).unwrap().assume_utc();
        for timestamp in [first, last] {
            assert_eq!(decode(&encode(timestamp, Uuid::nil())), Ok((timestamp, Uuid::nil())));
        }
    }

    #[test]
    fn rejects_malformed_and_out_of_range_values() {
        let id = Uuid::nil();
        for value in [
            String::new(),
            id.to_string(),
            format!(":{id}"),
            "0:".to_owned(),
            format!("invalid:{id}"),
            format!("{}:{id}", i128::MAX),
            format!("{}0:{id}", i128::MAX),
            "0:not-a-uuid".to_owned(),
            format!("0:{id}:extra"),
        ] {
            assert!(decode(&value).is_err(), "accepted invalid cursor: {value}");
        }
    }
}
