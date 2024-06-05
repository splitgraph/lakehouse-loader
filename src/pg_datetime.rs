use postgres::types::{FromSql, Type};

// tokio-postgres provides awkward Rust type conversions for Postgres TIMESTAMP and TIMESTAMPTZ values
// It's easier just to handle the raw values ourselves
pub struct UnixEpochDayOffset(i32);
// Number of days from 1970-01-01 to 2000-01-01
const J2000_EPOCH_DAYS: i32 = 10957;

impl FromSql<'_> for UnixEpochDayOffset {
    fn from_sql(_ty: &Type, buf: &[u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let byte_array: [u8; 4] = buf.try_into()?;
        let offset = i32::from_be_bytes(byte_array) + J2000_EPOCH_DAYS;
        Ok(Self(offset))
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::DATE
    }
}
impl From<UnixEpochDayOffset> for i32 {
    fn from(val: UnixEpochDayOffset) -> Self {
        val.0
    }
}

pub struct UnixEpochMicrosecondOffset(i64);
// Number of us from 1970-01-01 (Unix epoch) to 2000-01-01 (Postgres epoch)
const J2000_EPOCH_MICROSECONDS: i64 = J2000_EPOCH_DAYS as i64 * 86400 * 1000000;

impl FromSql<'_> for UnixEpochMicrosecondOffset {
    fn from_sql(_ty: &Type, buf: &[u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let byte_array: [u8; 8] = buf.try_into()?;
        let offset = i64::from_be_bytes(byte_array) + J2000_EPOCH_MICROSECONDS;
        Ok(Self(offset))
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::TIMESTAMP || *ty == Type::TIMESTAMPTZ
    }
}
impl From<UnixEpochMicrosecondOffset> for i64 {
    fn from(val: UnixEpochMicrosecondOffset) -> Self {
        val.0
    }
}

#[cfg(test)]
mod tests {
    use postgres::types::{FromSql, Type};

    use super::*;

    #[test]
    fn test_timestamp_just_after_j2000() {
        let offset =
            UnixEpochMicrosecondOffset::from_sql(&Type::TIMESTAMP, &[0, 0, 0, 0, 0, 0, 1, 2])
                .unwrap();
        assert_eq!(offset.0, 946_684_800_000_000 + 256 + 2);
    }
    #[test]
    fn test_timestamp_just_before_j2000() {
        let offset = UnixEpochMicrosecondOffset::from_sql(
            &Type::TIMESTAMP,
            &[255, 255, 255, 255, 255, 255, 255, 255],
        )
        .unwrap();
        assert_eq!(offset.0, 946_684_800_000_000 - 1);
    }
    #[test]
    fn test_date_just_after_j2000() {
        let offset = UnixEpochDayOffset::from_sql(&Type::DATE, &[0, 0, 1, 2]).unwrap();
        assert_eq!(offset.0, 10957 + 256 + 2);
    }
    #[test]
    fn test_date_just_before_j2000() {
        let offset = UnixEpochDayOffset::from_sql(&Type::DATE, &[255, 255, 255, 255]).unwrap();
        assert_eq!(offset.0, 10957 - 1);
    }
}
