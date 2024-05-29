const VARHDRSZ: i32 = 4;
const NUMERIC_POS: u16 = 0x0000;
const NUMERIC_NEG: u16 = 0x4000;
const NUMERIC_NAN: u16 = 0xC000;
const NUMERIC_PINF: u16 = 0xD000;
const NUMERIC_NINF: u16 = 0xF000;

// Follows the Postgres implementation in src/backend/utils/adt/numeric.c
pub fn numeric_typmod_precision(typmod: i32) -> u16 {
    (((typmod - VARHDRSZ) >> 16) & 0xffff) as u16
}

// Follows the Postgres implementation in src/backend/utils/adt/numeric.c
pub fn numeric_typmod_scale(typmod: i32) -> i16 {
    ((((typmod - VARHDRSZ) & 0x7ff) ^ 1024) - 1024) as i16
}

pub fn pg_numeric_to_arrow_decimal(buf: &[u8], result_scale: i16) -> i128 {
    assert!(buf.len() >= 8, "Numeric buffer not long enough");
    // Bytes 0 and 1 encode ndigits, the number of base-10000 digits
    let ndigits = u16::from_be_bytes(buf[0..2].try_into().unwrap());
    // Bytes 2 and 3 encode weight, the base-10000 weight of first digit
    let weight = i16::from_be_bytes(buf[2..4].try_into().unwrap());
    // Bytes 4 and 5 encode the sign
    let sign_word = u16::from_be_bytes(buf[4..6].try_into().unwrap());
    let sign_multiplier: i128 = match sign_word {
        NUMERIC_POS => 1,
        NUMERIC_NEG => -1,
        NUMERIC_NAN => panic!("Cannot convert numeric NaN"),
        NUMERIC_PINF => panic!("Cannot convert numeric +Inf"),
        NUMERIC_NINF => panic!("Cannot convert numeric 'Inf"),
        _ => panic!("Unexpected numeric sign: {}", sign_word),
    };
    // Bytes 6 and 7 encode dscale. We ignore them
    // The remaining bytes contain the digits. Every two bytes encode
    // a base-10000 digit
    let digits_bytes = &buf[8..];
    assert!(
        digits_bytes.len() >= (2 * ndigits) as usize,
        "Not enough digits in numeric buffer"
    );
    let mut abs_result: i128 = 0;
    for i in 0..ndigits {
        let digit_bytes: [u8; 2] = digits_bytes[(2 * i as usize)..(2 * i as usize + 2)]
            .try_into()
            .unwrap();
        // The value of the current base-10000 digit
        let digit = u16::from_be_bytes(digit_bytes);
        // The base-10 weight of the current base-10000 digit in abs_result
        let digit_multiplier_dweight: i16 = 4 * (weight - i as i16) + result_scale;
        if digit_multiplier_dweight <= -4 {
            // The weight of this base-10000 digit is too small to contribute to abs_result
        } else if digit_multiplier_dweight == -3 {
            abs_result += (digit / 1000) as i128;
        } else if digit_multiplier_dweight == -2 {
            abs_result += (digit / 100) as i128;
        } else if digit_multiplier_dweight == -1 {
            abs_result += (digit / 10) as i128;
        } else {
            // digit_multiplier_dweight > 0
            let digit_multplier: i128 = 10_i128.pow(digit_multiplier_dweight as u32);
            abs_result += digit_multplier * (digit as i128);
        }
    }

    abs_result * sign_multiplier
}
