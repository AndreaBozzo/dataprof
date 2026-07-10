//! Semantic validators for pattern detection.
//!
//! Each validator is a `fn(&str) -> bool` that runs on values already matched by
//! a pattern's regex. The validator pass rate feeds into the confidence formula:
//!
//! ```text
//! confidence = base(specificity) × match_factor × validator_pass_rate
//! ```
//!
//! Validators are pure functions; some implementations may use small temporary
//! allocations while validating.

/// Validate an Italian CAP (Codice di Avviamento Postale).
///
/// Valid range: 00010–98168. The regex `^\d{5}$` matches any 5-digit string;
/// this validator restricts to the actual Italian postal code range.
pub fn validate_cap_it(s: &str) -> bool {
    match s.parse::<u32>() {
        Ok(n) => (10..=98168).contains(&n),
        Err(_) => false,
    }
}

/// Validate an Italian P.IVA (Partita IVA) check digit.
///
/// The 11th digit is computed from the first 10 using the Italian tax authority
/// algorithm: odd-position digits are summed directly, even-position digits are
/// doubled and digits > 9 have 9 subtracted, then `(10 - sum % 10) % 10` must
/// equal the check digit.
pub fn validate_piva_it(s: &str) -> bool {
    let digits: Vec<u8> = s.bytes().map(|b| b.wrapping_sub(b'0')).collect();
    if digits.len() != 11 || digits.iter().any(|&d| d > 9) {
        return false;
    }

    let mut sum: u32 = 0;
    for (i, &d) in digits[..10].iter().enumerate() {
        if i % 2 == 0 {
            // Odd position (1-indexed) → direct sum
            sum += d as u32;
        } else {
            // Even position (1-indexed) → double, subtract 9 if >= 10
            let doubled = (d as u32) * 2;
            sum += if doubled > 9 { doubled - 9 } else { doubled };
        }
    }

    let check = ((10 - (sum % 10)) % 10) as u8;
    check == digits[10]
}

/// Validate an Italian Codice Fiscale check character.
///
/// Uses the official algorithm: alternating odd/even lookup tables for the first
/// 15 characters, sum modulo 26 maps to the check letter (16th character).
pub fn validate_codice_fiscale(s: &str) -> bool {
    let bytes: Vec<u8> = s.bytes().collect();
    if bytes.len() != 16 {
        return false;
    }

    let odd_values = |c: u8| -> Option<u32> {
        match c {
            b'0' => Some(1),
            b'1' => Some(0),
            b'2' => Some(5),
            b'3' => Some(7),
            b'4' => Some(9),
            b'5' => Some(13),
            b'6' => Some(15),
            b'7' => Some(17),
            b'8' => Some(19),
            b'9' => Some(21),
            b'A' => Some(1),
            b'B' => Some(0),
            b'C' => Some(5),
            b'D' => Some(7),
            b'E' => Some(9),
            b'F' => Some(13),
            b'G' => Some(15),
            b'H' => Some(17),
            b'I' => Some(19),
            b'J' => Some(21),
            b'K' => Some(2),
            b'L' => Some(4),
            b'M' => Some(18),
            b'N' => Some(20),
            b'O' => Some(11),
            b'P' => Some(3),
            b'Q' => Some(6),
            b'R' => Some(8),
            b'S' => Some(12),
            b'T' => Some(14),
            b'U' => Some(16),
            b'V' => Some(10),
            b'W' => Some(22),
            b'X' => Some(25),
            b'Y' => Some(24),
            b'Z' => Some(23),
            _ => None,
        }
    };

    let even_values = |c: u8| -> Option<u32> {
        match c {
            b'0'..=b'9' => Some((c - b'0') as u32),
            b'A'..=b'Z' => Some((c - b'A') as u32),
            _ => None,
        }
    };

    let mut sum: u32 = 0;
    for (i, &c) in bytes[..15].iter().enumerate() {
        let val = if i % 2 == 0 {
            // Odd position (1-indexed) → odd lookup
            odd_values(c)
        } else {
            // Even position (1-indexed) → even lookup
            even_values(c)
        };
        match val {
            Some(v) => sum += v,
            None => return false,
        }
    }

    let expected = b'A' + (sum % 26) as u8;
    bytes[15] == expected
}

/// Validate an IBAN checksum (ISO 7064 Mod 97-10).
///
/// Algorithm: move the first 4 characters to the end, convert letters to
/// two-digit numbers (A=10, B=11, ..., Z=35), compute the resulting number
/// modulo 97. Valid IBANs produce remainder 1.
pub fn validate_iban(s: &str) -> bool {
    let s = s.trim();
    if s.len() < 5 || s.len() > 34 {
        return false;
    }

    // Rearrange: move first 4 chars to end
    let rearranged = format!("{}{}", &s[4..], &s[..4]);

    // Convert to digit string (A=10, B=11, ..., Z=35)
    let mut numeric = String::with_capacity(rearranged.len() * 2);
    for c in rearranged.chars() {
        match c {
            '0'..='9' => numeric.push(c),
            'A'..='Z' => {
                let val = (c as u32) - ('A' as u32) + 10;
                numeric.push_str(&val.to_string());
            }
            _ => return false,
        }
    }

    // Compute mod 97 using chunked arithmetic to avoid big-integer math.
    // `numeric` contains only ASCII digits (checked above), so the UTF-8 and
    // parse steps cannot fail: remainder is at most 2 digits, the chunk at most
    // 9, and an 11-digit number always fits in u64.
    let mut remainder: u64 = 0;
    for chunk in numeric.as_bytes().chunks(9) {
        let chunk_str =
            std::str::from_utf8(chunk).expect("chunk of ASCII digits is always valid UTF-8");
        let combined = format!("{remainder}{chunk_str}");
        remainder = combined
            .parse::<u64>()
            .expect("at most 11 ASCII digits always parse as u64")
            % 97;
    }

    remainder == 1
}

/// Validate a credit card number using the Luhn algorithm.
///
/// Strips spaces/dashes, then doubles every second digit from the right,
/// sums all digits, and checks that the total is divisible by 10.
pub fn validate_credit_card(s: &str) -> bool {
    let digits: Vec<u8> = s
        .bytes()
        .filter(|&b| b != b' ' && b != b'-')
        .map(|b| b.wrapping_sub(b'0'))
        .collect();

    if digits.len() < 13 || digits.len() > 19 || digits.iter().any(|&d| d > 9) {
        return false;
    }

    let mut sum: u32 = 0;
    let parity = digits.len() % 2;
    for (i, &d) in digits.iter().enumerate() {
        if i % 2 == parity {
            let doubled = (d as u32) * 2;
            sum += if doubled > 9 { doubled - 9 } else { doubled };
        } else {
            sum += d as u32;
        }
    }

    sum.is_multiple_of(10)
}

/// Validate an IPv6 address using the standard library parser.
///
/// The regex pre-filter is intentionally loose; this validator handles all
/// valid representations including `::` compression and mixed IPv4 notation.
pub fn validate_ipv6(s: &str) -> bool {
    s.parse::<std::net::Ipv6Addr>().is_ok()
}

/// Validate a US Social Security Number.
///
/// Checks that the area number (first 3 digits) is not 000, 666, or 900-999.
/// The group and serial must also be non-zero.
pub fn validate_ssn_us(s: &str) -> bool {
    let clean: String = s.chars().filter(|c| c.is_ascii_digit()).collect();
    if clean.len() != 9 {
        return false;
    }
    // `clean` is exactly 9 ASCII digits (checked above), so these cannot fail.
    let area: u16 = clean[0..3].parse().expect("3 ASCII digits parse as u16");
    let group: u16 = clean[3..5].parse().expect("2 ASCII digits parse as u16");
    let serial: u16 = clean[5..9].parse().expect("4 ASCII digits parse as u16");

    area != 0 && area != 666 && area < 900 && group != 0 && serial != 0
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- CAP (IT) validator ----

    #[test]
    fn test_cap_valid() {
        assert!(validate_cap_it("00118")); // Roma
        assert!(validate_cap_it("20121")); // Milano
        assert!(validate_cap_it("10100")); // Torino
        assert!(validate_cap_it("80100")); // Napoli
        assert!(validate_cap_it("00010")); // Lowest valid
        assert!(validate_cap_it("98168")); // Highest valid (Messina)
    }

    #[test]
    fn test_cap_invalid() {
        assert!(!validate_cap_it("00000")); // Below range
        assert!(!validate_cap_it("00009")); // Below range
        assert!(!validate_cap_it("99999")); // Above range
        assert!(!validate_cap_it("98169")); // Just above range
    }

    // ---- P.IVA (IT) validator ----

    #[test]
    fn test_piva_valid() {
        // Known valid P.IVA numbers
        assert!(validate_piva_it("12345678903"));
        assert!(validate_piva_it("00000000000"));
    }

    #[test]
    fn test_piva_invalid_check_digit() {
        assert!(!validate_piva_it("12345678901")); // Wrong check digit
        assert!(!validate_piva_it("12345678902")); // Wrong check digit
    }

    #[test]
    fn test_piva_invalid_length() {
        assert!(!validate_piva_it("1234567890")); // Too short
        assert!(!validate_piva_it("123456789012")); // Too long
    }

    #[test]
    fn test_piva_invalid_chars() {
        assert!(!validate_piva_it("1234567890a"));
        assert!(!validate_piva_it("abcdefghijk"));
    }

    // ---- Codice Fiscale (IT) validator ----

    #[test]
    fn test_codice_fiscale_valid() {
        assert!(validate_codice_fiscale("RSSMRA85M01H501Q"));
        assert!(validate_codice_fiscale("BNCGVN90A01F205O"));
        assert!(validate_codice_fiscale("VRDLCU75D15L219V"));
    }

    #[test]
    fn test_codice_fiscale_invalid_check() {
        assert!(!validate_codice_fiscale("RSSMRA85M01H501A")); // Wrong check letter
        assert!(!validate_codice_fiscale("RSSMRA85M01H501Z")); // Wrong check letter
    }

    #[test]
    fn test_codice_fiscale_invalid_length() {
        assert!(!validate_codice_fiscale("RSSMRA85M01H501")); // Too short
        assert!(!validate_codice_fiscale("RSSMRA85M01H501ZZ")); // Too long
    }

    // ---- IBAN validator ----

    #[test]
    fn test_iban_valid() {
        assert!(validate_iban("GB82WEST12345698765432"));
        assert!(validate_iban("DE89370400440532013000"));
        assert!(validate_iban("FR7630006000011234567890189"));
        assert!(validate_iban("IT60X0542811101000000123456"));
    }

    #[test]
    fn test_iban_invalid_checksum() {
        assert!(!validate_iban("GB82WEST12345698765431")); // Last digit changed
        assert!(!validate_iban("DE89370400440532013001")); // Last digit changed
    }

    #[test]
    fn test_iban_too_short() {
        assert!(!validate_iban("GB82"));
        assert!(!validate_iban(""));
    }

    #[test]
    fn test_iban_invalid_chars() {
        assert!(!validate_iban("GB82WEST1234569876543!"));
    }

    // ---- Credit Card (Luhn) validator ----

    #[test]
    fn test_credit_card_valid() {
        assert!(validate_credit_card("4532015112830366")); // Visa
        assert!(validate_credit_card("5425233430109903")); // Mastercard
        assert!(validate_credit_card("4111111111111111")); // Visa test
        assert!(validate_credit_card("4111 1111 1111 1111")); // With spaces
        assert!(validate_credit_card("4111-1111-1111-1111")); // With dashes
    }

    #[test]
    fn test_credit_card_invalid() {
        assert!(!validate_credit_card("4532015112830367")); // Wrong check digit
        assert!(!validate_credit_card("1234567890123456")); // Invalid Luhn
        assert!(!validate_credit_card("123456789012")); // Too short
    }

    // ---- IPv6 validator ----

    #[test]
    fn test_ipv6_valid() {
        assert!(validate_ipv6("2001:0db8:85a3:0000:0000:8a2e:0370:7334"));
        assert!(validate_ipv6("::1")); // Loopback compressed
        assert!(validate_ipv6("fe80::1")); // Link-local compressed
        assert!(validate_ipv6("::ffff:192.168.1.1")); // IPv4-mapped
        assert!(validate_ipv6("::")); // All zeros
    }

    #[test]
    fn test_ipv6_invalid() {
        assert!(!validate_ipv6("not-an-ipv6"));
        assert!(!validate_ipv6("192.168.1.1")); // IPv4
        assert!(!validate_ipv6("2001:db8::g123")); // Invalid hex
    }

    // ---- SSN (US) validator ----

    #[test]
    fn test_ssn_valid() {
        assert!(validate_ssn_us("123-45-6789"));
        assert!(validate_ssn_us("123456789"));
        assert!(validate_ssn_us("001-01-0001"));
    }

    #[test]
    fn test_ssn_invalid() {
        assert!(!validate_ssn_us("000-45-6789")); // Area 000
        assert!(!validate_ssn_us("666-45-6789")); // Area 666
        assert!(!validate_ssn_us("900-45-6789")); // Area 900+
        assert!(!validate_ssn_us("123-00-6789")); // Group 00
        assert!(!validate_ssn_us("123-45-0000")); // Serial 0000
    }
}
