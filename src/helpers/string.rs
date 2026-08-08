//! Binary data conversion utilities for reading various numeric types from byte slices.
//! Provides efficient little-endian conversion functions optimized for spreadsheet parsing.

/// Converts the first 8 bytes of a slice to a 64-bit floating point number.
/// Uses little-endian byte order for conversion.
#[inline]
pub(crate) fn to_f64(s: &[u8]) -> f64 {
    f64::from_le_bytes(s[..8].try_into().expect("f64"))
}

/// Converts the first 8 bytes of a slice to a 64-bit unsigned integer.
/// Uses little-endian byte order for conversion.
#[inline]
pub(crate) fn to_u64(s: &[u8]) -> u64 {
    u64::from_le_bytes(s[..8].try_into().expect("u64"))
}

/// Converts the first 4 bytes of a slice to a 32-bit unsigned integer.
/// Uses little-endian byte order for conversion.
#[inline]
pub(crate) fn to_u32(s: &[u8]) -> u32 {
    u32::from_le_bytes(s[..4].try_into().expect("u32"))
}

/// Converts the first 4 bytes of a slice to a 32-bit signed integer.
/// Uses little-endian byte order for conversion.
#[inline]
pub(crate) fn to_i32(s: &[u8]) -> i32 {
    i32::from_le_bytes(s[..4].try_into().expect("i32"))
}

/// Converts the first 2 bytes of a slice to a 16-bit unsigned integer.
/// Uses little-endian byte order for conversion.
#[inline]
pub(crate) fn to_u16(s: &[u8]) -> u16 {
    u16::from_le_bytes(s[..2].try_into().expect("u16"))
}

/// Converts the first 4 bytes of a slice to a usize value.
/// First converts to u32, then safely converts to usize for the current platform.
#[inline]
pub(crate) fn to_usize(s: &[u8]) -> usize {
    to_u32(s).try_into().expect("usize")
}
