use crate::error::RustySheetError;
use crate::spreadsheet::reference::index_to_reference;
use chrono::Duration;
use chrono::NaiveDate;
use chrono::NaiveDateTime;
use chrono::Timelike;
use iso8601_duration::Duration as IsoDuration;
use std::fmt::Display;

/// Types of cell data in spreadsheet files.
#[derive(Copy, Clone, Debug, Default, PartialEq)]
pub(crate) enum CellType {
    #[default]
    Empty,
    /// Boolean values (true/false)
    Boolean,
    /// Numeric values
    Number,
    /// Date/time values stored as numbers from 1900 epoch
    NumberDateTime1900,
    /// Date values stored as numbers from 1900 epoch
    NumberDate1900,
    /// Time values stored as numbers from 1900 epoch
    NumberTime1900,
    /// Date/time values stored as numbers from 1904 epoch
    NumberDateTime1904,
    /// Date values stored as numbers from 1904 epoch
    NumberDate1904,
    /// Time values stored as numbers from 1904 epoch
    NumberTime1904,
    /// ISO 8601 date/time strings
    IsoDateTime,
    /// ISO 8601 duration strings
    IsoDuration,
    /// Inline string values
    InlineString,
    /// Shared string table references
    SharedString,
    /// Error values
    Error,
}

impl CellType {
    /// Parses built-in Excel number format IDs to determine cell type.
    pub(crate) fn parse_builtin_number_format_id(id: &str, is_1904: bool) -> Option<Self> {
        match id {
            "22" => Some(if is_1904 { Self::NumberDateTime1904 } else { Self::NumberDateTime1900 }),
            "14" | "15" | "16" | "17" => Some(if is_1904 { Self::NumberDate1904 } else { Self::NumberDate1900 }),
            "18" | "19" | "20" | "21" | "45" | "46" | "47" => Some(if is_1904 { Self::NumberTime1904 } else { Self::NumberTime1900 }),
            _ => None,
        }
    }

    /// Parses custom number format strings to determine cell type.
    /// Analyzes format codes for date/time patterns.
    pub(crate) fn parse_custom_number_format(format: &str, is_1904: bool) -> Self {
        let mut is_escaped = false;
        let mut is_literal = false;
        let mut is_date = false;
        let mut is_time = false;
        let mut is_color = false;
        for character in format.chars() {
            match character {
                _ if is_escaped => is_escaped = false,
                '_' | '\\' if !is_escaped => is_escaped = true,

                '"' if is_literal => is_literal = false,
                '"' if !is_literal && !is_color => is_literal = true,

                ']' if is_color => is_color = false,
                '[' if !is_color && !is_literal => is_color = true,
                _ if is_literal || is_color => (),

                'Y' | 'y' | 'D' | 'd' => is_date = true,
                'H' | 'h' | 'S' | 's' => is_time = true,
                _ => (),
            }
        }

        if is_date && is_time {
            if is_1904 {
                Self::NumberDateTime1904
            } else {
                Self::NumberDateTime1900
            }
        } else if is_date {
            if is_1904 {
                Self::NumberDate1904
            } else {
                Self::NumberDate1900
            }
        } else if is_time {
            if is_1904 {
                Self::NumberTime1904
            } else {
                Self::NumberTime1900
            }
        } else {
            Self::Number
        }
    }
}

/// Converts Excel error codes to human-readable error strings.
pub(crate) fn to_error_value(value: u8) -> &'static str {
    match value {
        0x00 => "#NULL!",
        0x07 => "#DIV/0!",
        0x0F => "#VALUE!",
        0x17 => "#REF!",
        0x1D => "#NAME?",
        0x24 => "#NUM!",
        0x2A => "#N/A",
        0x2B => "#GETTING_DATA",
        _ => "#ERROR!",
    }
}

/// Represents a single cell in a spreadsheet with position, type, and value.
#[derive(Clone, Debug)]
pub(crate) struct Cell {
    /// Row index (0-based)
    pub(crate) row: usize,
    /// Column index (0-based)
    pub(crate) col: usize,
    /// Cell data type
    pub(crate) kind: CellType,
    /// Cell value as string
    pub(crate) value: String,
}

impl Cell {
    /// Returns the Excel-style cell reference (e.g., "A1", "B2").
    pub(crate) fn reference(&self) -> String {
        index_to_reference(self.row, self.col)
    }

    /// Converts cell value to boolean (1 = true, other = false).
    pub(crate) fn to_boolean(&self) -> bool {
        self.value == "1"
    }

    /// Converts cell value to 64-bit integer, parsing only leading numeric characters.
    pub(crate) fn to_bigint(&self) -> Result<i64, String> {
        let mut integer = self.value.as_str();
        for (index, char) in self.value.char_indices() {
            if !char.is_ascii_digit() && char != '-' {
                integer = if index > 0 {
                     &self.value[..index]
                } else {
                    ""
                };
                break;
            }
        }
        integer.parse::<i64>().map_err(|_| format!("parse '{}' to bigint failed", self.value))
    }

    /// Converts cell value to double-precision floating point.
    pub(crate) fn to_double(&self) -> Result<f64, String> {
        self.value.parse::<f64>().map_err(|_| format!("parse '{}' to double failed", self.value))
    }

    /// Converts cell value to days since 1970-01-01 epoch.
    /// Handles Excel date formats (1900 and 1904 epochs) and ISO dates.
    pub(crate) fn to_date(&self) -> Result<i32, String> {
        match self.kind {
            CellType::NumberDateTime1900 | CellType::NumberDate1900 | CellType::NumberTime1900 => {
                let days = self.to_double()?.trunc() as i32; // Handle Lotus 1-2-3 leap year bug
                Ok(days - 25_568 + if days >= 60 { -1 } else { 0 }) // Convert from 1900 to 1970 epoch
            }
            CellType::NumberDateTime1904 | CellType::NumberDate1904 | CellType::NumberTime1904 => {
                let days = self.to_double()?.trunc() as i32; // Handle Lotus 1-2-3 leap year bug
                Ok(days - 25_568 + 1_460) // Convert from 1904 to 1970 epoch
            }
            CellType::IsoDateTime => {
                NaiveDate::parse_from_str(&self.value, "%Y-%m-%d")
                    .map_err(|_| format!("parse '{}' to NaiveDate failed", self.value))
                    .map(|date| date.to_epoch_days())
            }
            CellType::IsoDuration => Ok(0), // Duration only used for ods time
            _ => Err(format!("parse '{}' to date failed", self.value))?
        }
    }

    /// Converts cell value to microseconds since midnight.
    /// Handles Excel time formats and ISO time/duration formats.
    pub(crate) fn to_time(&self) -> Result<i64, String> {
        match self.kind {
            CellType::NumberDateTime1900 | CellType::NumberDateTime1904 |
            CellType::NumberDate1900 | CellType::NumberDate1904 |
            CellType::NumberTime1900 | CellType::NumberTime1904 => {
                let fraction = self.to_double()?;
                Ok((fraction * 86_400_000_000f64).round() as i64)
            }
            CellType::IsoDateTime => {
                NaiveDateTime::parse_from_str(&self.value, "%Y-%m-%dT%H:%M:%S%.f")
                    .map_err(|_| format!("parse '{}' to NaiveDateTime failed", self.value))
                    .map(|datetime| {
                        let time = datetime.time();
                        let seconds = time.num_seconds_from_midnight() as i64;
                        let nanoseconds = time.nanosecond() as i64;
                        (seconds * 1_000_000) + (nanoseconds / 1_000)
                    })
            }
            CellType::IsoDuration => {
                if let Ok(duration) = self.value.parse::<IsoDuration>() {
                    let hour = duration.hour as i64;
                    let minute = duration.minute as i64;
                    let second = duration.second as i64;
                    Ok((hour * 3600 + minute * 60 + second) * 1000000)
                } else {
                    Err(format!("parse '{}' to iso8601 duration failed", self.value))?
                }
            }
            _ => Err(format!("parse '{}' to time failed", self.value))?,
        }
    }

    /// Converts cell value to microseconds since 1970-01-01 epoch.
    /// Handles Excel datetime formats and ISO datetime formats.
    pub(crate) fn to_datetime(&self) -> Result<i64, String> {
        match self.kind {
            CellType::NumberDateTime1900 | CellType::NumberDateTime1904 |
            CellType::NumberDate1900 | CellType::NumberDate1904 |
            CellType::NumberTime1900 | CellType::NumberTime1904 => {
                let days = self.to_date()? as f64;
                let time = self.to_double()?;
                Ok(((days + time.fract()) * 86_400_000_000f64).round() as i64)
            }
            CellType::IsoDateTime => {
                let datetime = if self.value.contains('T') {
                    NaiveDateTime::parse_from_str(&self.value, "%Y-%m-%dT%H:%M:%S%.f")
                        .map_err(|_| format!("parse '{}' to NaiveDateTime failed", self.value))
                } else {
                    NaiveDate::parse_from_str(&self.value, "%Y-%m-%d")
                        .map_err(|_| format!("parse '{}' to NaiveDate failed", self.value))
                        .map(|date| date.and_hms_opt(0, 0, 0).expect("Append 00:00:00"))
                };
                datetime.map(|datetime| datetime.and_utc().timestamp_micros())
            }
            CellType::IsoDuration => self.to_time(),
            _ => Err(format!("parse '{}' to datetime failed", self.value))?,
        }
    }
}

impl Display for Cell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = match self.kind {
            CellType::Boolean => if self.value == "1" { "true" } else { "false" }.to_owned(),
            CellType::NumberDateTime1900 => {
                if let Ok(value) = to_datetime_string(&self.value, false) {
                    value
                } else {
                    panic!(
                        "Parse cell value '{}' at {} to DateTime(1900) failed",
                        self.value,
                        self.reference()
                    );
                }
            }
            CellType::NumberDate1900 => {
                if let Ok(value) = to_date_string(&self.value, false) {
                    value
                } else {
                    panic!(
                        "Parse cell value '{}' at {} to Date(1900) failed",
                        self.value,
                        self.reference()
                    );
                }
            }
            CellType::NumberDateTime1904 => {
                if let Ok(value) = to_datetime_string(&self.value, true) {
                    value
                } else {
                    panic!(
                        "Parse cell value '{}' at {} to DateTime(1904) failed",
                        self.value,
                        self.reference()
                    );
                }
            }
            CellType::NumberDate1904 => {
                if let Ok(value) = to_date_string(&self.value, true) {
                    value
                } else {
                    panic!(
                        "Parse cell value '{}' at {} to Date(1904) failed",
                        self.value,
                        self.reference()
                    );
                }
            }
            CellType::NumberTime1900 | CellType::NumberTime1904 => {
                if let Ok(value) = to_time_string(&self.value) {
                    value
                } else {
                    panic!(
                        "Parse cell value '{}' at {} to Time failed",
                        self.value,
                        self.reference()
                    );
                }
            }
            CellType::IsoDateTime => self.value.replace("T", " "),
            CellType::IsoDuration => self
                .value
                .replace("PT", "")
                .replace("H", ":")
                .replace("M", ":")
                .replace("S", ""),
            _ => self.value.to_owned(),
        };
        write!(f, "{}", value)
    }
}

/// Converts Excel numeric date to ISO date string.
/// Handles Lotus 1-2-3 leap year bug for 1900 epoch.
fn to_date_string(value: &str, is_1904: bool) -> Result<String, RustySheetError> {
    let days = value.parse::<f64>()?.trunc() as i64; // Handle Lotus 1-2-3 leap year bug
    let duration = Duration::days(
        days + if is_1904 {
            1462
        } else if days < 60 {
            1
        } else {
            0
        },
    );
    let date = NaiveDate::from_ymd_opt(1899, 12, 30).expect("NaiveDate Literal") + duration;
    Ok(date.format("%Y-%m-%d").to_string())
}

/// Converts Excel numeric time to ISO time string.
pub(crate) fn to_time_string(value: &str) -> Result<String, RustySheetError> {
    let factor = value.parse::<f64>()?;
    let mut hours = (factor * 86400000f64).round() as i64;
    let milliseconds = hours % 1_000; hours /= 1_000;
    let seconds = hours % 60; hours /= 60;
    let minutes = hours % 60; hours /= 60;
    let timestamp = if milliseconds > 0 {
        format!("{hours:02}:{minutes:02}:{seconds:02}.{milliseconds:06}")
    } else {
        format!("{hours:02}:{minutes:02}:{seconds:02}")
    };
    Ok(timestamp)
}

/// Converts Excel numeric datetime to ISO datetime string.
pub(crate) fn to_datetime_string(value: &str, is_1904: bool) -> Result<String, RustySheetError> {
    if let Some(index) = value.find('.') {
        let date = to_date_string(&value[..index], is_1904)?;
        let time = to_time_string(&value[index..])?;
        Ok(format!("{date} {time}"))
    } else {
        let date = to_date_string(value, is_1904)?;
        Ok(format!("{date} 00:00:00"))
    }
}
