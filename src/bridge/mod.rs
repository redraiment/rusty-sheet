use duckdb::vtab::Value;
use libduckdb_sys::{
    duckdb_date, duckdb_free, duckdb_get_bool, duckdb_get_date, duckdb_get_double,
    duckdb_get_float, duckdb_get_int16, duckdb_get_int32, duckdb_get_int64, duckdb_get_int8,
    duckdb_get_interval, duckdb_get_list_child, duckdb_get_list_size, duckdb_get_map_key,
    duckdb_get_map_size, duckdb_get_map_value, duckdb_get_struct_child, duckdb_get_time,
    duckdb_get_time_tz, duckdb_get_timestamp, duckdb_get_timestamp_ms, duckdb_get_timestamp_ns,
    duckdb_get_timestamp_s, duckdb_get_timestamp_tz, duckdb_get_uint16, duckdb_get_uint32,
    duckdb_get_uint64, duckdb_get_uint8, duckdb_get_value_type, duckdb_get_varchar,
    duckdb_interval, duckdb_logical_type, duckdb_struct_type_child_count,
    duckdb_struct_type_child_name, duckdb_time, duckdb_time_tz, duckdb_timestamp,
    duckdb_timestamp_ms, duckdb_timestamp_ns, duckdb_timestamp_s, duckdb_value,
};
use std::{ffi::CStr, os::raw::c_void};

/// A bridge trait that extends the functionality of DuckDB's `Value` type by providing
/// unsafe access to its internal raw pointer and additional type conversion methods.
///
/// # Safety Warning
///
/// This trait contains inherently unsafe operations that bypass Rust's memory safety
/// guarantees. Use with extreme caution and only when you fully understand the risks.
#[allow(dead_code)]
pub(crate) trait ValueBridge {
    /// Extracts the raw `duckdb_value` pointer from the `Value` struct.
    ///
    /// # Safety
    ///
    /// This function is **EXTREMELY DANGEROUS** and **HIGHLY UNSTABLE** because:
    ///
    /// - **Memory Layout Dependency**: Relies on the exact internal memory layout of the
    ///   `Value` struct, which is an implementation detail that can change without notice
    /// - **Version Fragility**: Any update to duckdb-rs that modifies the `Value` struct
    ///   will cause this function to produce undefined behavior
    /// - **No ABI Guarantees**: The internal structure of `Value` is not part of the
    ///   public API and has no stability guarantees
    /// - **Potential Memory Corruption**: Incorrect usage can lead to segfaults, data
    ///   corruption, or other undefined behavior
    /// - **Compiler Optimization Issues**: Future compiler optimizations might break
    ///   the assumptions this code makes about memory layout
    ///
    /// # When This Breaks
    ///
    /// This function will break and cause undefined behavior if:
    /// - duckdb-rs changes the internal layout of `Value`
    /// - Additional fields are added to the `Value` struct
    /// - The struct alignment or padding changes
    /// - You're using a different version of duckdb-rs than this was tested with
    ///
    /// # Requirements for Safe Usage
    ///
    /// - Verify that `std::mem::size_of::<Value>() == std::mem::size_of::<duckdb_value>()`
    /// - Test thoroughly with your specific version of duckdb-rs
    /// - Add version-specific conditional compilation if needed
    /// - Always handle the possibility that the returned pointer is invalid
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use your_crate::ValueBridge;
    ///
    /// let value: Value = /* ... */;
    /// let raw_ptr = unsafe { value.get_value_ptr() };
    /// // raw_ptr may be completely invalid - handle with care!
    /// ```
    unsafe fn get_value_ptr(&self) -> duckdb_value;

    /// Returns the value as a boolean
    fn to_bool(&self) -> bool {
        unsafe { duckdb_get_bool(self.get_value_ptr()) }
    }

    /// Returns the value as an i8
    fn to_int8(&self) -> i8 {
        unsafe { duckdb_get_int8(self.get_value_ptr()) }
    }

    /// Returns the value as an u8
    fn to_uint8(&self) -> u8 {
        unsafe { duckdb_get_uint8(self.get_value_ptr()) }
    }

    /// Returns the value as an i16
    fn to_int16(&self) -> i16 {
        unsafe { duckdb_get_int16(self.get_value_ptr()) }
    }

    /// Returns the value as an u16
    fn to_uint16(&self) -> u16 {
        unsafe { duckdb_get_uint16(self.get_value_ptr()) }
    }

    /// Returns the value as an i32
    fn to_int32(&self) -> i32 {
        unsafe { duckdb_get_int32(self.get_value_ptr()) }
    }

    /// Returns the value as an u32
    fn to_uint32(&self) -> u32 {
        unsafe { duckdb_get_uint32(self.get_value_ptr()) }
    }

    /// Returns the value as a int64
    fn to_int64(&self) -> i64 {
        unsafe { duckdb_get_int64(self.get_value_ptr()) }
    }

    /// Returns the value as a int64
    fn to_uint64(&self) -> u64 {
        unsafe { duckdb_get_uint64(self.get_value_ptr()) }
    }

    /// Returns the value as a float
    fn to_float(&self) -> f32 {
        unsafe { duckdb_get_float(self.get_value_ptr()) }
    }

    /// Returns the value as a double
    fn to_double(&self) -> f64 {
        unsafe { duckdb_get_double(self.get_value_ptr()) }
    }

    /// Returns the value as a date
    fn to_date(&self) -> duckdb_date {
        unsafe { duckdb_get_date(self.get_value_ptr()) }
    }

    /// Returns the value as a time
    fn to_time(&self) -> duckdb_time {
        unsafe { duckdb_get_time(self.get_value_ptr()) }
    }

    /// Returns the value as a time_tz
    fn to_time_tz(&self) -> duckdb_time_tz {
        unsafe { duckdb_get_time_tz(self.get_value_ptr()) }
    }

    /// Returns the value as a timestamp
    fn to_timestamp(&self) -> duckdb_timestamp {
        unsafe { duckdb_get_timestamp(self.get_value_ptr()) }
    }

    /// Returns the value as a timestamp_tz
    fn to_timestamp_tz(&self) -> duckdb_timestamp {
        unsafe { duckdb_get_timestamp_tz(self.get_value_ptr()) }
    }

    /// Returns the value as a timestamp_s
    fn to_timestamp_s(&self) -> duckdb_timestamp_s {
        unsafe { duckdb_get_timestamp_s(self.get_value_ptr()) }
    }

    /// Returns the value as a timestamp_ms
    fn to_timestamp_ms(&self) -> duckdb_timestamp_ms {
        unsafe { duckdb_get_timestamp_ms(self.get_value_ptr()) }
    }

    /// Returns the value as a timestamp_ns
    fn to_timestamp_ns(&self) -> duckdb_timestamp_ns {
        unsafe { duckdb_get_timestamp_ns(self.get_value_ptr()) }
    }

    /// Returns the value as a interval
    fn to_interval(&self) -> duckdb_interval {
        unsafe { duckdb_get_interval(self.get_value_ptr()) }
    }

    /// Returns the value as a String
    fn to_varchar(&self) -> String {
        unsafe {
            let varchar = duckdb_get_varchar(self.get_value_ptr());
            let c_str = CStr::from_ptr(varchar);
            let string = c_str.to_string_lossy().into_owned();
            duckdb_free(varchar as *mut c_void);
            string
        }
    }

    /// Returns the value as a list
    fn to_list(&self) -> Vec<Value> {
        unsafe {
            let size = duckdb_get_list_size(self.get_value_ptr());
            (0..size)
                .map(|index| Value::from(duckdb_get_list_child(self.get_value_ptr(), index)))
                .collect()
        }
    }

    /// Returns the value as a map key & value entries
    fn to_map_entries(&self) -> Vec<(Value, Value)> {
        unsafe {
            let size = duckdb_get_map_size(self.get_value_ptr());
            (0..size)
                .map(|index| {
                    (
                        Value::from(duckdb_get_map_key(self.get_value_ptr(), index)),
                        Value::from(duckdb_get_map_value(self.get_value_ptr(), index)),
                    )
                })
                .collect()
        }
    }

    /// Returns the value as a struct type child names and values
    fn to_struct_properties(&self) -> Vec<(String, Value)> {
        let value_type = self.value_type();
        unsafe {
            let size = duckdb_struct_type_child_count(value_type);
            (0..size)
                .map(|index| {
                    let pointer = duckdb_struct_type_child_name(value_type, index);
                    let c_str = CStr::from_ptr(pointer);
                    let name = c_str.to_string_lossy().to_string();
                    duckdb_free(pointer as *mut c_void);

                    let value = duckdb_get_struct_child(self.get_value_ptr(), index);

                    (name, Value::from(value))
                })
                .collect()
        }
    }

    /// Returns the value logical type
    fn value_type(&self) -> duckdb_logical_type {
        unsafe { duckdb_get_value_type(self.get_value_ptr()) }
    }
}

impl ValueBridge for Value {
    /// # DANGER: Highly unstable memory layout hack
    ///
    /// This implementation assumes that `Value` is a simple wrapper around a single
    /// `duckdb_value` field with no additional data or padding. This assumption
    /// can easily be broken by:
    ///
    /// - Library updates
    /// - Compiler changes
    /// - Different target architectures
    /// - Debug vs release builds
    ///
    /// **DO NOT USE IN DIRECTLY**
    unsafe fn get_value_ptr(&self) -> duckdb_value {
        // Cast the Value reference to a raw pointer, then reinterpret it as duckdb_value
        // This is a dangerous assumption about the internal memory layout
        *(self as *const Value as *const duckdb_value)
    }
}
