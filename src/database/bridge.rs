use duckdb::vtab::Value;
use libduckdb_sys::duckdb_date;
use libduckdb_sys::duckdb_free;
use libduckdb_sys::duckdb_get_bool;
use libduckdb_sys::duckdb_get_date;
use libduckdb_sys::duckdb_get_double;
use libduckdb_sys::duckdb_get_float;
use libduckdb_sys::duckdb_get_int16;
use libduckdb_sys::duckdb_get_int32;
use libduckdb_sys::duckdb_get_int64;
use libduckdb_sys::duckdb_get_int8;
use libduckdb_sys::duckdb_get_interval;
use libduckdb_sys::duckdb_get_list_child;
use libduckdb_sys::duckdb_get_list_size;
use libduckdb_sys::duckdb_get_map_key;
use libduckdb_sys::duckdb_get_map_size;
use libduckdb_sys::duckdb_get_map_value;
use libduckdb_sys::duckdb_get_struct_child;
use libduckdb_sys::duckdb_get_time;
use libduckdb_sys::duckdb_get_time_tz;
use libduckdb_sys::duckdb_get_timestamp;
use libduckdb_sys::duckdb_get_timestamp_ms;
use libduckdb_sys::duckdb_get_timestamp_ns;
use libduckdb_sys::duckdb_get_timestamp_s;
use libduckdb_sys::duckdb_get_timestamp_tz;
use libduckdb_sys::duckdb_get_uint16;
use libduckdb_sys::duckdb_get_uint32;
use libduckdb_sys::duckdb_get_uint64;
use libduckdb_sys::duckdb_get_uint8;
use libduckdb_sys::duckdb_get_value_type;
use libduckdb_sys::duckdb_get_varchar;
use libduckdb_sys::duckdb_interval;
use libduckdb_sys::duckdb_logical_type;
use libduckdb_sys::duckdb_struct_type_child_count;
use libduckdb_sys::duckdb_struct_type_child_name;
use libduckdb_sys::duckdb_time;
use libduckdb_sys::duckdb_time_tz;
use libduckdb_sys::duckdb_timestamp;
use libduckdb_sys::duckdb_timestamp_ms;
use libduckdb_sys::duckdb_timestamp_ns;
use libduckdb_sys::duckdb_timestamp_s;
use libduckdb_sys::duckdb_value;
use std::ffi::CStr;
use std::os::raw::c_void;

#[allow(dead_code)]
pub(crate) trait ValueBridge {
    /// Gets the raw pointer to the underlying DuckDB value
    ///
    /// # Safety
    /// This method is unsafe as it accesses raw pointers and makes assumptions
    /// about the internal memory layout of DuckDB values
    fn get_value_ptr(&self) -> duckdb_value;

    /// Converts the value to a boolean
    fn to_bool(&self) -> bool {
        unsafe { duckdb_get_bool(self.get_value_ptr()) }
    }

    /// Converts the value to a signed 8-bit integer
    fn to_int8(&self) -> i8 {
        unsafe { duckdb_get_int8(self.get_value_ptr()) }
    }

    /// Converts the value to an unsigned 8-bit integer
    fn to_uint8(&self) -> u8 {
        unsafe { duckdb_get_uint8(self.get_value_ptr()) }
    }

    /// Converts the value to a signed 16-bit integer
    fn to_int16(&self) -> i16 {
        unsafe { duckdb_get_int16(self.get_value_ptr()) }
    }

    /// Converts the value to an unsigned 16-bit integer
    fn to_uint16(&self) -> u16 {
        unsafe { duckdb_get_uint16(self.get_value_ptr()) }
    }

    /// Converts the value to a signed 32-bit integer
    fn to_int32(&self) -> i32 {
        unsafe { duckdb_get_int32(self.get_value_ptr()) }
    }

    /// Converts the value to an unsigned 32-bit integer
    fn to_uint32(&self) -> u32 {
        unsafe { duckdb_get_uint32(self.get_value_ptr()) }
    }

    /// Converts the value to a usize (platform-dependent size)
    ///
    /// This is a convenience method that converts via u32
    fn to_usize(&self) -> usize {
        self.to_uint32() as usize
    }

    /// Converts the value to a signed 64-bit integer
    fn to_int64(&self) -> i64 {
        unsafe { duckdb_get_int64(self.get_value_ptr()) }
    }

    /// Converts the value to an unsigned 64-bit integer
    fn to_uint64(&self) -> u64 {
        unsafe { duckdb_get_uint64(self.get_value_ptr()) }
    }

    /// Converts the value to a 32-bit floating point number
    fn to_float(&self) -> f32 {
        unsafe { duckdb_get_float(self.get_value_ptr()) }
    }

    /// Converts the value to a 64-bit floating point number
    fn to_double(&self) -> f64 {
        unsafe { duckdb_get_double(self.get_value_ptr()) }
    }

    /// Converts the value to a DuckDB date structure
    fn to_date(&self) -> duckdb_date {
        unsafe { duckdb_get_date(self.get_value_ptr()) }
    }

    /// Converts the value to a DuckDB time structure
    fn to_time(&self) -> duckdb_time {
        unsafe { duckdb_get_time(self.get_value_ptr()) }
    }

    /// Converts the value to a DuckDB time with timezone structure
    fn to_time_tz(&self) -> duckdb_time_tz {
        unsafe { duckdb_get_time_tz(self.get_value_ptr()) }
    }

    /// Converts the value to a DuckDB timestamp structure
    fn to_timestamp(&self) -> duckdb_timestamp {
        unsafe { duckdb_get_timestamp(self.get_value_ptr()) }
    }

    /// Converts the value to a DuckDB timestamp with timezone structure
    fn to_timestamp_tz(&self) -> duckdb_timestamp {
        unsafe { duckdb_get_timestamp_tz(self.get_value_ptr()) }
    }

    /// Converts the value to a DuckDB timestamp in seconds
    fn to_timestamp_s(&self) -> duckdb_timestamp_s {
        unsafe { duckdb_get_timestamp_s(self.get_value_ptr()) }
    }

    /// Converts the value to a DuckDB timestamp in milliseconds
    fn to_timestamp_ms(&self) -> duckdb_timestamp_ms {
        unsafe { duckdb_get_timestamp_ms(self.get_value_ptr()) }
    }

    /// Converts the value to a DuckDB timestamp in nanoseconds
    fn to_timestamp_ns(&self) -> duckdb_timestamp_ns {
        unsafe { duckdb_get_timestamp_ns(self.get_value_ptr()) }
    }

    /// Converts the value to a DuckDB interval structure
    fn to_interval(&self) -> duckdb_interval {
        unsafe { duckdb_get_interval(self.get_value_ptr()) }
    }

    /// Converts the value to a UTF-8 string
    ///
    /// The returned string is owned and memory is properly managed
    fn to_varchar(&self) -> String {
        unsafe {
            let varchar = duckdb_get_varchar(self.get_value_ptr());
            let c_str = CStr::from_ptr(varchar);
            let string = c_str.to_string_lossy().into_owned();
            duckdb_free(varchar as *mut c_void);
            string
        }
    }

    /// Converts the value to a vector of DuckDB values (list type)
    fn to_list(&self) -> Vec<Value> {
        unsafe {
            let size = duckdb_get_list_size(self.get_value_ptr());
            (0..size)
                .map(|index| Value::from(duckdb_get_list_child(self.get_value_ptr(), index)))
                .collect()
        }
    }

    /// Converts the value to a vector of key-value pairs (map type)
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

    /// Converts the value to a vector of field name-value pairs (struct type)
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

    /// Returns the logical type of the value
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
    /// **DO NOT USE DIRECTLY**
    fn get_value_ptr(&self) -> duckdb_value {
        // Cast the Value reference to a raw pointer, then reinterpret it as duckdb_value
        // This is a dangerous assumption about the internal memory layout
        unsafe { *(self as *const Value as *const duckdb_value) }
    }
}
