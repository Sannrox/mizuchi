//! Byte-level parser fuzz: syntax errors fail closed; the parser must not panic.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(sql) = std::str::from_utf8(data) else {
        return;
    };
    let _ = mizuchi::parse_sql(sql);
});
