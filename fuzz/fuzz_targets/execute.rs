//! Execution fuzz: the engine returns an error instead of panicking.

#![no_main]

use libfuzzer_sys::fuzz_target;
use mizuchi_fuzz::session;

fuzz_target!(|data: &[u8]| {
    let Ok(sql) = std::str::from_utf8(data) else {
        return;
    };
    let s = session();
    let _ = s.execute_sql(sql);
});
