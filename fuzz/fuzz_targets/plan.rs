//! Binder fuzz: unknown SQL fails closed against a tiny catalog.

#![no_main]

use libfuzzer_sys::fuzz_target;
use mizuchi_fuzz::catalog;

fuzz_target!(|data: &[u8]| {
    let Ok(sql) = std::str::from_utf8(data) else {
        return;
    };
    let _ = mizuchi::plan_sql(sql, &catalog());
});
