//! Grammar fuzz: generate supported SQL and check pull vs multi-worker results.

#![no_main]

use libfuzzer_sys::fuzz_target;
use mizuchi_fuzz::{grammar_sql, rows, session};

fuzz_target!(|data: &[u8]| {
    let sql = grammar_sql(data);
    let pull = session();
    let staged = session().with_workers(2).expect("workers");
    match (pull.execute_sql(&sql), staged.execute_sql(&sql)) {
        (Ok(a), Ok(b)) => assert_eq!(rows(&a), rows(&b), "{sql}"),
        (Err(_), Err(_)) => {}
        (Ok(_), Err(e)) => panic!("workers=2 failed for {sql}: {e}"),
        (Err(e), Ok(_)) => panic!("pull failed for {sql}: {e}"),
    }
});
