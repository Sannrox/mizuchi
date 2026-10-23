//! Durable stage outputs, so a batch query resumes after coordinator takeover (#162).
//!
//! A staged query writes each stage's output to the shared spill directory and
//! commits it with a record naming the output file and its digest. Each
//! leadership generation (its fencing token) commits to its own records, and
//! the first time a generation opens a query it writes a durable seal listing
//! the earlier commits it adopts: the previous seal's, plus the records the
//! previous sealing generation had published by then. A stage is committed for
//! a generation when it has its own record or an adopted one, so a commit a
//! superseded coordinator publishes after its successor sealed is never
//! adopted, by that successor or any later one, and nothing is rolled back.
//! A commit whose output is missing or does not match its digest fails the
//! query: it never yields a wrong result.
//!
//! Seals form one numbered sequence: a generation claims the next number with
//! an exclusive hard link, so choosing what to adopt and publishing the seal
//! are one atomic step; a seal that loses the race is recomputed against the
//! winner, and a generation older than the last seal's is fenced.
//!
//! Layout under `<spill>/queries/<query id>/`: `stage-<n>.g<gen>.<writer>.arrow`
//! outputs, `stage-<n>.g<gen>.commit` records, and `seal-<n>` seals; records
//! and seals are published with exclusive hard links.

use std::collections::BTreeMap;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use mizuchi_core::batch::Batch;
use mizuchi_core::error::{Error, Result};
use sha2::{Digest as _, Sha256};

use crate::memory::write_spill;

/// One generation's view of a staged query's committed outputs.
#[derive(Debug, Clone)]
pub struct StageCommits {
    root: PathBuf,
    generation: u64,
    /// Stage → record file adopted from earlier generations.
    adopted: BTreeMap<usize, String>,
}

impl StageCommits {
    /// Opens `query_id` under `spill_dir` for leadership `generation` (the
    /// fencing token; 0 when unfenced), sealing what it inherits the first
    /// time. Query ids are 1 to 128 of `[A-Za-z0-9_-]`, so an id can never
    /// name a path outside the directory.
    pub fn open(spill_dir: &Path, query_id: &str, generation: u64) -> Result<Self> {
        if query_id.is_empty()
            || query_id.len() > 128
            || !query_id
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            return Err(Error::Plan(format!(
                "staged query id {query_id:?} must be 1 to 128 of [A-Za-z0-9_-]"
            )));
        }
        // The spill root must already exist: a root created here could vanish
        // in a crash, with every commit under it.
        if !spill_dir.is_dir() {
            return Err(Error::Io(format!(
                "spill directory {} must exist for staged queries",
                spill_dir.display()
            )));
        }
        let queries = spill_dir.join("queries");
        let root = queries.join(query_id);
        std::fs::create_dir_all(&root).map_err(|e| io(&root, e))?;
        // The directory entries themselves must survive a crash, or an
        // acknowledged commit could vanish with its whole query directory.
        for dir in [spill_dir, queries.as_path()] {
            sync_dir(dir)?;
        }
        let mut commits = Self {
            root,
            generation,
            adopted: BTreeMap::new(),
        };
        commits.adopted = commits.seal()?;
        Ok(commits)
    }

    fn record_name(stage: usize, generation: u64) -> String {
        format!("stage-{stage}.g{generation}.commit")
    }

    /// The seals present, in sequence order, with the generation each sealed.
    fn seals(&self) -> Result<Vec<(u64, u64)>> {
        let mut seals = Vec::new();
        for entry in std::fs::read_dir(&self.root).map_err(|e| io(&self.root, e))? {
            let entry = entry.map_err(|e| io(&self.root, e))?;
            if let Some(sequence) = entry
                .file_name()
                .to_str()
                .and_then(|name| name.strip_prefix("seal-"))
                .and_then(|sequence| sequence.parse::<u64>().ok())
            {
                let (generation, _) = self.read_seal(sequence)?;
                seals.push((sequence, generation));
            }
        }
        seals.sort_unstable();
        Ok(seals)
    }

    fn seal_path(&self, sequence: u64) -> PathBuf {
        self.root.join(format!("seal-{sequence:020}"))
    }

    /// A seal's generation and adopted commits.
    fn read_seal(&self, sequence: u64) -> Result<(u64, BTreeMap<usize, String>)> {
        let path = self.seal_path(sequence);
        let unreadable = || Error::Io(format!("seal {} is unreadable", path.display()));
        let text = std::fs::read_to_string(&path).map_err(|_| unreadable())?;
        let mut lines = text.lines();
        let generation = lines
            .next()
            .and_then(|line| line.strip_prefix('g'))
            .and_then(|generation| generation.parse().ok())
            .ok_or_else(unreadable)?;
        let adopted = lines
            .map(|line| {
                line.split_once('\t')
                    .and_then(|(stage, record)| Some((stage.parse().ok()?, record.to_string())))
                    .ok_or_else(unreadable)
            })
            .collect::<Result<_>>()?;
        Ok((generation, adopted))
    }

    /// This generation's sealed inheritance, sealing it on first open: the
    /// last seal's adopted commits plus that sealing generation's records
    /// published by now, claimed as the next seal in the sequence.
    fn seal(&self) -> Result<BTreeMap<usize, String>> {
        loop {
            let seals = self.seals()?;
            if let Some(&(sequence, _)) = seals
                .iter()
                .rev()
                .find(|(_, generation)| *generation == self.generation)
            {
                return Ok(self.read_seal(sequence)?.1);
            }
            let last = seals.last().copied();
            if let Some((_, newest)) = last
                && newest > self.generation
            {
                return Err(Error::Fenced(format!(
                    "generation {newest} already took over this query from generation {}",
                    self.generation
                )));
            }
            let mut adopted = BTreeMap::new();
            if let Some((sequence, previous)) = last {
                adopted = self.read_seal(sequence)?.1;
                for entry in std::fs::read_dir(&self.root).map_err(|e| io(&self.root, e))? {
                    let entry = entry.map_err(|e| io(&self.root, e))?;
                    let name = entry.file_name().to_string_lossy().into_owned();
                    if let Some(stage) = name
                        .strip_suffix(&format!(".g{previous}.commit"))
                        .and_then(|rest| rest.strip_prefix("stage-"))
                        .and_then(|stage| stage.parse::<usize>().ok())
                    {
                        adopted.insert(stage, name);
                    }
                }
            }
            let contents: String = std::iter::once(format!("g{}\n", self.generation))
                .chain(
                    adopted
                        .iter()
                        .map(|(stage, record)| format!("{stage}\t{record}\n")),
                )
                .collect();
            let next = last.map_or(1, |(sequence, _)| sequence + 1);
            if self.publish(&self.seal_path(next), &contents)? {
                return Ok(adopted);
            }
            // Another seal took this number first; decide again against it.
        }
    }

    /// Publishes `contents` at `target` with an exclusive hard link from a
    /// synced staging file. False when `target` already exists.
    fn publish(&self, target: &Path, contents: &str) -> Result<bool> {
        let staged = self
            .root
            .join(format!(".{}.tmp", writer_id(self.generation)));
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&staged)
            .map_err(|e| io(&staged, e))?;
        file.write_all(contents.as_bytes())
            .and_then(|()| file.sync_all())
            .map_err(|e| io(&staged, e))?;
        let linked = std::fs::hard_link(&staged, target);
        let _ = std::fs::remove_file(&staged);
        match linked {
            Ok(()) => sync_dir(&self.root).map(|()| true),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
            Err(e) => Err(io(target, e)),
        }
    }

    /// The committed output of `stage` for this generation, or `None` when
    /// neither this generation nor an adopted one committed it. `definition`
    /// identifies what the stage computes; a commit made for a different
    /// definition, or whose output is missing, altered or unreadable, is an
    /// error.
    pub fn committed(&self, stage: usize, definition: &str) -> Result<Option<Vec<Batch>>> {
        let own = Self::record_name(stage, self.generation);
        let record = if self.root.join(&own).exists() {
            own
        } else {
            match self.adopted.get(&stage) {
                Some(record) => record.clone(),
                None => return Ok(None),
            }
        };
        let partial = |why: &str| {
            Error::Io(format!(
                "stage {stage} of {} is committed but its output is {why}; the query cannot resume",
                self.root.display()
            ))
        };
        let text = std::fs::read_to_string(self.root.join(&record))
            .map_err(|_| partial("missing its commit record"))?;
        let mut lines = text.lines();
        let (Some(output), Some(digest), Some(defined)) =
            (lines.next(), lines.next(), lines.next())
        else {
            return Err(partial("unreadable"));
        };
        if defined != hex(&Sha256::digest(definition.as_bytes())) {
            return Err(Error::Plan(format!(
                "stage {stage} of {} changed since it was committed; use a new query id",
                self.root.display()
            )));
        }
        if output == "empty" && digest == "empty" {
            return Ok(Some(Vec::new()));
        }
        if output.contains('/') || output.contains('\\') || output.starts_with('.') {
            return Err(partial("outside its query directory"));
        }
        let path = self.root.join(output);
        let bytes = std::fs::read(&path).map_err(|_| partial("missing"))?;
        if hex(&Sha256::digest(&bytes)) != digest {
            return Err(partial("altered"));
        }
        read_output(&path)
            .map(Some)
            .map_err(|_| partial("unreadable"))
    }

    /// Writes `batches` as `stage`'s output for `definition` and commits it
    /// for this generation. A commit this generation already made stands.
    pub fn commit(&self, stage: usize, definition: &str, batches: &[Batch]) -> Result<()> {
        let (output, digest) = match batches.first() {
            None => ("empty".to_string(), "empty".to_string()),
            Some(first) => {
                let output = format!(
                    "stage-{stage}.g{}.{}.arrow",
                    self.generation,
                    writer_id(self.generation)
                );
                let path = self.root.join(&output);
                write_spill(&path, first.schema(), batches)?;
                std::fs::File::open(&path)
                    .and_then(|file| file.sync_all())
                    .map_err(|e| io(&path, e))?;
                let bytes = std::fs::read(&path).map_err(|e| io(&path, e))?;
                (output, hex(&Sha256::digest(&bytes)))
            }
        };
        let record = self.root.join(Self::record_name(stage, self.generation));
        let defined = hex(&Sha256::digest(definition.as_bytes()));
        if !self.publish(&record, &format!("{output}\n{digest}\n{defined}\n"))? && output != "empty"
        {
            let _ = std::fs::remove_file(self.root.join(&output));
        }
        Ok(())
    }
}

/// Unique per process, call and moment, so staging files never collide.
fn writer_id(generation: u64) -> String {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    format!(
        "{}-{}-{}-{generation}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_nanos()),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    )
}

/// A committed output with its own schema, so a zero-row stage keeps its
/// columns.
fn read_output(path: &Path) -> Result<Vec<Batch>> {
    let file = std::fs::File::open(path).map_err(|e| io(path, e))?;
    let reader = arrow::ipc::reader::FileReader::try_new(file, None)
        .map_err(|e| Error::Io(e.to_string()))?;
    let schema = reader.schema();
    let batches = reader
        .map(|batch| Batch::from_arrow(batch.map_err(|e| Error::Io(e.to_string()))?))
        .collect::<Result<Vec<_>>>()?;
    if batches.is_empty() {
        return Ok(vec![Batch::empty(schema)?]);
    }
    Ok(batches)
}

fn sync_dir(dir: &Path) -> Result<()> {
    std::fs::File::open(dir)
        .and_then(|dir| dir.sync_all())
        .map_err(|e| io(dir, e))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn io(path: &Path, error: std::io::Error) -> Error {
    Error::Io(format!("stage commit {}: {error}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(0);

    fn spill() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "mizuchi-stage-commit-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn batches(values: &[i64]) -> Vec<Batch> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(
                values.iter().copied().map(Some).collect::<Vec<_>>(),
            ))],
        )
        .unwrap();
        vec![Batch::from_arrow(batch).unwrap()]
    }

    fn values(out: &[Batch]) -> Vec<i64> {
        out.iter()
            .flat_map(|batch| {
                batch
                    .as_arrow()
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
                    .map(|v| v.unwrap())
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    #[test]
    fn a_committed_stage_reads_back_and_its_first_commit_stands() {
        let dir = spill();
        let commits = StageCommits::open(&dir, "q-1", 1).unwrap();
        assert!(commits.committed(0, "def").unwrap().is_none());
        commits.commit(0, "def", &batches(&[1, 2, 3])).unwrap();
        commits.commit(0, "def", &batches(&[9])).unwrap();
        let reopened = StageCommits::open(&dir, "q-1", 1).unwrap();
        assert_eq!(
            values(&reopened.committed(0, "def").unwrap().unwrap()),
            [1, 2, 3]
        );
        commits.commit(1, "def", &[]).unwrap();
        assert!(reopened.committed(1, "def").unwrap().unwrap().is_empty());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_successor_adopts_earlier_commits_but_never_a_late_one() {
        let dir = spill();
        let first = StageCommits::open(&dir, "q-2", 1).unwrap();
        first.commit(0, "def", &batches(&[5, 6])).unwrap();
        // Takeover: generation 2 seals what generation 1 committed so far.
        let second = StageCommits::open(&dir, "q-2", 2).unwrap();
        assert_eq!(
            values(&second.committed(0, "def").unwrap().unwrap()),
            [5, 6]
        );
        // The superseded coordinator publishes a stage after the seal.
        first.commit(1, "def", &batches(&[99])).unwrap();
        assert!(second.committed(1, "def").unwrap().is_none());
        // Nor does any later generation adopt it.
        let third = StageCommits::open(&dir, "q-2", 3).unwrap();
        assert!(third.committed(1, "def").unwrap().is_none());
        assert_eq!(values(&third.committed(0, "def").unwrap().unwrap()), [5, 6]);
        second.commit(1, "def", &batches(&[11])).unwrap();
        assert!(
            third.committed(1, "def").unwrap().is_none(),
            "sealed before it"
        );
        let fourth = StageCommits::open(&dir, "q-2", 4).unwrap();
        assert!(
            fourth.committed(1, "def").unwrap().is_none(),
            "3 sealed first"
        );
        // A generation older than the last seal cannot seal anew.
        assert!(matches!(
            StageCommits::open(&dir, "q-2", 3).map(drop),
            Ok(())
        ));
        let late = StageCommits::open(&dir, "q-2-late", 2).unwrap();
        drop(late);
        StageCommits::open(&dir, "q-2-late", 5).unwrap();
        assert!(matches!(
            StageCommits::open(&dir, "q-2-late", 3),
            Err(Error::Fenced(_))
        ));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_changed_stage_definition_is_not_reused() {
        let dir = spill();
        let commits = StageCommits::open(&dir, "q-5", 1).unwrap();
        commits.commit(0, "SELECT 1 AS x", &batches(&[1])).unwrap();
        let err = commits.committed(0, "SELECT 2 AS x").unwrap_err();
        assert!(
            err.to_string().contains("changed since it was committed"),
            "{err}"
        );
        assert!(StageCommits::open(&dir.join("absent"), "q-5", 1).is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn missing_or_altered_output_fails_instead_of_resuming() {
        let dir = spill();
        let commits = StageCommits::open(&dir, "q-3", 1).unwrap();
        commits.commit(0, "def", &batches(&[1, 2])).unwrap();
        let root = dir.join("queries").join("q-3");
        let output = std::fs::read_dir(&root)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| path.extension().is_some_and(|ext| ext == "arrow"))
            .unwrap();
        let bytes = std::fs::read(&output).unwrap();
        std::fs::write(&output, &bytes[..bytes.len() / 2]).unwrap();
        let err = commits.committed(0, "def").unwrap_err().to_string();
        assert!(err.contains("altered"), "{err}");
        std::fs::remove_file(&output).unwrap();
        let err = commits.committed(0, "def").unwrap_err().to_string();
        assert!(err.contains("missing"), "{err}");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_zero_row_stage_keeps_its_columns() {
        let dir = spill();
        let commits = StageCommits::open(&dir, "q-4", 1).unwrap();
        let schema = batches(&[1])[0].schema();
        commits
            .commit(0, "def", &[Batch::empty(schema.clone()).unwrap()])
            .unwrap();
        let read = commits.committed(0, "def").unwrap().unwrap();
        assert_eq!(read[0].schema(), schema);
        assert_eq!(read.iter().map(Batch::num_rows).sum::<usize>(), 0);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn query_ids_cannot_leave_the_spill_directory() {
        let dir = spill();
        for id in ["", "../x", "a/b", "q.1"] {
            assert!(StageCommits::open(&dir, id, 1).is_err(), "{id:?}");
        }
        let _ = std::fs::remove_dir_all(dir);
    }
}
