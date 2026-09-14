// The one-at-a-time directory-size worker, kept off the request loop so a slow tree cannot stall input.
use crate::backend::dirsize::{self, DirSize};
use crate::backend::events::Event;
use crate::backend::proto::dirsized_line;
use crate::backend::state::State;
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

#[derive(Clone)]
pub struct Job {
    pub row: usize,
    pub path: PathBuf,
    pub row_generation: u64,
    pub cache_generation: u64,
}

pub struct Running {
    pub job: Job,
    pub cancel: Arc<AtomicBool>,
}

pub struct Done {
    job: Job,
    result: Option<DirSize>,
    ms: f64,
}

// Answered paths are re-answered at once; the path key survives a sort without confusing the row it moved to.
pub fn queue_dirsizes(out: &mut BufWriter<io::Stdout>, st: &mut State, rows: &[usize]) {
    for &row in rows {
        if row >= st.listing.len() || !st.listing.is_dir(row) {
            continue;
        }
        let path = st.base.join(st.listing.name(row));
        if let Some(&(bytes, partial)) = st.dirsizes.get(&path) {
            writeln!(out, "{}", dirsized_line(row, bytes, partial, 0.0)).ok();
            continue;
        }
        let already_running = st.dirsize_running.as_ref().is_some_and(|running| {
            running.job.row_generation == st.dirsize_row_generation && running.job.path == path
        });
        let already_queued = st
            .dirsize_queue
            .iter()
            .any(|job| job.row_generation == st.dirsize_row_generation && job.path == path);
        if already_running || already_queued {
            continue;
        }
        st.dirsize_queue.push(Job {
            row,
            path,
            row_generation: st.dirsize_row_generation,
            cache_generation: st.dirsize_cache_generation,
        });
    }
    out.flush().ok();
}

// At most one recursive walk runs. Cache hits queued behind a cancelled walk are drained without spawning.
pub fn start_next(out: &mut BufWriter<io::Stdout>, st: &mut State, tx: &Sender<Event>) {
    while st.dirsize_running.is_none() && !st.dirsize_queue.is_empty() {
        let job = st.dirsize_queue.remove(0);
        if let Some(&(bytes, partial)) = st.dirsizes.get(&job.path) {
            writeln!(out, "{}", dirsized_line(job.row, bytes, partial, 0.0)).ok();
            continue;
        }
        let cancel = Arc::new(AtomicBool::new(false));
        st.dirsize_running = Some(Running {
            job: job.clone(),
            cancel: Arc::clone(&cancel),
        });
        let replies = tx.clone();
        thread::spawn(move || {
            let started = Instant::now();
            let result = dirsize::walk_cancellable(&job.path, &cancel);
            let ms = started.elapsed().as_secs_f64() * 1000.0;
            let _ = replies.send(Event::DirSize(Done { job, result, ms }));
        });
    }
    out.flush().ok();
}

// A completion can fill the path cache after a sort, but only its original row generation may receive the line.
pub fn report_done(out: &mut BufWriter<io::Stdout>, st: &mut State, done: Done) {
    st.dirsize_running.take();
    let Some(result) = done.result else { return };
    if done.job.cache_generation == st.dirsize_cache_generation {
        st.dirsizes
            .insert(done.job.path.clone(), (result.bytes, result.partial));
    }
    let same_row = done.job.row_generation == st.dirsize_row_generation
        && done.job.row < st.listing.len()
        && st.base.join(st.listing.name(done.job.row)) == done.job.path;
    if same_row {
        writeln!(
            out,
            "{}",
            dirsized_line(done.job.row, result.bytes, result.partial, done.ms)
        )
        .ok();
        out.flush().ok();
    }
}

// A new viewport or row order stops stale IO promptly. A fresh listing also expires path-keyed answers.
pub fn cancel_dirsizes(st: &mut State, clear_cache: bool) {
    if let Some(running) = &st.dirsize_running {
        running.cancel.store(true, Ordering::Relaxed);
    }
    st.dirsize_queue.clear();
    st.dirsize_row_generation = st.dirsize_row_generation.wrapping_add(1);
    if clear_cache {
        st.dirsizes.clear();
        st.dirsize_cache_generation = st.dirsize_cache_generation.wrapping_add(1);
    }
}
