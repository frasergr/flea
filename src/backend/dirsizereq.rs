// The one-at-a-time directory-size worker, kept off the request loop so a slow tree cannot stall input.
use crate::backend::dirsize::{self, DirSize};
use crate::backend::events::Event;
use crate::backend::mime::Db;
use crate::backend::ordering;
use crate::backend::proto::dirsized_line;
use crate::backend::state::State;
use crate::backend::thumbs::Pool;
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
    pub report: bool,
}

pub struct Running {
    pub job: Job,
    pub cancel: Arc<AtomicBool>,
}

pub struct PendingSort {
    pub line: String,
    pub remaining: usize,
    pub cache_generation: u64,
}

pub struct Done {
    job: Job,
    result: Option<DirSize>,
    ms: f64,
}

// Size sorting starts immediately in name order, then this queue obtains every recursive key for one final reorder.
pub fn begin_size_sort(st: &mut State, line: &str) {
    if crate::json::field_str(line, "by").as_deref() != Some("size") {
        return;
    }
    let mut remaining = 0;
    for row in 0..st.listing.len() {
        if !st.listing.is_dir(row) {
            continue;
        }
        st.dirsize_queue.push_back(Job {
            row,
            path: st.base.join(st.listing.name(row)),
            row_generation: st.dirsize_row_generation,
            cache_generation: st.dirsize_cache_generation,
            report: false,
        });
        remaining += 1;
    }
    if remaining > 0 {
        st.dirsize_sort = Some(PendingSort {
            line: line.to_string(),
            remaining,
            cache_generation: st.dirsize_cache_generation,
        });
    }
}

// Answered paths are re-answered at once; viewport requests upgrade sort-only jobs so their cells fill too.
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
        if let Some(running) = st.dirsize_running.as_mut().filter(|running| {
            running.job.row_generation == st.dirsize_row_generation && running.job.path == path
        }) {
            running.job.row = row;
            running.job.report = true;
            continue;
        }
        if let Some(job) = st.dirsize_queue.iter_mut().find(|job| {
            job.row_generation == st.dirsize_row_generation && job.path == path
        }) {
            job.row = row;
            job.report = true;
            continue;
        }
        st.dirsize_queue.push_back(Job {
            row,
            path,
            row_generation: st.dirsize_row_generation,
            cache_generation: st.dirsize_cache_generation,
            report: true,
        });
    }
    out.flush().ok();
}

// At most one recursive walk runs; a deque keeps an all-folder size sort linear in its directory count.
pub fn start_next(out: &mut BufWriter<io::Stdout>, st: &mut State, tx: &Sender<Event>) {
    while st.dirsize_running.is_none() {
        let Some(job) = st.dirsize_queue.pop_front() else { break };
        if let Some(&(bytes, partial)) = st.dirsizes.get(&job.path) {
            if job.report {
                writeln!(out, "{}", dirsized_line(job.row, bytes, partial, 0.0)).ok();
            }
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

// Completion either fills one requested cell or applies the all-folder pass's one final reorder.
pub fn report_done(out: &mut BufWriter<io::Stdout>, st: &mut State, done: Done, mime: &Db, pool: &Pool) {
    let active = st.dirsize_running.take().map(|running| running.job);
    let Some(result) = done.result else { return };
    let valid = done.job.cache_generation == st.dirsize_cache_generation;
    if valid {
        let fresh = st
            .dirsizes
            .insert(done.job.path.clone(), (result.bytes, result.partial))
            .is_none();
        if fresh {
            if let Some(sort) = st
                .dirsize_sort
                .as_mut()
                .filter(|sort| sort.cache_generation == done.job.cache_generation)
            {
                sort.remaining = sort.remaining.saturating_sub(1);
            }
        }
    }
    if st.dirsize_sort.as_ref().is_some_and(|sort| sort.remaining == 0) {
        let sort = st.dirsize_sort.take().unwrap();
        if ordering::request_with_dir_sizes(
            &mut st.listing,
            &st.base,
            mime,
            &sort.line,
            &st.dirsizes,
        )
        .is_ok()
        {
            st.dirsize_row_generation = st.dirsize_row_generation.wrapping_add(1);
            st.outstanding = st.outstanding.saturating_sub(pool.cancel_all().len());
            st.asked.clear();
            writeln!(out, "{}", r#"{"t":"dirsorted"}"#).ok();
            out.flush().ok();
            return;
        }
    }
    let answer = active.as_ref().filter(|job| {
        job.report
            && valid
            && job.row_generation == st.dirsize_row_generation
            && job.row < st.listing.len()
            && st.base.join(st.listing.name(job.row)) == done.job.path
    });
    if let Some(job) = answer {
        writeln!(
            out,
            "{}",
            dirsized_line(job.row, result.bytes, result.partial, done.ms)
        )
        .ok();
        out.flush().ok();
    }
}

// A fling suppresses obsolete cell answers but does not abort the all-folder pass needed for an exact size order.
pub fn cancel_viewport(st: &mut State) {
    if st.dirsize_sort.is_some() {
        if let Some(running) = &mut st.dirsize_running {
            running.job.report = false;
        }
        for job in &mut st.dirsize_queue {
            job.report = false;
        }
    } else {
        cancel_dirsizes(st, false);
    }
}

// A new listing or row order cancels the entire pass and invalidates its generation.
pub fn cancel_dirsizes(st: &mut State, clear_cache: bool) {
    if let Some(running) = &st.dirsize_running {
        running.cancel.store(true, Ordering::Relaxed);
    }
    st.dirsize_queue.clear();
    st.dirsize_sort = None;
    st.dirsize_row_generation = st.dirsize_row_generation.wrapping_add(1);
    if clear_cache {
        st.dirsizes.clear();
        st.dirsize_cache_generation = st.dirsize_cache_generation.wrapping_add(1);
    }
}
