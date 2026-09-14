// Everything the read loop holds: the tables it parses once, and the listing state it mutates.
use crate::backend::aliases::Aliases;
use crate::backend::archive::Formats;
use crate::backend::dirsizereq::{Job as DirSizeJob, PendingSort as DirSizeSort, Running as DirSizeRunning};
use crate::backend::icons::Names;
use crate::backend::kind::Kinds;
use crate::backend::listing::Listing;
use crate::backend::mime::Db;
use crate::backend::search::Search;
use crate::backend::thumbspec::Thumbnailers;
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

// Read once for the process: a per-window load would put a file read inside the viewport path.
pub struct Tables {
    pub mime: Db,
    pub icons: Names,
    pub aliases: Arc<Aliases>,
    pub thumbs: Arc<Thumbnailers>,
    // A type's Kind text never changes for the process's life, unlike State's per-listing caches; RefCell because the loop is single-threaded.
    pub kinds: RefCell<Kinds>,
    // Probed once at startup, the same discipline thumbspec.rs applies to a thumbnailer's program.
    pub formats: Arc<Formats>,
}

// Everything the loop mutates, gathered so a handler takes one borrow instead of ten arguments.
pub struct State {
    pub listing: Listing,
    pub base: PathBuf,
    // Only the rows a client named, so this never grows with the directory; see AGENTS.md "Thumbnail requests".
    pub asked: Vec<(PathBuf, usize)>,
    pub outstanding: usize,
    // Answered directory paths for the current listing and row order.
    pub dirsizes: HashMap<PathBuf, (u64, bool)>,
    // One background walker and the paths waiting behind it; a deque keeps all-folder sorts linear.
    pub dirsize_running: Option<DirSizeRunning>,
    pub dirsize_queue: VecDeque<DirSizeJob>,
    pub dirsize_sort: Option<DirSizeSort>,
    // Row generation changes on reorder/cancel; cache generation changes whenever those answers are invalidated.
    pub dirsize_row_generation: u64,
    pub dirsize_cache_generation: u64,
    // The subtree walk the loop ticks; None means no search is running.
    pub search: Option<Search>,
    // When the running walk last announced its count, so SEARCH_REPORT can throttle the stream.
    pub search_reported: Instant,
}
