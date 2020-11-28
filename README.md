# dv-in-rust
Distributed Versioning in Rust


[![Build Status][actions-badge]][actions-url]


[actions-badge]: https://github.com/lichen-liu/dv-in-rust/workflows/o2versioner-build/badge.svg
[actions-url]: https://github.com/lichen-liu/dv-in-rust/actions?query=workflow%3Ao2versioner-build


## Paper of reference
Distributed Versioning: Consistent Replication for Scaling Back-end Databases of Dynamic Content Web Sites  
[Link to Paper](https://www.eecg.utoronto.ca/~amza/papers/consistency.pdf)  
Cristiana Amza, Alan L. Cox and Willy Zwaenepoel  


## How to use build and run

### To build and run
```sh
cargo run --bin dbproxy_exe
cargo run --bin scheduler_exe
cargo run --bin sequencer_exe
```

### To build and test
```sh
cargo test
# To show stdout at the end
cargo test -- --show-output
# To pipe into stdout
cargo test -- --nocapture
```

### To build the entire library
```sh
cargo build
```

### To check the entire library
```sh
cargo check
```


## Progress

### Framework
- [x] Scheduler
- [x] Sequencer
- [x] DbProxy
- [ ] ~~MySQL protocol~~ Aborted, use msql interface instead

### Features
- [x] msql: simple sql
- [ ] msql: Msql and MsqlText interface
- [x] msql: annotation-based
- [ ] msql: auto annotation
- [x] Begin tx stmt
- [ ] Query stmt
- [ ] Commit&Abort tx stmt
- [ ] Single read
- [ ] Early release
- [ ] Multiple Schedulers


### Project layout
```
o2versioner
├── src  
│  ├── comm       # communication-related
│  ├── core       # core data structure and algorithm
│  ├── dbproxy    # dbproxy library
│  ├── scheduler  # scheduler library
│  ├── sequencer  # sequencer library
│  ├── util       # utility library
│  ├── lib.rs     # declaration of the mods above
│  ├── main.rs    # main executable
│  └── msql.rs    # msql: simple sql for scheduler frontend
└── tests         # system level testing
```

## Architecture

### Sequencer
- `sequenecer::handler::main()` - main entrance
- for every incomming tcp connection - `tokio::spawn(process_connection)`
- `process_connection`
  - Run until the connection is closed
  - Process all requests through this connection
  - Reply response after a request `future` is received and processed


## Notes for asynchronous
1. Everything is around objects that are implemented with `trait Future<Output=T>` or `trait Stream<Item=T>`.
2. `Future<Output=T>` is an asynchronous version of `T`; `Stream<Item=T>` is an asynchonous version of `Iterator<Item=T>`.
`Stream<Item=T>` is essentially an iterator of `Future<Output=T>` that resolves into `T` when being handled.
3. `Future` and `Stream` must be run to complete; otherwise their enclosed closures won't be executed.


### Notes for `trait Future<Output=T>`, `tokio::spawn()`, `.await` and `async`
1. An object implementing `trait Future<Output=T>` can only be transformed into another object implementing `trait Future<Output=Y>`
with side affects once being resolved. This can be done via provided methods from `trait FutureExt` (and/or `trait TryFutureExt`) which is provided for
every object that implements `trait Future`. Or, by using keyword `.await` to nonblockingly yield `T` from `Future<Output=T>`.
2. However, `.await` must resides within `async` functions or blocks, which returns an anonymous object that implements `trait Future`.
The only way for a `Future` to fully resolve is via an executor, such as `tokio::spawn`.
3. Functions or closures with `.await` inside must be declared with `async`, and they will return an anonymous object that implements `trait Future`.
4. `.await` means nonblockingly executing the future. The program is still executed from top to bottom as usual.
`.await` only means the current task won't block the current thread that runs the current task. After `.await` is returned, the next line is executed.
5. Multithreading: OS maps N threads onto K CPU cores.  
Asynchronous: Tokio maps N spawned tasks (via `tokio::spawn()`) onto K worker threads.
6. `.await` (or nonblocking) means yielding the current task, so that the current worker thread can
execute other spawned async tasks. Blocking means the current async task will fully occupy the current
worker thread to spin and do nothing, basically wasting the worker thread pool resources.
7. By default, all `Future` are executed one after one, as they are treated as a single task.
On the other hand, `tokio::spawn()` spawns the argument `Future` as a separate task, which may run on the current thread or
another thread depending on the `tokio::runtime`, but in any cases, the spawned task is "decoupled" from the
parent task, so they can run concurrently (not necessarily in parallel).
8. `tokio::spawn()` returns a handle of `Future`, and this `Future` must also be properly `.await` or `tokio::try_join!()` for it to execute to complete,
similar to a thread join.
9. `async` closure is not yet supported, but can be achieved by having a closure returning a `Future`. This can be done by:
```rust
|item| {
    // clone the data that needs to go into the async block
    let x_cloned = x.clone();
    // This block basically returns a `Future<Output=Result<(), _>>` that captures the actions enclosed
    async move {
        some_async_fn(x_cloned).await;
        Ok(())
    }
})
```

### Notes for `trait Stream<Item=T>`
1. `trait Stream` does not imply `trait Future`, they are different.
2. Object implementing `trait Stream` can be drained to completion. This draining process is however a `Future`.
3. `Stream<Item=T>` is essentially `Iterator<Item: Future<Output=T>>`, and operations on `Stream` is similar to those on `Iterator` objects.
These provided methods are inside `trait StreamExt` (and/or `trait TryStreamExt`), which are free for objects implementing `trait Stream`.
All of these methods are essentially applying a closure on `T` that is yielded by `Stream<Item=T>`.
4. `trait Stream::map()`: synchronously converts `Stream<Item=T>` to `Stream<Item=Y>` by mapping yielded `T` directly to `Y` via the closure. The closure is synchronous.
5. `trait Stream::then()`: asynchronously converts `Stream<Item=T>` to `Stream<Item=Y>` by mapping yielded `T` to `Future<Output=Y>` via the closure. The closure is asynchronous. Then this `Future<Output=Y>` is yielded (like calling `.await` on it) to `Y` implicitly and automatically by the method.
6. `trait Stream::for_each`: asynchronously converts `Stream<Item=T>` to `Future<Output=()>` by mapping yielded `T` to `Future<Output=()>` via the closure. The closure is asynchronous. As all items inside the `Stream<Item=T>` are asynchronously converted to `Future<Output=()>`, this essentially means the `Stream` is drained to complete. The returned `Future<Output=()>` can then be `.await` or `tokio::spawn()` to faciliate side affect of the closure to be executed. Note, all `Future<Output=()>` returned by the closure are then being yielded back to back as one serial task, using `trait Stream::for_each_concurrent` can spawn them as concurrent tasks once `T` is yielded and `Future<Output=()>` is returned by the closure.


## References
1. [The book](https://doc.rust-lang.org/book/title-page.html)  
2. [Package Layout](https://doc.rust-lang.org/cargo/guide/project-layout.html)  
3. [Actix Web](https://actix.rs/docs/getting-started/)
4. [Naming Convention](https://doc.rust-lang.org/1.0.0/style/style/naming/README.html)
