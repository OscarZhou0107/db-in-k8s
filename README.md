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
# To capture stdout
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
│  └── main.rs    # main executable
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


## Notes for Tokio, `async` and `.await`
1. Everything is around `future` or `stream` (like `Vec<future>`).
2. `future` and `stream` must be run to complete; otherwise their enclosed closures won't be executed.
3. `future` needs to be executed eventually by `.await`. If a `inner: future` lives inside another `outer: future`,
`outer.await` does not automatically imply `inner` will be executed nor `inner.await` is applied. In such case, `inner.await`
needs to be applied manually inside `outer`.
4. Functions or closures with `.await` inside must be declared with `async`, and they will return a `future`.
5. `.await` means nonblockingly executing the future. The program is still executed from top to bottom as usual.
`.await` only means the current thread won't be blocked or spinning to wait for the `future` to return.
After `.await` is returned, the next line is executed.
6. Multithreading: OS maps N threads onto K CPU cores.  
Asynchronous: Tokio maps N spawned async functions (`tokio::spawn()`) onto K worker threads.
7. `.await` (or nonblocking) means yielding the current spawned async function, so that the current worker thread can
execute other spawned async functions. Blocking means the current spawned async function will fully occupy the current
worker thread to spin and do nothing, basically wasting the worker thread pool resources.
8. `tokio::spawn()` returns a `future`, and this `future` must also be properly `.await` for it to execute to complete,
similar to a thread join.

## References
1. [The book](https://doc.rust-lang.org/book/title-page.html)  
2. [Package Layout](https://doc.rust-lang.org/cargo/guide/project-layout.html)  
3. [Actix Web](https://actix.rs/docs/getting-started/)
4. [Naming Convention](https://doc.rust-lang.org/1.0.0/style/style/naming/README.html)
