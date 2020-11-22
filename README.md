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
cargo run --bin dbproxy
cargo run --bin scheduler
cargo run --bin sequencer
```

### To build and test
```sh
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
src  
├── bin        # Executables
├── core       # communication-related
├── core       # core data structure and algorithm
├── dbproxy    # dbproxy library
├── scheduler  # scheduler library
├── sequencer  # sequencer library
└── lib.rs     # Declaration of the mods above
```

### References
1. [The book](https://doc.rust-lang.org/book/title-page.html)  
2. [Package Layout](https://doc.rust-lang.org/cargo/guide/project-layout.html)  
3. [Actix Web](https://actix.rs/docs/getting-started/)
4. [Naming Convention](https://doc.rust-lang.org/1.0.0/style/style/naming/README.html)
