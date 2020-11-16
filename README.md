# dv-in-rust
Distributed Versioning in Rust

## 1.0 Paper of reference
Distributed Versioning: Consistent Replication for Scaling Back-end Databases of Dynamic Content Web Sites  
[Link to Paper](https://www.eecg.utoronto.ca/~amza/papers/consistency.pdf)  
Cristiana Amza, Alan L. Cox and Willy Zwaenepoel  


## 2.0 How to use build and run

### 2.1 To build and run
```sh
cargo run --bin dbproxy
cargo run --bin scheduler
cargo run --bin sequencer
```

### 2.2 Project layout
```
src  
├── bin        # Executables
├── common     # Common library
├── dbproxy    # dbproxy library
├── scheduler  # scheduler library
├── sequencer  # sequencer library
└── lib.rs     # Declaration of the mods above
```

### 2.3 To build the entire library
```sh
cargo build
```

### 2.4 To check the entire library
```sh
cargo check
```

### 2.4 References
1. [The book](https://doc.rust-lang.org/book/title-page.html)  
2. [Package Layout](https://doc.rust-lang.org/cargo/guide/project-layout.html)  
3. [Actix Web](https://actix.rs/docs/getting-started/)
4. [Naming Convention](https://doc.rust-lang.org/1.0.0/style/style/naming/README.html)
