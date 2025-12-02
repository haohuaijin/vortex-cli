# Vortex CLI

A command-line tool for inspecting and analyzing Vortex format files. This CLI provides utilities to examine metadata, schemas, layouts, and statistics of Vortex columnar data files.

## Features

- **Metadata Inspection**: View basic file information including row counts and data types
- **Schema Display**: Examine Arrow schemas and field definitions
- **Layout Analysis**: Inspect encoding strategies and data organization
- **Comprehensive Inspection**: Get all information in a single command
- **Multiple Output Formats**: Support for both human-readable text and JSON output
- **Optimized Performance**: Built with mimalloc allocator for improved memory management

## Installation

### Prerequisites

- Rust toolchain (2024 edition)
- Cargo package manager

### Build from Source

```bash
git clone <repository-url>
cd vortex-cli
cargo build --release
```

The compiled binary will be available at `target/release/vortex-cli`.

## Usage

### Basic Syntax

```bash
vortex-cli <COMMAND> [OPTIONS] <FILE>
```

### Available Commands

#### 1. Metadata

Display basic metadata information from a Vortex file.

```bash
vortex-cli metadata <FILE> [OPTIONS]
```

**Options:**
- `-f, --format <FORMAT>`: Output format (json or text) [default: text]

**Example:**
```bash
vortex-cli metadata data.vortex
vortex-cli metadata data.vortex -f json
```

#### 2. Schema

Display the Arrow schema from a Vortex file.

```bash
vortex-cli schema <FILE> [OPTIONS]
```

**Options:**
- `-f, --format <FORMAT>`: Output format (json or text) [default: text]
- `-v, --verbose`: Show detailed field information

**Example:**
```bash
vortex-cli schema data.vortex
vortex-cli schema data.vortex -v
vortex-cli schema data.vortex -f json
```

#### 3. Layout

Display layout and encoding information from a Vortex file.

```bash
vortex-cli layout <FILE> [OPTIONS]
```

**Options:**
- `-f, --format <FORMAT>`: Output format (json or text) [default: text]
- `-v, --verbose`: Show detailed layout tree

**Example:**
```bash
vortex-cli layout data.vortex
vortex-cli layout data.vortex -v
```

#### 4. Inspect

Display comprehensive information including metadata, schema, and layout.

```bash
vortex-cli inspect <FILE> [OPTIONS]
```

**Options:**
- `-f, --format <FORMAT>`: Output format (json or text) [default: text]
- `-v, --verbose`: Show verbose output with detailed information

**Example:**
```bash
vortex-cli inspect data.vortex
vortex-cli inspect data.vortex -v
vortex-cli inspect data.vortex -f json
```

## Output Formats

### Text Format

The default text format provides human-readable, formatted output with tables and hierarchical displays. Perfect for quick inspection and debugging.

### JSON Format

The JSON format outputs structured data that can be easily parsed by other tools or scripts. Useful for automation and integration with data pipelines.

## Dependencies

The project relies on the following key dependencies:

- **vortex**: Core Vortex library for columnar data handling
- **tokio**: Async runtime for file operations
- **clap**: Command-line argument parsing
- **arrow/parquet**: Apache Arrow and Parquet format support
- **datafusion**: Query engine integration
- **mimalloc**: High-performance memory allocator
- **anyhow**: Error handling

## Development

### Project Structure

```
vortex-cli/
├── Cargo.toml          # Project dependencies and metadata
├── src/
│   └── main.rs         # Main CLI implementation
└── README.md           # This file
```

### Building for Development

```bash
cargo build
```

### Running Tests

```bash
cargo test
```

### Running the CLI in Development

```bash
cargo run -- <COMMAND> <FILE> [OPTIONS]
```

## About Vortex Format

Vortex is a columnar data format designed for efficient storage and processing of structured data. It supports various encoding strategies to optimize for different data patterns and query workloads.

Key features of Vortex format:
- Multiple encoding strategies per column
- Support for nested and complex data types
- Compatible with Apache Arrow
- Optimized for analytical queries

## License

See the project repository for license information.

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.

## Support

For questions and support, please open an issue in the project repository.
