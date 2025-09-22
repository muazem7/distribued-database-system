# Distributed Database System

This repository contains a Java implementation of a distributed database system designed for educational purposes. It provides a modular framework for understanding and experimenting with database concepts, distributed storage, indexing, and MapReduce operations.

## Project Structure

- **`src/main/java/*`** – Main source code organized by functionality:
  - **`access/`** – Abstract classes for table access, indices, and record handling.
  - **`buffer/`** – Buffer management with different replacement policies.
  - **`catalog/`** – Catalog manager and database object definitions (tables, indices, attributes, data types).
  - **`mapReduce/`** – MapReduce framework implementation including executors, tasks, operators, and exercises.
  - **`net/`** – Networking layer with TCP client/server and receive handlers.
  - **`operator/`** – Query operators like joins, projections, aggregations, and sort operations.
  - **`storage/`** – Disk and page management, record storage, and SQL type abstractions.

## Features

- Abstract and concrete implementations of table access and indexing.
- Buffer management with multiple replacement policies.
- MapReduce framework supporting single-phase and multi-phase jobs.
- Networking support for distributed execution.
- Core relational operators: selection, projection, join, aggregation, and sort.
- Extensible design for adding new operators or exercises.
- Comprehensive test suite for exercises and modules.

## Usage

1. Clone the repository:

```bash
   git clone <repository-url>
   cd <repository-directory>
```

2. Build the project using Maven:

```bash
    mvn clean install
```

3. Run tests:

```bash
    mvn test
```

## License

This repository is provided for educational purposes and does not include a formal license. Please use responsibly.