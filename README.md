# BINSEQ Format Specification

[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE.md)
![actions status](https://github.com/arcinstitute/binseq/workflows/CI/badge.svg)
[![Crates.io](https://img.shields.io/crates/d/binseq?color=orange&label=crates.io)](https://crates.io/crates/binseq)
[![docs.rs](https://img.shields.io/docsrs/binseq?color=green&label=docs.rs)](https://docs.rs/binseq/latest/binseq/)

## Overview

BINSEQ is a binary file format family designed for efficient storage and processing of DNA sequences.
They make use of two-bit encoding for nucleotides and are optimized for high-performance parallel processing.

BINSEQ currently has two flavors:

1. **BQ**: (`*.bq`) files are for _fixed-length_ records **without** quality scores.
2. **VBQ**: (`*.vbq`) files are for _variable-length_ records **with optional** quality scores and headers.

Both flavors support both single and paired sequences.

## Getting Started

This is a **library** for reading and writing BINSEQ files, for a **command-line interface** see [bqtools](https://github.com/arcinstitute/bqtools).

To get started please refer to our [documentation](https://docs.rs/binseq/latest/binseq/).
For example programs which make use of the library check out our [examples directory](https://github.com/arcinstitute/binseq/tree/main/examples).

For more information about the BINSEQ file family, please refer to our [preprint](https://www.biorxiv.org/content/10.1101/2025.04.08.647863v1).
