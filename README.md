# BINSEQ Format Specification
Version 1.0

## Overview
BINSEQ (.bq) is a binary file format designed for efficient storage of fixed-length DNA sequences.
It uses 2-bit encoding for nucleotides and focuses exclusively on sequence data, optimizing for modern high-throughput sequencing applications where quality scores and sequence identifiers are not required.

## File Structure
A BINSEQ file consists of two sections:
1. Fixed-size header (32 bytes)
2. Record data section

### Header Format (32 bytes total)
| Offset | Size (bytes) | Name     | Description                     | Type    |
|--------|--------------|----------|---------------------------------|---------|
| 0      | 4            | magic    | Magic number (0x42534551)       | uint32  |
| 4      | 1            | format   | Format version (currently 2)    | uint8   |
| 5      | 4            | slen     | Sequence length (primary)       | uint32  |
| 9      | 4            | xlen     | Sequence length (secondary)     | uint32  |
| 13     | 19           | reserved | Reserved for future use         | bytes   |

### Record Format
Each record consists of:
1. Flag field (8 bytes, uint64)
2. Sequence data (ceil(N/32) * 8 bytes, where N is sequence length)

The flag field is implementation-defined and can be used for filtering, metadata, or other purposes. The placement of the flag field at the start of each record enables efficient filtering without reading sequence data.

Total record size = 8 + (ceil(N/32) * 8) bytes, where N is sequence length

## Encoding
- Each nucleotide is encoded using 2 bits:
  - A = 00
  - C = 01
  - G = 10
  - T = 11
- Non-ATCG characters are **unsupported**.
- Sequences are stored in Little-Endian order
- The final u64 of sequence data is padded with zeros if the sequence length is not divisible by 32

The following sequence: `ACGT` would be encoded as:

```text
# A := 00
# C := 01
# G := 10
# T := 11

0x11100100
```

Note that the little-endian encoding reverses the order of the nucleotides in raw binary form.

## Implementation Notes
- Sequences are stored in u64 chunks, each holding up to 32 bases
- Random access to any record can be calculated as:
  - record_size = 8 + (ceil(sequence_length/32) * 8)
  - record_start = 16 + (record_index * record_size)
- Total number of records can be calculated as: (file_size - 16) / record_size
- Flag field placement allows for efficient filtering strategies:
  - Records can be skipped based on flag values without reading sequence data
  - Flag checks can be vectorized for parallel processing
  - Memory access patterns are predictable for better cache utilization

## Example Storage Requirements
Common sequence lengths:
- 32bp reads:
  - Sequence: 1 * 8 = 8 bytes (fits in one u64)
  - Flag: 8 bytes
  - Total per record: 16 bytes
- 100bp reads:
  - Sequence: 4 * 8 = 32 bytes (requires four u64s)
  - Flag: 8 bytes
  - Total per record: 40 bytes
- 150bp reads:
  - Sequence: 5 * 8 = 40 bytes (requires five u64s)
  - Flag: 8 bytes
  - Total per record: 48 bytes

## Validation
Implementations should verify:
1. Correct magic number
2. Compatible version number
3. Sequence length is greater than 0
4. File size minus header (32 bytes) is divisible by the record size

## Future Considerations
- The 7 reserved bytes in the header allow for future format extensions
- The 64-bit flag field provides space for implementation-specific features such as:
  - Quality score summaries
  - Filtering flags
  - Read group identifiers
  - Processing state
  - Count data
