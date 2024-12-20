3.03 KB •78 lines•Formatting may be inconsistent from source
# BINSEQ Format Specification
Version 1.0

## Overview
BINSEQ (.bsq) is a binary file format designed for efficient storage of fixed-length DNA sequences. It uses 2-bit encoding for nucleotides and focuses exclusively on sequence data, optimizing for modern high-throughput sequencing applications where quality scores and sequence identifiers are not required.

## File Structure
A BINSEQ file consists of two sections:
1. Fixed-size header (16 bytes)
2. Record data section

### Header Format (16 bytes total)
| Offset | Size (bytes) | Description                     | Type    |
|--------|-------------|---------------------------------|---------|
| 0      | 4          | Magic number (0x42534551)       | uint32  |
| 4      | 1          | Format version (currently 1)     | uint8   |
| 5      | 4          | Sequence length                  | uint32  |
| 9      | 7          | Reserved for future use         | bytes   |

### Record Format
Each record consists of:
1. Flag field (4 bytes, uint32)
2. Sequence data (ceil(N/4) bytes, where N is sequence length)

The flag field is implementation-defined and can be used for filtering, metadata, or other purposes. The placement of the flag field at the start of each record enables efficient filtering without reading sequence data.

Total record size = 4 + ceil(N/4) bytes

## Encoding
- Each nucleotide is encoded using 2 bits:
  - A = 00
  - C = 01
  - G = 10
  - T = 11
- Non-ATCG characters are encoded as A (00)
- Bits are packed into bytes from left to right
- The final byte of sequence data is padded with zeros if the sequence length is not divisible by 4

## Implementation Notes
- Records should be processed in 32-base chunks for optimal SIMD operations
- Random access to any record can be calculated as: record_start = 16 + (record_index * record_size)
- Total number of records can be calculated as: (file_size - 16) / record_size
- Record size calculation: record_size = 4 + ceil(sequence_length/4)
- Flag field placement allows for efficient filtering strategies:
  - Records can be skipped based on flag values without reading sequence data
  - Flag checks can be vectorized for parallel processing
  - Memory access patterns are predictable for better cache utilization

## Example Storage Requirements
Common sequence lengths:
- 50bp reads:
  - Sequence: ceil(50/4) = 13 bytes
  - Flag: 4 bytes
  - Total per record: 17 bytes
- 100bp reads:
  - Sequence: ceil(100/4) = 25 bytes
  - Flag: 4 bytes
  - Total per record: 29 bytes
- 300bp reads:
  - Sequence: ceil(300/4) = 75 bytes
  - Flag: 4 bytes
  - Total per record: 79 bytes

## Validation
Implementations should verify:
1. Correct magic number
2. Compatible version number
3. Sequence length is greater than 0
4. File size minus header (16 bytes) is divisible by the record size

## Future Considerations
- The 7 reserved bytes in the header allow for future format extensions
- The 32-bit flag field provides space for implementation-specific features such as:
  - Quality score summaries
  - Filtering flags
  - Read group identifiers
  - Processing state
  - Count data
