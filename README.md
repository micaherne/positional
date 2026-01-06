# Positional

A git-like command-line interface for managing chess game databases using the CCAMC (Chess Content-Addressable Move-Chain) format.

## Overview

Positional provides efficient storage and retrieval of large chess game collections through content-addressable storage and structural deduplication. Games sharing common opening sequences automatically share storage, resulting in significant space savings.

## Features

- **Content-Addressable Storage**: Automatic deduplication of identical move sequences
- **Source Tracking**: Track where games came from with immutable source records
- **Git-like Workflow**: Familiar commands (init, import, export, list, show, stats, verify)
- **ECO-Based Deduplication**: Games sharing openings share blob chains via ECO classification
- **Efficient Storage**: ~13% compression on typical game collections
- **Exact Reconstruction**: Preserves moves, headers, comments, variations, and layout
- **Progress Tracking**: Visual progress bars for long-running operations
- **Scriptable**: `--quiet` flag for automation

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/positional.git
cd positional

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Make executable (Unix-like systems)
chmod +x positional.py
```

## Quick Start

```bash
# Initialize a new database
positional.py init my-games
cd my-games

# Import a PGN file
positional.py import ~/Downloads/games.pgn --label "My Games 2024"

# List sources
positional.py list sources

# View statistics
positional.py stats

# Export games
positional.py export "My Games 2024" > exported.pgn

# Verify integrity
positional.py verify
```

## Commands

### positional init [directory]
Create a new positional store in the specified directory (or current directory).

### positional import <pgn-file> --label <name>
Import a PGN file, creating a tracked source with the given label.

### positional export <source-label>
Export all games from a source as PGN to stdout.

### positional list sources
List all sources in the store with game counts and sizes.

### positional show <source-label>
Show detailed information about a source and its games.

### positional stats
Display storage statistics and deduplication metrics.

### positional verify
Verify the integrity of all blob chains in the store.

## Global Options

- `-C <path>` - Run as if started in `<path>` (like git -C)
- `--quiet` - Suppress progress output (useful for scripting)
- `--help` - Show help for any command

## Store Structure

When you initialize a store, Positional creates a hidden `.positional/` directory containing:

```
.positional/
  ├── moves         # Move blob pack file
  ├── idx           # Blob hash index
  ├── metadata      # Game metadata
  ├── strings       # Deduplicated string pool
  ├── registry      # Game registry
  ├── sources       # Source tracking
  └── config        # Store configuration
```

Your workspace remains clean for organizing imports, exports, and documentation.

## Documentation

- **[CLI.md](CLI.md)** - Complete CLI specification and usage guide
- **[CCAMC_SPEC.md](CCAMC_SPEC.md)** - Technical specification of the CCAMC format
- **[CCAMC_README.md](CCAMC_README.md)** - Implementation guide and design overview

## Example Workflow

```bash
# Set up a chess library
mkdir chess-library
cd chess-library
positional.py init

# Import various collections
positional.py import ~/pgn/lichess-2024.pgn --label "Lichess 2024"
positional.py import ~/pgn/masters.pgn --label "Classical Masters"
positional.py import ~/pgn/my-games.pgn --label "My Games"

# Check what we have
positional.py list sources
positional.py stats

# Export a specific collection
positional.py export "Classical Masters" > masters-export.pgn

# Verify everything is intact
positional.py verify
```

## Storage Efficiency

Example results from a 756-game Maroczy Bind collection:

- **Original PGN**: 468.8 KB
- **Positional Store**: 406.5 KB (13% compression)
  - Move blobs: 189.6 KB (3,032 unique blobs)
  - Metadata: 100.4 KB
  - Strings: 10.8 KB (517 unique strings)
  - Index: 47.4 KB
  - Registry: 58.2 KB
  - Sources: 146 B

## Requirements

- Python 3.8+
- python-chess library

## License

[Choose appropriate license]

## Contributing

Contributions welcome! The format is designed to be simple and extensible. Key design principles:

- Content-addressable whenever possible
- Fixed-size structures for predictable I/O
- Sparse storage (don't store what's not there)
- Stream-friendly (no global indices required during write)

## Technical Details

Positional uses a backward-linked Merkle DAG structure where each game is stored as a chain of 64-byte move blobs. Content-addressable hashing enables automatic deduplication - games sharing opening sequences automatically share storage.

For complete technical details, see [CCAMC_SPEC.md](CCAMC_SPEC.md).
