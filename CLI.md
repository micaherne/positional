# Positional CLI Specification

## Overview

`positional` is a command-line interface for managing CCAMC (Chess Content-Addressable Move-Chain) stores. It provides git-like workflows for importing, organizing, and exporting chess game collections.

## Design Philosophy

- **Database model**: The store is a hidden database (`.positional/`), not a working tree
- **Workspace friendly**: Users work in a clean directory, organizing imports/exports as they wish
- **Transparent operations**: Clear progress feedback for long operations
- **Scriptable**: `--quiet` flag for automation

## Store Structure

When you run `positional init`, it creates:

```
my-project/
  └── .positional/
      ├── moves         # Move blob pack file
      ├── idx           # Blob hash index
      ├── metadata      # Game metadata
      ├── strings       # Deduplicated string pool
      ├── registry      # Game registry (game_id -> hashes)
      ├── sources       # Source tracking
      └── config        # Store configuration (marker file)
```

The workspace directory is yours to organize - common patterns:
```
my-project/
  ├── .positional/      # Database (managed by positional)
  ├── imports/          # Staging area for PGN files
  ├── exports/          # Exported subsets
  └── notes.md          # Documentation
```

## Store Detection

`positional` detects the store location using this priority:

1. **Explicit path**: If `-C <path>` is specified:
   - If `<path>/.positional/` exists, use that as the store
   - Otherwise, if `<path>` contains store files directly (config marker), use `<path>`
   - Otherwise, error: "not a positional repository: <path>"
2. **Current directory**: If `.positional/` exists here, use current directory
3. **Parent search**: Walk up directory tree until `.positional/` is found
4. **Error**: If no store found, report "not in a positional repository"

This allows you to:
- Run commands from anywhere within your project tree (parent search)
- Point `-C` at either a workspace (with `.positional/` subdir) or a bare store
- Support both workspace and bare-store models flexibly

## MVP Commands

### positional init

Create a new positional store.

```bash
positional init [<directory>]
```

**Arguments:**
- `<directory>` - Directory to initialize (default: current directory)

**Behavior:**
- Creates the directory if it doesn't exist
- Creates `.positional/` subdirectory with empty store files
- Initializes the INIT_BLOB (starting position marker)
- Creates `config` marker file

**Examples:**
```bash
# Initialize in current directory
positional init

# Initialize in new directory
positional init chess-library

# Initialize and start working
positional init my-games && cd my-games
```

**Errors:**
- If `.positional/` already exists: "already a positional repository"

---

### positional import

Import a PGN file into the store, creating a tracked source.

```bash
positional import <pgn-file> --label <source-label>
```

**Arguments:**
- `<pgn-file>` - Path to PGN file to import (required)
- `--label <name>` - Human-readable source label (required)

**Options:**
- `--quiet` - Suppress progress output

**Behavior:**
- Reads PGN file sequentially
- Creates a source entry with:
  - Label (user-provided)
  - Import timestamp (ISO 8601)
  - File size in bytes
  - SHA-256 hash of source file
- Ingests each game, creating blob chains
- Links each game to the source in registry
- Shows progress: `Importing: 1,234 / 5,000 games [=====> ] 24%`
- Saves store incrementally (every 100 games for memory efficiency)

**Examples:**
```bash
# Basic import
positional import lichess_db_standard_rated_2023-01.pgn --label "Lichess Jan 2023"

# Import from elsewhere
positional -C ~/chess-db import ~/Downloads/carlsen.pgn --label "Carlsen Games"

# Quiet mode for scripts
positional import games.pgn --label "Import 2024" --quiet
```

**Output (default):**
```
Importing: lichess_db_standard_rated_2023-01.pgn
Progress: 45,123 / 45,123 games [=============] 100%
Source: 3a4f2b8c1d9e7a5f
Label: Lichess Jan 2023
Games: 45,123
Size: 125.4 MB
Completed in 2m 15s
```

**Output (quiet mode):**
```
3a4f2b8c1d9e7a5f
```

---

### positional export

Export games from a source as PGN.

```bash
positional export <source-label>
```

**Arguments:**
- `<source-label>` - Label of the source to export (required)

**Options:**
- `--quiet` - Suppress progress output (only emit PGN)

**Behavior:**
- Looks up source by label
- Retrieves all game IDs associated with that source
- Reconstructs each game (moves + metadata + annotations)
- Outputs PGN to stdout
- Shows progress to stderr: `Exporting: 1,234 / 5,000 games [=====> ] 24%`

**Examples:**
```bash
# Export to file
positional export "Lichess Jan 2023" > lichess-jan.pgn

# Export and pipe to pgn-extract
positional export "Carlsen Games" | pgn-extract -C -N -V

# Quiet mode (no progress to stderr)
positional export "Carlsen Games" --quiet > carlsen.pgn
```

**Errors:**
- If label not found: "source not found: <label>"
- If multiple sources match (future): "ambiguous source label: <label>"

---

### positional list sources

List all sources in the store.

```bash
positional list sources
```

**Behavior:**
- Displays table of all sources with:
  - Source hash (short form: 8-char hex prefix)
  - Label
  - Number of games
  - Size (formatted: KB/MB/GB)
  - Import date

**Example output:**
```
SOURCE    LABEL                          GAMES    SIZE       IMPORTED
3a4f2b8c  Lichess Jan 2023              45,123   125.4 MB   2024-01-15
7f9a3e2d  Carlsen Games                    892     2.3 MB   2024-02-01
c8b5a1f0  Maroczy Bind Collection          756     1.8 MB   2024-02-10

Total: 3 sources, 46,771 games, 129.5 MB
```

---

### positional show

Show detailed information about a source.

```bash
positional show <source-label>
```

**Arguments:**
- `<source-label>` - Label of the source to inspect

**Behavior:**
- Displays source metadata
- Lists all games from that source with one-line summaries

**Example output:**
```
Source: 7f9a3e2d1c4b8a5f
Label: Carlsen Games
Imported: 2024-02-01 14:23:45 UTC
File SHA-256: a3f2e8d9c1b4...
Games: 892
Size: 2.3 MB

Games:
  #12845: Carlsen,M - Caruana,F 1-0 (Tata Steel 2023, Round 1)
  #12846: Anand,V - Carlsen,M 0-1 (Tata Steel 2023, Round 2)
  #12847: Carlsen,M - Giri,A 1/2-1/2 (Tata Steel 2023, Round 3)
  ...
  (892 games total)
```

---

### positional stats

Display storage statistics and deduplication efficiency.

```bash
positional stats
```

**Behavior:**
- Analyzes store files
- Shows compression/deduplication metrics

**Example output:**
```
Positional Store Statistics

Storage:
  Move blobs:    191 KB (3,456 blobs)
  Metadata:      101 KB (892 games)
  Strings:        11 KB (234 unique strings)
  Index:          48 KB
  Registry:       62 KB
  Sources:         1 KB (3 sources)
  ─────────────────────────────
  Total:         414 KB

Games: 46,771
Sources: 3

Deduplication:
  Unique blob chains: 3,456
  Total game references: 46,771
  Sharing ratio: 13.5x
  
Original PGN size (est): ~5.2 MB
CCAMC size: 414 KB
Compression ratio: 92%
```

---

### positional verify

Verify integrity of the store.

```bash
positional verify
```

**Options:**
- `--quiet` - Only output errors

**Behavior:**
- Checks all blob chains for integrity
- Verifies hash consistency
- Validates parent/child relationships
- Checks for orphaned blobs
- Ensures registry integrity

**Example output (success):**
```
Verifying store integrity...
✓ Checked 3,456 move blobs
✓ Verified 46,771 blob chains
✓ Validated registry entries
✓ Confirmed zobrist hashes

Store is valid.
```

**Example output (errors found):**
```
Verifying store integrity...
✓ Checked 3,456 move blobs
✗ Found 2 broken blob chains:
  - Game #12845: Parent blob not found (hash: 9a3e...)
  - Game #18234: Hash mismatch in blob chain
✓ Validated registry entries

Errors found: 2
```

---

## Global Options

All commands support:

- `-C <path>` - Run as if positional was started in `<path>`
- `--help` - Show help for command
- `--version` - Show version information

---

## Future Enhancements

### Collections (Post-MVP)
```bash
positional collection create <name> [--filter <query>]
positional collection list
positional collection show <name>
positional collection add <name> <game-ids...>
positional export --collection <name>
```

### Filters (Post-MVP)
```bash
positional filter create <name> --query 'eco:B20-B99 result:1-0'
positional filter list
positional filter show <name>
positional export --filter <name>
```

### Diff Operations (Post-MVP)
```bash
positional diff <source1> <source2>
positional diff --collection <coll1> --collection <coll2>
```

### Batch Import (Post-MVP)
```bash
# Import multiple files as separate sources
positional import *.pgn --each-as-source

# Import multiple files as single source
positional import *.pgn --label "Combined Collection"
```

### Configuration File (Post-MVP)

`.positional/config` format:
```toml
[import]
auto_eco_dedupe = true
flush_interval = 100

[export]
include_comments = true
include_variations = true

[progress]
show_progress = true
update_interval = 100
```

Access via:
```bash
positional config set import.flush_interval 200
positional config get import.auto_eco_dedupe
positional config list
```

---

## Implementation Notes

### Progress Reporting
- Progress goes to **stderr** (allows piping stdout)
- Update every 100 games or 0.5s (whichever is less frequent)
- Use carriage return `\r` for in-place updates
- Format: `Operation: count / total [progress-bar] percentage (rate)`

### Error Handling
- Exit codes:
  - `0` - Success
  - `1` - General error
  - `2` - Invalid arguments
  - `3` - Store not found
  - `4` - Source not found
  - `5` - Verification failed
- Errors to stderr
- Brief error messages with actionable suggestions

### Label Matching
- Exact match required for MVP
- Case-sensitive
- Future: fuzzy matching, hash prefixes

---

## Example Workflows

### Building a Chess Library
```bash
# Set up
mkdir chess-library
cd chess-library
positional init

# Import collections
positional import ~/Downloads/lichess-2023.pgn --label "Lichess 2023"
positional import ~/Downloads/masters.pgn --label "Classical Masters"

# Check what we have
positional list sources
positional stats

# Export a subset
positional export "Classical Masters" > masters-export.pgn
```

### Working with Multiple Stores
```bash
# Openings database
positional init openings-db
positional -C openings-db import eco.pgn --label "ECO Lines"

# Tactics database  
positional init tactics-db
positional -C tactics-db import puzzles.pgn --label "Lichess Puzzles"

# Use them independently
positional -C openings-db stats
positional -C tactics-db export "Lichess Puzzles" > puzzles.pgn
```

### Scripting
```bash
#!/bin/bash
# Batch process PGN files

for pgn in imports/*.pgn; do
    name=$(basename "$pgn" .pgn)
    hash=$(positional import "$pgn" --label "$name" --quiet)
    echo "Imported $name as $hash"
done

# Verify integrity
positional verify --quiet || echo "Verification failed!"
```
