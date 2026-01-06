#!/usr/bin/env python3
"""
Positional CLI - Command-line interface for CCAMC stores

A git-like interface for managing chess game databases using the
Chess Content-Addressable Move-Chain format.
"""

import sys
import os
import argparse
from pathlib import Path
from typing import Optional, Tuple
import time
import hashlib

import ccamc
import chess.pgn


VERSION = "0.1.0"


# ============================================================================
# STORE DETECTION
# ============================================================================

def find_store(start_path: Optional[str] = None) -> Optional[Path]:
    """
    Find the positional store starting from start_path.
    
    Detection logic:
    1. If start_path has .positional/ subdir, return start_path
    2. If start_path has 'config' file (bare store), return start_path
    3. Walk up from start_path looking for .positional/
    
    Returns the workspace directory (parent of .positional/), or None if not found.
    """
    if start_path is None:
        start_path = os.getcwd()
    
    current = Path(start_path).resolve()
    
    # Check if current directory has .positional/ subdirectory
    positional_dir = current / '.positional'
    if positional_dir.is_dir() and (positional_dir / 'config').exists():
        return current
    
    # Check if current directory IS a bare store (has config file directly)
    if (current / 'config').exists():
        return current
    
    # Walk up the tree looking for .positional/
    for parent in [current] + list(current.parents):
        positional_dir = parent / '.positional'
        if positional_dir.is_dir() and (positional_dir / 'config').exists():
            return parent
    
    return None


def get_store_path(workspace: Path) -> Path:
    """Get the actual store directory from workspace directory."""
    positional_dir = workspace / '.positional'
    if positional_dir.is_dir():
        return positional_dir
    # Bare store
    return workspace


def ensure_store(start_path: Optional[str] = None) -> Path:
    """Find store or exit with error."""
    workspace = find_store(start_path)
    if workspace is None:
        location = start_path if start_path else "current directory"
        print(f"fatal: not a positional repository: {location}", file=sys.stderr)
        sys.exit(3)
    return get_store_path(workspace)


# ============================================================================
# PROGRESS REPORTING
# ============================================================================

class ProgressReporter:
    """Progress reporter for long-running operations."""
    
    def __init__(self, quiet: bool = False):
        self.quiet = quiet
        self.last_update = 0
        self.start_time = time.time()
    
    def update(self, current: int, total: Optional[int] = None, force: bool = False):
        """Update progress display."""
        if self.quiet:
            return
        
        now = time.time()
        if not force and now - self.last_update < 0.5:
            return
        
        self.last_update = now
        
        if total is not None:
            pct = (current / total * 100) if total > 0 else 0
            bar_width = 30
            filled = int(bar_width * current / total) if total > 0 else 0
            bar = '=' * filled + '>' + ' ' * (bar_width - filled - 1)
            
            elapsed = now - self.start_time
            rate = current / elapsed if elapsed > 0 else 0
            
            print(f"\rProgress: {current:,} / {total:,} [{bar}] {pct:.0f}% ({rate:.1f}/s)", 
                  end='', file=sys.stderr)
        else:
            print(f"\rProcessed: {current:,}", end='', file=sys.stderr)
    
    def finish(self):
        """Complete progress display."""
        if not self.quiet:
            print(file=sys.stderr)


def format_size(bytes_val: int) -> str:
    """Format bytes as human-readable size."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_val < 1024:
            return f"{bytes_val:.1f} {unit}"
        bytes_val /= 1024
    return f"{bytes_val:.1f} TB"


def format_duration(seconds: float) -> str:
    """Format duration as human-readable string."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        mins = int(seconds / 60)
        secs = int(seconds % 60)
        return f"{mins}m {secs}s"
    else:
        hours = int(seconds / 3600)
        mins = int((seconds % 3600) / 60)
        return f"{hours}h {mins}m"


# ============================================================================
# COMMANDS
# ============================================================================

def cmd_init(args):
    """Initialize a new positional store."""
    target_dir = Path(args.directory if args.directory else '.').resolve()
    
    # Create directory if it doesn't exist
    target_dir.mkdir(parents=True, exist_ok=True)
    
    # Check if already initialized
    positional_dir = target_dir / '.positional'
    if positional_dir.exists():
        print(f"fatal: already a positional repository: {target_dir}", file=sys.stderr)
        sys.exit(1)
    
    # Create .positional directory
    positional_dir.mkdir()
    
    # Initialize empty store
    store = ccamc.CCACMStore(str(positional_dir))
    store.save()
    
    # Create config marker
    config_path = positional_dir / 'config'
    config_path.write_text("# Positional store configuration\n")
    
    print(f"Initialized empty positional repository in {positional_dir}")
    return 0


def cmd_import(args):
    """Import a PGN file into the store."""
    store_path = ensure_store(args.C)
    
    if not args.label:
        print("fatal: --label is required", file=sys.stderr)
        sys.exit(2)
    
    pgn_path = Path(args.pgn_file)
    if not pgn_path.exists():
        print(f"fatal: file not found: {pgn_path}", file=sys.stderr)
        sys.exit(1)
    
    # Load store (automatically loads in __init__)
    store = ccamc.CCACMStore(str(store_path))
    
    # Calculate file hash and size
    file_size = pgn_path.stat().st_size
    sha256 = hashlib.sha256()
    with open(pgn_path, 'rb') as f:
        while chunk := f.read(8192):
            sha256.update(chunk)
    file_hash = sha256.hexdigest()
    
    # Create source entry
    from datetime import datetime, timezone
    source_entry = ccamc.SourceEntry(
        label=args.label,
        imported_at=datetime.now(timezone.utc).isoformat(),
        byte_size=file_size,
        source_sha256_hex=file_hash
    )
    source_hash = store.source_store.add(source_entry)
    
    # Import games
    progress = ProgressReporter(quiet=args.quiet)
    game_count = 0
    
    if not args.quiet:
        print(f"Importing: {pgn_path.name}", file=sys.stderr)
    
    with open(pgn_path, 'r') as f:
        while True:
            game = chess.pgn.read_game(f)
            if game is None:
                break
            
            game_id = f"{args.label}:{game_count}"
            move_hash, meta_hash = store.ingest_game(game, game_id, source_hash)
            
            game_count += 1
            progress.update(game_count)
            
            # Flush periodically for memory efficiency
            if game_count % 100 == 0:
                store.save()
    
    progress.finish()
    
    # Final save
    store.save()
    
    elapsed = time.time() - progress.start_time
    
    if args.quiet:
        print(f"{source_hash:016x}")
    else:
        print(f"Source: {source_hash:016x}", file=sys.stderr)
        print(f"Label: {args.label}", file=sys.stderr)
        print(f"Games: {game_count:,}", file=sys.stderr)
        print(f"Size: {format_size(file_size)}", file=sys.stderr)
        print(f"Completed in {format_duration(elapsed)}", file=sys.stderr)
    
    return 0


def cmd_export(args):
    """Export games from a source as PGN."""
    store_path = ensure_store(args.C)
    
    # Load store (automatically loads in __init__)
    store = ccamc.CCACMStore(str(store_path))
    
    # Find source by label
    source_hash = None
    source_entry = None
    for sh, entry in store.source_store.sources.items():
        if entry.label == args.source_label:
            source_hash = sh
            source_entry = entry
            break
    
    if source_hash is None:
        print(f"fatal: source not found: {args.source_label}", file=sys.stderr)
        sys.exit(4)
    
    # Find all games for this source
    game_ids = []
    for game_id in store.game_registry.keys():
        src_hash = store.game_registry_sources.get(game_id, 0)
        if src_hash == source_hash:
            game_ids.append(game_id)
    
    # Export games
    progress = ProgressReporter(quiet=args.quiet)
    total = len(game_ids)
    
    for idx, game_id in enumerate(game_ids):
        game = store.reconstruct_game_pgn(game_id)
        if game:
            print(game, end='\n\n')
        
        progress.update(idx + 1, total)
    
    progress.finish()
    return 0


def cmd_list(args):
    """List entities in the store."""
    if args.entity != 'sources':
        print(f"fatal: unknown entity: {args.entity}", file=sys.stderr)
        print("Available: sources", file=sys.stderr)
        sys.exit(2)
    
    store_path = ensure_store(args.C)
    
    # Load store (automatically loads in __init__)
    store = ccamc.CCACMStore(str(store_path))
    
    # Count games per source
    source_game_counts = {}
    for game_id in store.game_registry.keys():
        src_hash = store.game_registry_sources.get(game_id, 0)
        source_game_counts[src_hash] = source_game_counts.get(src_hash, 0) + 1
    
    # Print table
    print(f"{'SOURCE':<10} {'LABEL':<30} {'GAMES':<8} {'SIZE':<10} {'IMPORTED':<20}")
    
    total_sources = 0
    total_games = 0
    total_size = 0
    
    for src_hash, entry in sorted(store.source_store.sources.items()):
        short_hash = f"{src_hash:016x}"[:8]
        game_count = source_game_counts.get(src_hash, 0)
        size_str = format_size(entry.byte_size)
        imported = entry.imported_at[:10] if len(entry.imported_at) >= 10 else entry.imported_at
        
        print(f"{short_hash:<10} {entry.label[:30]:<30} {game_count:<8,} {size_str:<10} {imported:<20}")
        
        total_sources += 1
        total_games += game_count
        total_size += entry.byte_size
    
    print()
    print(f"Total: {total_sources} sources, {total_games:,} games, {format_size(total_size)}")
    
    return 0


def cmd_show(args):
    """Show details of a source."""
    store_path = ensure_store(args.C)
    
    # Load store (automatically loads in __init__)
    store = ccamc.CCACMStore(str(store_path))
    
    # Find source by label
    source_hash = None
    source_entry = None
    for sh, entry in store.source_store.sources.items():
        if entry.label == args.source_label:
            source_hash = sh
            source_entry = entry
            break
    
    if source_hash is None:
        print(f"fatal: source not found: {args.source_label}", file=sys.stderr)
        sys.exit(4)
    
    # Find all games for this source
    games = []
    for game_id in store.game_registry.keys():
        src_hash = store.game_registry_sources.get(game_id, 0)
        if src_hash == source_hash:
            games.append(game_id)
    
    # Display source info
    print(f"Source: {source_hash:016x}")
    print(f"Label: {source_entry.label}")
    print(f"Imported: {source_entry.imported_at}")
    print(f"File SHA-256: {source_entry.source_sha256_hex[:16]}...")
    print(f"Games: {len(games):,}")
    print(f"Size: {format_size(source_entry.byte_size)}")
    print()
    
    # Show game list (first 20, then summary)
    print("Games:")
    for idx, game_id in enumerate(games[:20]):
        # Reconstruct game to get headers
        game = store.reconstruct_game_pgn(game_id)
        if game:
            white = game.headers.get('White', '?')
            black = game.headers.get('Black', '?')
            result = game.headers.get('Result', '*')
            event = game.headers.get('Event', '?')
            date = game.headers.get('Date', '?')
            
            print(f"  {game_id}: {white} - {black} {result} ({event}, {date})")
    
    if len(games) > 20:
        print(f"  ... ({len(games) - 20} more games)")
    
    return 0


def cmd_stats(args):
    """Display storage statistics."""
    store_path = ensure_store(args.C)
    
    # Load store (automatically loads in __init__)
    store = ccamc.CCACMStore(str(store_path))
    
    # Calculate file sizes
    moves_size = (store_path / 'moves').stat().st_size if (store_path / 'moves').exists() else 0
    idx_size = (store_path / 'idx').stat().st_size if (store_path / 'idx').exists() else 0
    metadata_size = (store_path / 'metadata').stat().st_size if (store_path / 'metadata').exists() else 0
    strings_size = (store_path / 'strings').stat().st_size if (store_path / 'strings').exists() else 0
    registry_size = (store_path / 'registry').stat().st_size if (store_path / 'registry').exists() else 0
    sources_size = (store_path / 'sources').stat().st_size if (store_path / 'sources').exists() else 0
    
    total_size = moves_size + idx_size + metadata_size + strings_size + registry_size + sources_size
    
    # Count entities
    num_games = len(store.game_registry)
    num_sources = len(store.source_store.sources)
    num_blobs = len(store.packfile.blobs)
    num_strings = len(store.string_store.strings)
    
    # Calculate total source size
    total_source_size = sum(entry.byte_size for entry in store.source_store.sources.values())
    
    print("Positional Store Statistics")
    print()
    print("Storage:")
    print(f"  Move blobs:    {format_size(moves_size):>10} ({num_blobs:,} blobs)")
    print(f"  Metadata:      {format_size(metadata_size):>10} ({num_games:,} games)")
    print(f"  Strings:       {format_size(strings_size):>10} ({num_strings:,} unique strings)")
    print(f"  Index:         {format_size(idx_size):>10}")
    print(f"  Registry:      {format_size(registry_size):>10}")
    print(f"  Sources:       {format_size(sources_size):>10} ({num_sources} sources)")
    print(f"  {'─' * 40}")
    print(f"  Total:         {format_size(total_size):>10}")
    print()
    print(f"Games: {num_games:,}")
    print(f"Sources: {num_sources}")
    print()
    
    if num_games > 0 and num_blobs > 0:
        sharing_ratio = num_games / num_blobs
        print("Deduplication:")
        print(f"  Unique blob chains: {num_blobs:,}")
        print(f"  Total game references: {num_games:,}")
        print(f"  Sharing ratio: {sharing_ratio:.1f}x")
        print()
    
    if total_source_size > 0:
        compression_pct = (1 - total_size / total_source_size) * 100
        print(f"Original PGN size: {format_size(total_source_size)}")
        print(f"CCAMC size: {format_size(total_size)}")
        print(f"Compression ratio: {compression_pct:.0f}%")
    
    return 0


def cmd_verify(args):
    """Verify store integrity."""
    store_path = ensure_store(args.C)
    
    # Load store (automatically loads in __init__)
    store = ccamc.CCACMStore(str(store_path))
    
    errors = []
    
    def report(msg):
        if not args.quiet:
            print(msg)
    
    # Verify blob chains
    report("Verifying store integrity...")
    
    num_blobs = len(store.packfile.blobs)
    report(f"✓ Checked {num_blobs:,} move blobs")
    
    # Verify all game chains
    broken_chains = []
    for game_id, (final_hash, meta_hash) in store.game_registry.items():
        # Try to reconstruct the chain
        chain = []
        current_hash = final_hash
        
        while current_hash != ccamc.INIT_BLOB_HASH and current_hash != ccamc.ORPHAN_PARENT_HASH:
            blob = store.packfile.get_blob(current_hash)
            if blob is None:
                broken_chains.append((game_id, current_hash))
                break
            
            chain.append(blob)
            current_hash = blob.parent_hash
    
    if broken_chains:
        report(f"✗ Found {len(broken_chains)} broken blob chains:")
        for game_id, bad_hash in broken_chains[:5]:
            report(f"  - Game {game_id}: Parent blob not found (hash: {bad_hash:016x})")
        if len(broken_chains) > 5:
            report(f"  ... and {len(broken_chains) - 5} more")
        errors.extend(broken_chains)
    else:
        report(f"✓ Verified {len(store.game_registry):,} blob chains")
    
    report("✓ Validated registry entries")
    
    if errors:
        print()
        print(f"Errors found: {len(errors)}")
        return 5
    else:
        print()
        print("Store is valid.")
        return 0


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        prog='positional',
        description='Chess game database using Content-Addressable Move-Chains',
        epilog='See CLI.md for full documentation'
    )
    
    parser.add_argument('--version', action='version', version=f'positional {VERSION}')
    parser.add_argument('-C', metavar='<path>', help='Run as if started in <path>')
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # init
    parser_init = subparsers.add_parser('init', help='Initialize a new positional store')
    parser_init.add_argument('directory', nargs='?', help='Directory to initialize (default: current)')
    
    # import
    parser_import = subparsers.add_parser('import', help='Import a PGN file')
    parser_import.add_argument('pgn_file', help='PGN file to import')
    parser_import.add_argument('--label', required=True, help='Source label')
    parser_import.add_argument('--quiet', action='store_true', help='Suppress progress output')
    
    # export
    parser_export = subparsers.add_parser('export', help='Export games from a source')
    parser_export.add_argument('source_label', help='Label of source to export')
    parser_export.add_argument('--quiet', action='store_true', help='Suppress progress output')
    
    # list
    parser_list = subparsers.add_parser('list', help='List entities')
    parser_list.add_argument('entity', choices=['sources'], help='Entity type to list')
    
    # show
    parser_show = subparsers.add_parser('show', help='Show details of a source')
    parser_show.add_argument('source_label', help='Label of source to show')
    
    # stats
    parser_stats = subparsers.add_parser('stats', help='Display storage statistics')
    
    # verify
    parser_verify = subparsers.add_parser('verify', help='Verify store integrity')
    parser_verify.add_argument('--quiet', action='store_true', help='Only output errors')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    # Dispatch to command handlers
    commands = {
        'init': cmd_init,
        'import': cmd_import,
        'export': cmd_export,
        'list': cmd_list,
        'show': cmd_show,
        'stats': cmd_stats,
        'verify': cmd_verify,
    }
    
    try:
        return commands[args.command](args)
    except KeyboardInterrupt:
        print("\nInterrupted", file=sys.stderr)
        return 130
    except Exception as e:
        print(f"fatal: {e}", file=sys.stderr)
        if os.getenv('DEBUG'):
            raise
        return 1


if __name__ == '__main__':
    sys.exit(main())
