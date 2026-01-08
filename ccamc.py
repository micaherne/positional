#!/usr/bin/env python3
"""
Chess Content-Addressable Move-Chain (CCAMC) v1.0 Implementation

A high-density, backward-linked Merkle DAG for storing chess games with
bit-perfect move-order reconstruction and structural deduplication.
"""

import struct
import os
import hashlib
import io
import csv
import pickle
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional, List, Tuple, Dict, Set
from pathlib import Path
import chess
import chess.pgn
import chess.polyglot


# ============================================================================
# CONSTANTS
# ============================================================================

# STR tag roster (PGN seven tag roster)
STR_TAG_NAMES = ['Event', 'Site', 'Date', 'Round', 'White', 'Black', 'Result']

# Result string to code mapping
RESULT_MAP = {'1-0': 0, '0-1': 1, '1/2-1/2': 2, '*': 3}

# Promotion piece mappings
PROMOTION_TO_CODE = {
    None: 0,
    chess.QUEEN: 1,
    chess.ROOK: 2,
    chess.BISHOP: 3,
    chess.KNIGHT: 4,
}

CODE_TO_PROMOTION = {
    0: None,
    1: chess.QUEEN,
    2: chess.ROOK,
    3: chess.BISHOP,
    4: chess.KNIGHT,
}

def _compute_orphan_parent_hash() -> int:
    """Compute the well-known hash for orphan variation parent marker."""
    marker = b"ORPHAN_VARIATION_PARENT_MARKER"
    h = hashlib.blake2b(marker, digest_size=8).digest()
    return struct.unpack('<Q', h)[0]

ORPHAN_PARENT_HASH = _compute_orphan_parent_hash()

# INIT_BLOB_HASH computed after ZobristHasher is defined
INIT_BLOB_HASH = None  # Will be set after zobrist definition


# =========================================================================
# PART 1B: SOURCE DESCRIPTOR
# =========================================================================

@dataclass
class SourceEntry:
    label: str
    imported_at: str  # ISO timestamp
    byte_size: int
    source_sha256_hex: str

    def to_blob(self) -> bytes:
        # Stable serialization for hashing
        parts = [
            self.label,
            self.imported_at,
            str(self.byte_size),
            self.source_sha256_hex,
        ]
        return "\n".join(parts).encode('utf-8')

    def hash(self) -> int:
        h = hashlib.blake2b(self.to_blob(), digest_size=8).digest()
        return struct.unpack('<Q', h)[0]


class SourceStore:
    """Content-addressable source index (hash -> SourceEntry)."""

    def __init__(self, path: str):
        self.path = Path(path)
        self.sources: Dict[int, SourceEntry] = {}

    def add(self, entry: SourceEntry) -> int:
        h = entry.hash()
        if h not in self.sources:
            self.sources[h] = entry
        return h

    def get(self, source_hash: int) -> Optional[SourceEntry]:
        return self.sources.get(source_hash)

    def save(self):
        with open(self.path, 'w', encoding='utf-8') as f:
            for h, entry in self.sources.items():
                f.write(f"{h:016x}\t{entry.label}\t{entry.imported_at}\t{entry.byte_size}\t{entry.source_sha256_hex}\n")

    def load(self):
        if not self.path.exists():
            return
        self.sources = {}
        with open(self.path, 'r', encoding='utf-8') as f:
            for line in f:
                parts = line.rstrip('\n').split('\t')
                if len(parts) != 5:
                    continue
                h_str, label, imported_at, size_str, sha256_hex = parts
                try:
                    h_val = int(h_str, 16)
                    entry = SourceEntry(label=label, imported_at=imported_at, byte_size=int(size_str), source_sha256_hex=sha256_hex)
                    self.sources[h_val] = entry
                except Exception:
                    continue


# ============================================================================
# PART 1: PACKED UCI MOVE (16-BIT)
# ============================================================================

def encode_move_packed(move: chess.Move) -> int:
    """
    Encode a chess move as a 16-bit packed UCI move.
    
    Bits 0-5:   FromSq (0-63)
    Bits 6-11:  ToSq (0-63)
    Bits 12-14: Promotion (0=None, 1=Q, 2=R, 3=B, 4=N)
    Bit 15:     Reserved (always 0)
    """
    from_sq = move.from_square
    to_sq = move.to_square
    promo = PROMOTION_TO_CODE.get(move.promotion, 0)
    
    packed = (from_sq & 0x3F) | ((to_sq & 0x3F) << 6) | ((promo & 0x07) << 12)
    return packed & 0xFFFF


def decode_move_packed(packed: int, board: chess.Board) -> Optional[chess.Move]:
    """Decode a 16-bit packed move back to a chess.Move."""
    from_sq = packed & 0x3F
    to_sq = (packed >> 6) & 0x3F
    promo_code = (packed >> 12) & 0x07
    promo = CODE_TO_PROMOTION.get(promo_code)
    
    try:
        move = chess.Move(from_sq, to_sq, promotion=promo)
        if move in board.legal_moves:
            return move
    except:
        pass
    
    return None


def _hash_file_sha256(path: str) -> Tuple[int, str]:
    """Return (size, sha256 hex) of a file."""
    h = hashlib.sha256()
    size = 0
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            size += len(chunk)
            h.update(chunk)
    return size, h.hexdigest()


# ============================================================================
# PART 2: ZOBRIST HASHING
# ============================================================================
# Using the Polyglot Zobrist hash from chess.polyglot (industry standard)

def _compute_init_blob_hash() -> int:
    """Create and hash the initial position blob (starting position, no moves)."""
    # Create the initial blob: parent=0, no moves, result=*
    blob = bytearray(64)
    struct.pack_into('<Q', blob, 0, 0)           # parent_hash = 0
    struct.pack_into('<H', blob, 0x3E, 3)        # result = 3 (*=in progress)
    
    # Hash the blob
    h = hashlib.blake2b(bytes(blob), digest_size=8).digest()
    return struct.unpack('<Q', h)[0]

# Update the global constant
INIT_BLOB_HASH = _compute_init_blob_hash()


# ============================================================================
# PART 3: MOVE-BLOB STRUCTURE (64 BYTES)
# ============================================================================

@dataclass
class MoveBlob:
    """A 64-byte block containing up to 27 chess moves."""
    
    parent_hash: int          # 8B: INIT_BLOB_HASH, ORPHAN_PARENT_HASH, or hash of previous blob
    moves: List[int]          # Up to 27 packed moves (2B each)
    result: int               # 2B: 0=1-0, 1=0-1, 2=1/2, 3=*
    
    def serialize(self) -> bytes:
        """Serialize blob to 64-byte format."""
        data = bytearray(64)
        
        # ParentHash (8B)
        struct.pack_into('<Q', data, 0x00, self.parent_hash)
        
        # MoveData (54B = 27 moves × 2B)
        for i, move_packed in enumerate(self.moves[:27]):
            struct.pack_into('<H', data, 0x08 + i * 2, move_packed & 0xFFFF)
        
        # Result (2B)
        struct.pack_into('<H', data, 0x3E, self.result & 0xFFFF)
        
        return bytes(data)
    
    @staticmethod
    def deserialize(data: bytes) -> 'MoveBlob':
        """Deserialize 64-byte blob."""
        if len(data) != 64:
            raise ValueError("Blob must be exactly 64 bytes")
        
        parent_hash = struct.unpack_from('<Q', data, 0x00)[0]
        
        # Read all 27 move slots and stop at first 0x0000 (invalid move)
        moves = []
        for i in range(27):
            move_packed = struct.unpack_from('<H', data, 0x08 + i * 2)[0]
            if move_packed == 0:  # 0x0000 = a1->a1 (invalid)
                break
            moves.append(move_packed)
        
        result = struct.unpack_from('<H', data, 0x3E)[0]
        
        return MoveBlob(parent_hash, moves, result)
    
    def compute_hash(self) -> int:
        """Compute XXHash64 of this blob."""
        # Simple 64-bit hash for content addressing
        h = hashlib.blake2b(self.serialize(), digest_size=8).digest()
        return struct.unpack('<Q', h)[0]


# ============================================================================
# PART 4: FILE ARCHITECTURE
# ============================================================================

class PackFile:
    """The .pack file: Header + concatenated 64-byte Move-Blobs."""
    
    MAGIC = b'CHSS'
    VERSION = 1
    
    def __init__(self, path: str):
        self.path = Path(path)
        self.blobs: Dict[int, MoveBlob] = {}  # hash -> blob
        self.blob_order: List[int] = []        # order blobs were added
        # Index for finding existing blobs by parent and moves
        self.blob_index: Dict[Tuple[int, Tuple[int, ...]], int] = {}  # (parent_hash, moves_tuple) -> blob_hash
        self.loaded = False
    
    def add_blob(self, blob: MoveBlob) -> int:
        """Add a blob and return its hash."""
        blob_hash = blob.compute_hash()
        if blob_hash not in self.blobs:
            self.blobs[blob_hash] = blob
            self.blob_order.append(blob_hash)
            # Index by parent and moves for deduplication
            moves_key = (blob.parent_hash, tuple(blob.moves))
            self.blob_index[moves_key] = blob_hash
        return blob_hash
    
    def find_blob_by_moves(self, parent_hash: int, moves: List[int]) -> Optional[int]:
        """Find existing blob with same parent and moves."""
        moves_key = (parent_hash, tuple(moves))
        return self.blob_index.get(moves_key)
    
    def get_blob(self, blob_hash: int) -> Optional[MoveBlob]:
        """Retrieve a blob by hash."""
        return self.blobs.get(blob_hash)
    
    def save(self):
        """Write blobs to .pack file."""
        with open(self.path, 'wb') as f:
            # Header (16B)
            header = bytearray(16)
            header[0:4] = self.MAGIC
            struct.pack_into('<H', header, 4, self.VERSION)
            struct.pack_into('<Q', header, 6, len(self.blob_order))
            f.write(bytes(header))
            
            # Body: all blobs in order
            for blob_hash in self.blob_order:
                f.write(self.blobs[blob_hash].serialize())
    
    def load(self):
        """Read blobs from .pack file."""
        if not self.path.exists():
            return
        
        with open(self.path, 'rb') as f:
            # Read header
            header = f.read(16)
            if header[0:4] != self.MAGIC:
                raise ValueError("Invalid pack file magic")
            
            version = struct.unpack_from('<H', header, 4)[0]
            blob_count = struct.unpack_from('<Q', header, 6)[0]
            
            # Read blobs
            for _ in range(blob_count):
                blob_data = f.read(64)
                if len(blob_data) != 64:
                    break
                blob = MoveBlob.deserialize(blob_data)
                blob_hash = blob.compute_hash()
                self.blobs[blob_hash] = blob
                self.blob_order.append(blob_hash)
                # Rebuild index
                moves_key = (blob.parent_hash, tuple(blob.moves))
                self.blob_index[moves_key] = blob_hash
        
        self.loaded = True


class IndexFile:
    """The .idx file: sorted (BlobHash, ByteOffset) entries."""
    
    def __init__(self, path: str):
        self.path = Path(path)
        self.index: List[Tuple[int, int]] = []  # (hash, offset)
    
    def build_from_packfile(self, packfile: PackFile):
        """Build index from pack file blobs."""
        self.index = []
        offset = 16  # After header
        for blob_hash in packfile.blob_order:
            self.index.append((blob_hash, offset))
            offset += 64
        self.index.sort()  # Sort by hash for binary search
    
    def save(self):
        """Write index to file."""
        with open(self.path, 'wb') as f:
            for blob_hash, offset in self.index:
                f.write(struct.pack('<QQ', blob_hash, offset))
    
    def load(self):
        """Read index from file."""
        if not self.path.exists():
            return
        
        self.index = []
        with open(self.path, 'rb') as f:
            while True:
                entry = f.read(16)
                if len(entry) != 16:
                    break
                blob_hash, offset = struct.unpack('<QQ', entry)
                self.index.append((blob_hash, offset))


# ============================================================================
# PART 4.5: STRING STORE
# ============================================================================

class StringStore:
    """Content-addressable string storage with deduplication."""
    
    def __init__(self, path: str):
        self.path = Path(path)
        self.strings: Dict[int, bytes] = {}  # hash -> UTF-8 bytes
    
    def add_string(self, text: str) -> int:
        """Add a string and return its hash."""
        utf8_bytes = text.encode('utf-8')
        string_hash = hashlib.blake2b(utf8_bytes, digest_size=8).digest()
        string_hash = struct.unpack('<Q', string_hash)[0]
        
        if string_hash not in self.strings:
            self.strings[string_hash] = utf8_bytes
        
        return string_hash
    
    def get_string(self, string_hash: int) -> Optional[str]:
        """Retrieve a string by hash."""
        utf8_bytes = self.strings.get(string_hash)
        if utf8_bytes is not None:
            return utf8_bytes.decode('utf-8')
        return None
    
    def save(self):
        """Write strings to disk."""
        with open(self.path, 'wb') as f:
            # Write count
            f.write(struct.pack('<Q', len(self.strings)))
            
            # Write each string: [Hash:8B][Length:4B][UTF-8 Data:Length]
            for string_hash, utf8_bytes in self.strings.items():
                f.write(struct.pack('<Q', string_hash))
                f.write(struct.pack('<I', len(utf8_bytes)))
                f.write(utf8_bytes)
    
    def load(self):
        """Read strings from disk."""
        if not self.path.exists():
            return
        
        with open(self.path, 'rb') as f:
            # Read count
            count_bytes = f.read(8)
            if len(count_bytes) != 8:
                return
            count = struct.unpack('<Q', count_bytes)[0]
            
            # Read each string
            for _ in range(count):
                hash_bytes = f.read(8)
                len_bytes = f.read(4)
                if len(hash_bytes) != 8 or len(len_bytes) != 4:
                    break
                
                string_hash = struct.unpack('<Q', hash_bytes)[0]
                length = struct.unpack('<I', len_bytes)[0]
                utf8_bytes = f.read(length)
                
                if len(utf8_bytes) != length:
                    break
                
                self.strings[string_hash] = utf8_bytes


# ============================================================================
# PART 4.6: METADATA STRUCTURES (SPARSE RECORD FORMAT)
# ============================================================================

# Record types for sparse annotation storage
class RecordType:
    COMMENT = 0
    NAG = 1
    VARIATION = 2
    NEWLINE = 3

@dataclass
class AnnotationRecord:
    """A single sparse annotation record for a specific move."""
    move_index: int      # Index into mainline move chain
    record_type: int     # RecordType value
    
    # Comment-specific (type=COMMENT)
    text_hash: Optional[int] = None
    is_pre: bool = False              # False=post, True=pre
    is_semicolon: bool = False        # False=brace, True=semicolon
    comment_newline: bool = False     # Newline after comment
    
    # NAG-specific (type=NAG)
    nag_code: Optional[int] = None
    
    # Variation-specific (type=VARIATION)
    variation_move_hash: Optional[int] = None
    variation_meta_hash: Optional[int] = None
    
    # NEWLINE has no additional fields
    
    def serialize(self) -> bytes:
        """Serialize record to bytes."""
        buf = io.BytesIO()
        
        # Write move index as varint
        self._write_varint(buf, self.move_index)
        
        # Build record type byte with flags
        type_byte = self.record_type & 0x07
        if self.record_type == RecordType.COMMENT:
            if self.is_pre:
                type_byte |= 0x08
            if self.is_semicolon:
                type_byte |= 0x10
            if self.comment_newline:
                type_byte |= 0x20
        elif self.record_type == RecordType.NEWLINE:
            # Bit 5 used for consistency
            type_byte |= 0x20
        
        buf.write(struct.pack('<B', type_byte))
        
        # Write type-specific payload
        if self.record_type == RecordType.COMMENT:
            buf.write(struct.pack('<Q', self.text_hash))
        elif self.record_type == RecordType.NAG:
            buf.write(struct.pack('<B', self.nag_code))
        elif self.record_type == RecordType.VARIATION:
            buf.write(struct.pack('<Q', self.variation_move_hash))
            buf.write(struct.pack('<Q', self.variation_meta_hash))
        
        return buf.getvalue()
    
    @staticmethod
    def deserialize(buf: io.BytesIO) -> 'AnnotationRecord':
        """Deserialize record from bytes."""
        # Read move index varint
        move_index = AnnotationRecord._read_varint(buf)
        
        # Read type byte
        type_byte = struct.unpack('<B', buf.read(1))[0]
        record_type = type_byte & 0x07
        
        rec = AnnotationRecord(move_index=move_index, record_type=record_type)
        
        if record_type == RecordType.COMMENT:
            rec.is_pre = bool(type_byte & 0x08)
            rec.is_semicolon = bool(type_byte & 0x10)
            rec.comment_newline = bool(type_byte & 0x20)
            rec.text_hash = struct.unpack('<Q', buf.read(8))[0]
        elif record_type == RecordType.NAG:
            rec.nag_code = struct.unpack('<B', buf.read(1))[0]
        elif record_type == RecordType.VARIATION:
            rec.variation_move_hash = struct.unpack('<Q', buf.read(8))[0]
            rec.variation_meta_hash = struct.unpack('<Q', buf.read(8))[0]
        
        return rec
    
    @staticmethod
    def _write_varint(buf: io.BytesIO, value: int):
        """Write unsigned varint."""
        while value >= 128:
            buf.write(struct.pack('<B', (value & 0x7F) | 0x80))
            value >>= 7
        buf.write(struct.pack('<B', value & 0x7F))
    
    @staticmethod
    def _read_varint(buf: io.BytesIO) -> int:
        """Read unsigned varint."""
        result = 0
        shift = 0
        while True:
            byte = struct.unpack('<B', buf.read(1))[0]
            result |= (byte & 0x7F) << shift
            if not (byte & 0x80):
                break
            shift += 7
        return result


@dataclass
class GameMetadata:
    """Complete metadata for a game (sparse record format)."""
    final_move_hash: int                    # Binds to move chain
    str_tags: Dict[int, int]                 # STR tag_id (0-6) -> string_hash
    extra_tags: Dict[int, int]               # Hash(tag_name) -> Hash(tag_value)
    annotation_records: List[AnnotationRecord]  # Sparse annotation records
    
    def serialize(self) -> bytes:
        """Serialize metadata blob to bytes."""
        buf = io.BytesIO()
        
        # Final move hash binding
        buf.write(struct.pack('<Q', self.final_move_hash))
        
        # STR tags
        buf.write(struct.pack('<B', len(self.str_tags)))
        for tag_id in sorted(self.str_tags.keys()):
            buf.write(struct.pack('<BQ', tag_id, self.str_tags[tag_id]))
        
        # Extra tags
        buf.write(struct.pack('<H', len(self.extra_tags)))
        for name_hash in sorted(self.extra_tags.keys()):
            buf.write(struct.pack('<QQ', name_hash, self.extra_tags[name_hash]))
        
        # Annotation records
        buf.write(struct.pack('<H', len(self.annotation_records)))
        for rec in self.annotation_records:
            buf.write(rec.serialize())
        
        return buf.getvalue()
    
    @staticmethod
    def deserialize(data: bytes) -> 'GameMetadata':
        """Deserialize metadata blob."""
        buf = io.BytesIO(data)
        
        # Final move hash
        final_move_hash = struct.unpack('<Q', buf.read(8))[0]
        
        # STR tags
        str_count = struct.unpack('<B', buf.read(1))[0]
        str_tags = {}
        for _ in range(str_count):
            tag_id, value_hash = struct.unpack('<BQ', buf.read(9))
            str_tags[tag_id] = value_hash
        
        # Extra tags
        extra_count = struct.unpack('<H', buf.read(2))[0]
        extra_tags = {}
        for _ in range(extra_count):
            name_hash, value_hash = struct.unpack('<QQ', buf.read(16))
            extra_tags[name_hash] = value_hash
        
        # Annotation records
        rec_count = struct.unpack('<H', buf.read(2))[0]
        annotation_records = []
        for _ in range(rec_count):
            rec = AnnotationRecord.deserialize(buf)
            annotation_records.append(rec)
        
        return GameMetadata(final_move_hash, str_tags, extra_tags, annotation_records)
    
    def compute_hash(self) -> int:
        """Compute hash of metadata blob."""
        data = self.serialize()
        h = hashlib.blake2b(data, digest_size=8).digest()
        return struct.unpack('<Q', h)[0]


class MetadataStore:
    """Store for game metadata blobs."""
    
    def __init__(self, path: str):
        self.path = Path(path)
        self.metadata: Dict[int, GameMetadata] = {}  # hash -> metadata
    
    def add_metadata(self, meta: GameMetadata) -> int:
        """Add metadata and return its hash."""
        meta_hash = meta.compute_hash()
        if meta_hash not in self.metadata:
            self.metadata[meta_hash] = meta
        return meta_hash
    
    def get_metadata(self, meta_hash: int) -> Optional[GameMetadata]:
        """Retrieve metadata by hash."""
        return self.metadata.get(meta_hash)
    
    def save(self):
        """Write metadata to disk."""
        with open(self.path, 'wb') as f:
            # Write count
            f.write(struct.pack('<Q', len(self.metadata)))
            
            # Write each metadata blob: [Hash:8B][Length:4B][Data:Length]
            for meta_hash, meta in self.metadata.items():
                data = meta.serialize()
                f.write(struct.pack('<Q', meta_hash))
                f.write(struct.pack('<I', len(data)))
                f.write(data)
    
    def load(self):
        """Read metadata from disk."""
        if not self.path.exists():
            return
        
        with open(self.path, 'rb') as f:
            # Read count
            count_bytes = f.read(8)
            if len(count_bytes) != 8:
                return
            count = struct.unpack('<Q', count_bytes)[0]
            
            # Read each metadata blob
            for _ in range(count):
                hash_bytes = f.read(8)
                len_bytes = f.read(4)
                if len(hash_bytes) != 8 or len(len_bytes) != 4:
                    break
                
                meta_hash = struct.unpack('<Q', hash_bytes)[0]
                length = struct.unpack('<I', len_bytes)[0]
                data = f.read(length)
                
                if len(data) != length:
                    break
                
                meta = GameMetadata.deserialize(data)
                self.metadata[meta_hash] = meta


# ============================================================================
# PART 5: CCAMC STORE
# ============================================================================

class CCACMStore:
    """Main CCAMC store for managing games."""
    
    def __init__(self, directory: str, eco_path: str = "eco.tsv"):
        self.dir = Path(directory)
        self.dir.mkdir(exist_ok=True)
        
        self.packfile = PackFile(str(self.dir / "moves"))
        self.indexfile = IndexFile(str(self.dir / "idx"))
        self.string_store = StringStore(str(self.dir / "strings"))
        self.metadata_store = MetadataStore(str(self.dir / "metadata"))
        self.source_store = SourceStore(str(self.dir / "sources"))
        
        # Zobrist hashing via chess.polyglot
        self.game_registry: Dict[str, Tuple[int, int]] = {}  # game_id -> (final_move_hash, metadata_hash)
        self.game_registry_sources: Dict[str, int] = {}      # game_id -> source_hash (int)
        
        # Load ECO catalog (pre-converted to packed moves)
        self.eco_lines: List[Tuple[str, str, List[int]]] = []  # (code, name, packed_moves)
        self.eco_trie: Dict[int, Dict] = {}
        self._load_eco_catalog(eco_path)
        self._build_eco_trie()
        
        # Ensure initial position blob exists in store
        self._ensure_init_blob()
        
        self.packfile.load()
        self.indexfile.load()
        self.string_store.load()
        self.metadata_store.load()
        self.source_store.load()
        
        self._load_registry()
    
    def _ensure_init_blob(self):
        """Ensure initial position blob is in the packfile."""
        # The initial blob should already be hashed to INIT_BLOB_HASH
        # We just need to make sure it exists in the store
        if INIT_BLOB_HASH not in self.packfile.blobs:
            init_blob = MoveBlob(
                parent_hash=0,
                moves=[],
                result=3  # In progress
            )
            # Add using the normal method to ensure proper storage
            computed_hash = self.packfile.add_blob(init_blob)
            # Verify it matches INIT_BLOB_HASH
            assert computed_hash == INIT_BLOB_HASH, f"Init blob hash mismatch: {computed_hash:016x} != {INIT_BLOB_HASH:016x}"
    
    def _load_registry(self):
        """Load game registry from disk."""
        registry_path = self.dir / "registry"
        if registry_path.exists():
            with open(registry_path, 'r') as f:
                for line in f:
                    if line.strip():
                        # Split from right to handle game_ids with colons
                        parts = line.strip().rsplit(':', 3)
                        if len(parts) >= 3:
                            game_id = parts[0]
                            move_hash_str = parts[1]
                            meta_hash_str = parts[2]
                            source_hash_str = parts[3] if len(parts) >= 4 else "0"
                            self.game_registry[game_id] = (int(move_hash_str, 16), int(meta_hash_str, 16))
                            try:
                                self.game_registry_sources[game_id] = int(source_hash_str, 16)
                            except Exception:
                                self.game_registry_sources[game_id] = 0
    
    def _load_eco_catalog(self, eco_path: str):
        """Load ECO opening classifications and convert to packed moves."""
        if not os.path.exists(eco_path):
            print(f"Warning: ECO file not found at {eco_path}, ECO deduplication disabled")
            return

        cache_path = eco_path + ".cache"
        try:
            if os.path.exists(cache_path):
                with open(cache_path, 'rb') as cf:
                    cache = pickle.load(cf)
                src_mtime = os.path.getmtime(eco_path)
                src_size = os.path.getsize(eco_path)
                if cache.get('src_mtime') == src_mtime and cache.get('src_size') == src_size:
                    cached_lines = cache.get('eco_lines')
                    if cached_lines:
                        self.eco_lines = cached_lines
                        print(f"Loaded {len(self.eco_lines)} ECO lines (from cache)")
                        return
        except Exception:
            pass  # Fallback to parsing
        
        with open(eco_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                code = row.get('eco', '')
                name = row.get('name', '')
                pgn = row.get('pgn', '')
                
                # Parse and convert to packed moves
                try:
                    game = chess.pgn.read_game(io.StringIO(pgn))
                    if game:
                        packed_moves = []
                        node = game
                        while node.variations:
                            node = node.variation(0)
                            packed_moves.append(encode_move_packed(node.move))
                        
                        # Only include ECO lines with >= 6 ply
                        if len(packed_moves) >= 6:
                            self.eco_lines.append((code, name, packed_moves))
                except:
                    pass  # Skip malformed entries
        
        try:
            src_mtime = os.path.getmtime(eco_path)
            src_size = os.path.getsize(eco_path)
            with open(cache_path, 'wb') as cf:
                pickle.dump({'src_mtime': src_mtime, 'src_size': src_size, 'eco_lines': self.eco_lines}, cf)
        except Exception:
            pass
        
        print(f"Loaded {len(self.eco_lines)} ECO lines (>= 6 ply)")

    def _build_eco_trie(self):
        """Build a prefix trie for ECO packed moves to speed up matching."""
        root: Dict[int, Dict] = {}
        for eco_code, eco_name, eco_packed in self.eco_lines:
            node = root
            for move in eco_packed:
                node = node.setdefault(move, {})
            node.setdefault('_end', []).append((eco_code, eco_name, eco_packed))
        self.eco_trie = root
    
    def _find_matching_eco_lines(self, packed_moves: List[int]) -> List[Tuple[str, str, List[int]]]:
        """
        Find all ECO lines that match the start of this game.
        Returns list of (code, name, packed_eco_moves) sorted by length (shortest first).
        """
        matches = []
        node = self.eco_trie
        for move in packed_moves:
            node = node.get(move)
            if node is None:
                break
            if '_end' in node:
                # As we walk moves, we encounter prefixes in increasing length order
                matches.extend(node['_end'])
        return matches
    
    def _create_eco_blob_chain(self, eco_packed: List[int], parent_hash: int) -> int:
        """
        Create blob chain for an ECO sequence.
        Returns the final blob hash in the chain.
        Uses existing blob_index for deduplication.
        """
        move_idx = 0
        current_parent = parent_hash
        
        while move_idx < len(eco_packed):
            chunk_size = min(22, len(eco_packed) - move_idx)
            chunk = eco_packed[move_idx:move_idx + chunk_size]
            
            # Check if blob already exists
            existing_hash = self.packfile.find_blob_by_moves(current_parent, chunk)
            if existing_hash is not None:
                current_parent = existing_hash
                move_idx += chunk_size
                continue
            
            # Create new blob
            blob = MoveBlob(
                parent_hash=current_parent,
                moves=chunk,
                result=3  # Unknown result for ECO blobs
            )
            
            blob_hash = self.packfile.add_blob(blob)
            current_parent = blob_hash
            move_idx += chunk_size
        
        return current_parent
    
    def _save_registry(self):
        """Save game registry to disk."""
        registry_path = self.dir / "registry"
        with open(registry_path, 'w') as f:
            for game_id, (move_hash, meta_hash) in self.game_registry.items():
                src_hash = self.game_registry_sources.get(game_id, 0)
                f.write(f"{game_id}:{move_hash:016x}:{meta_hash:016x}:{src_hash:016x}\n")
    
    def _ingest_variation(self, start_node: chess.pgn.ChildNode) -> Tuple[int, int]:
        """
        Ingest a variation branch as a separate move chain.
        Returns (final_move_hash, metadata_hash).
        """
        # Collect moves in this variation
        moves = []
        node = start_node
        while not node.is_end():
            moves.append(node.move)
            if node.variations:
                node = node.variation(0)  # Follow mainline within variation
            else:
                break
        
        # Ingest the move chain similar to ingest_game
        packed_moves = []
        board = start_node.parent.board()
        for move in moves:
            packed_moves.append(encode_move_packed(move))
            board.push(move)
        
        # Split into blobs (max 22 moves per blob)
        parent_hash = INIT_BLOB_HASH
        for i in range(0, len(packed_moves), 22):
            chunk = packed_moves[i:i+22]
            
            blob = MoveBlob(
                parent_hash=parent_hash,
                moves=chunk,
                result=3  # Unknown
            )
            
            parent_hash = self.packfile.add_blob(blob)
        
        # Extract annotations from variation
        annotations = self._extract_annotations_from_node(start_node)
        
        # Create metadata for variation
        meta = GameMetadata(
            str_tags={},
            extra_tags={},
            annotation_records=annotations
        )
        
        meta_bytes = meta.serialize(self.string_store)
        meta_hash = self.metadata_store.add_metadata(meta_bytes)
        
        return parent_hash, meta_hash
    
    def _ingest_variation(self, start_node: chess.pgn.ChildNode) -> Tuple[int, int]:
        """
        Ingest a variation branch as a separate move chain.
        Extracts both moves and annotations (including nested variations).
        Returns (final_move_hash, metadata_hash).
        """
        # Collect moves in this variation
        moves = []
        node = start_node
        while not node.is_end():
            moves.append(node.move)
            if node.variations:
                node = node.variation(0)  # Follow mainline within variation
            else:
                break
        
        # Ingest the move chain
        packed_moves = []
        board = start_node.parent.board()
        for move in moves:
            packed_moves.append(encode_move_packed(move))
            board.push(move)
        
        # Split into blobs (max 22 moves per blob)
        parent_hash = INIT_BLOB_HASH
        final_move_hash = INIT_BLOB_HASH  # Track the final blob hash
        for i in range(0, len(packed_moves), 22):
            chunk = packed_moves[i:i+22]
            
            blob = MoveBlob(
                parent_hash=parent_hash,
                moves=chunk,
                result=3
            )
            
            parent_hash = self.packfile.add_blob(blob)
            final_move_hash = parent_hash  # Update to latest hash
        
        # Now extract annotations from this variation (including nested variations)
        annotations = self._extract_annotations_for_variation(start_node)
        
        # Create metadata for variation
        meta = GameMetadata(
            final_move_hash=final_move_hash,
            str_tags={},
            extra_tags={},
            annotation_records=annotations
        )
        
        meta_hash = self.metadata_store.add_metadata(meta)
        
        return final_move_hash, meta_hash
    
    def _extract_annotations_for_variation(self, start_node: chess.pgn.ChildNode) -> List[AnnotationRecord]:
        """Extract annotations from a variation (recursively handles nested variations)."""
        records = []
        move_index = 0
        
        node = start_node
        is_first_move = True
        while not node.is_end():
            # Comments
            if node.comment:
                comment_text = node.comment.strip()
                if comment_text:
                    is_semicolon = comment_text.startswith(';')
                    if is_semicolon:
                        comment_text = comment_text[1:].strip()
                    
                    text_hash = self.string_store.add_string(comment_text)
                    rec = AnnotationRecord(
                        move_index=move_index,
                        record_type=RecordType.COMMENT,
                        text_hash=text_hash,
                        is_pre=False,
                        is_semicolon=is_semicolon,
                        comment_newline=False
                    )
                    records.append(rec)
            
            # NAGs
            for nag in node.nags:
                rec = AnnotationRecord(
                    move_index=move_index,
                    record_type=RecordType.NAG,
                    nag_code=nag
                )
                records.append(rec)
            
            # Nested variations (alternatives at this position)
            # Skip on first move because we're at the fork point where we entered this variation
            if not is_first_move and len(node.parent.variations) > 1:
                for var_node in node.parent.variations[1:]:  # Skip mainline (index 0)
                    var_move_hash, var_meta_hash = self._ingest_variation(var_node)
                    rec = AnnotationRecord(
                        move_index=move_index,
                        record_type=RecordType.VARIATION,
                        variation_move_hash=var_move_hash,
                        variation_meta_hash=var_meta_hash
                    )
                    records.append(rec)
            
            if node.variations:
                node = node.variation(0)
                is_first_move = False
                move_index += 1
            else:
                break
        
        return records
    
    def _extract_annotations(self, game: chess.pgn.GameNode) -> List[AnnotationRecord]:
        """Extract annotations (comments, NAGs, variations) from PGN game tree."""
        records = []
        move_index = 0
        
        node = game
        while not node.is_end():
            node = node.next()
            
            # Comments
            if node.comment:
                comment_text = node.comment.strip()
                if comment_text:
                    is_semicolon = comment_text.startswith(';')
                    if is_semicolon:
                        comment_text = comment_text[1:].strip()
                    
                    text_hash = self.string_store.add_string(comment_text)
                    rec = AnnotationRecord(
                        move_index=move_index,
                        record_type=RecordType.COMMENT,
                        text_hash=text_hash,
                        is_pre=False,
                        is_semicolon=is_semicolon,
                        comment_newline=False
                    )
                    records.append(rec)
            
            # NAGs
            for nag in node.nags:
                rec = AnnotationRecord(
                    move_index=move_index,
                    record_type=RecordType.NAG,
                    nag_code=nag
                )
                records.append(rec)
            
            # Variations (alternative moves from this position)
            if len(node.parent.variations) > 1:
                # node.parent.variations[0] is the mainline (the move we just processed)
                # Remaining variations are alternatives
                for var_node in node.parent.variations[1:]:
                    var_move_hash, var_meta_hash = self._ingest_variation(var_node)
                    rec = AnnotationRecord(
                        move_index=move_index,
                        record_type=RecordType.VARIATION,
                        variation_move_hash=var_move_hash,
                        variation_meta_hash=var_meta_hash
                    )
                    records.append(rec)
            
            move_index += 1
        
        return records
    
    def ingest_game(self, game: chess.pgn.GameNode, game_id: Optional[str] = None, source_hash: Optional[int] = None) -> Tuple[int, int]:
        """
        Ingest a game into the store using copy-on-write.
        Returns (final_move_hash, metadata_hash).
        """
        if game_id is None:
            game_id = f"game_{len(self.game_registry)}"
        
        moves = list(game.mainline_moves())
        result_str = game.headers.get('Result', '*')
        result = RESULT_MAP.get(result_str, 3)
        
        # Extract headers for metadata
        str_tags = self._extract_str_tags(game.headers)
        extra_tags = self._extract_extra_tags(game.headers)
        
        # Extract annotations
        annotation_records = self._extract_annotations(game)
        
        # Convert moves to packed format
        packed_moves = []
        board = game.board()
        for move in moves:
            packed_moves.append(encode_move_packed(move))
            board.push(move)

        # Helper to advance a board by packed moves incrementally
        def _apply_packed(board_obj: chess.Board, packed_seq):
            for move_hash in packed_seq:
                move = decode_move_packed(move_hash, board_obj)
                if move:
                    board_obj.push(move)
        
        # Find matching ECO lines and create/reuse their blob chains
        eco_matches = self._find_matching_eco_lines(packed_moves)
        parent_hash = INIT_BLOB_HASH
        move_idx = 0
        
        # Create blob chains for matching ECO lines (hierarchical)
        for eco_code, eco_name, eco_packed in eco_matches:
            # Create/find ECO blob chain from current parent
            parent_hash = self._create_eco_blob_chain(eco_packed, parent_hash)
            move_idx = len(eco_packed)

        # Align board to the state after ECO moves (once)
        board = game.board()
        if move_idx:
            _apply_packed(board, packed_moves[:move_idx])
        
        # Continue with remaining moves after ECO sequences
        blob_hashes = []
        
        while move_idx < len(packed_moves):
            # Try to find the longest existing blob that matches from this position
            best_match_hash = None
            best_match_len = 0
            
            # Try different chunk sizes, largest first
            for chunk_size in range(min(22, len(packed_moves) - move_idx), 0, -1):
                chunk = packed_moves[move_idx:move_idx + chunk_size]
                
                # Look for existing blob with this parent and moves
                existing_hash = self.packfile.find_blob_by_moves(parent_hash, chunk)
                if existing_hash is not None:
                    best_match_hash = existing_hash
                    best_match_len = chunk_size
                    break  # Found a match, use it
            
            if best_match_hash is not None:
                # Reuse existing blob and advance board by its moves
                chunk = packed_moves[move_idx:move_idx + best_match_len]
                _apply_packed(board, chunk)
                blob_hashes.append(best_match_hash)
                parent_hash = best_match_hash
                move_idx += best_match_len
            else:
                # No existing match, create new blob with remaining moves (up to 22)
                chunk_size = min(22, len(packed_moves) - move_idx)
                chunk = packed_moves[move_idx:move_idx + chunk_size]

                # Advance board incrementally
                _apply_packed(board, chunk)

                # Determine result for this blob
                is_final = (move_idx + chunk_size == len(packed_moves))
                blob_result = result if is_final else 3

                blob = MoveBlob(
                    parent_hash=parent_hash,
                    moves=chunk,
                    result=blob_result
                )

                blob_hash = self.packfile.add_blob(blob)
                blob_hashes.append(blob_hash)
                parent_hash = blob_hash
                move_idx += chunk_size
        
        # Final hash is either the last blob created, or the last ECO blob if no additional moves
        final_hash = blob_hashes[-1] if blob_hashes else parent_hash
        
        # Create metadata (for now, basic version without annotations)
        metadata = GameMetadata(
            final_move_hash=final_hash,
            str_tags=str_tags,
            extra_tags=extra_tags,
            annotation_records=annotation_records
        )
        
        meta_hash = self.metadata_store.add_metadata(metadata)
        self.game_registry[game_id] = (final_hash, meta_hash)
        if source_hash is not None:
            self.game_registry_sources[game_id] = source_hash
        
        return (final_hash, meta_hash)
    
    def _extract_str_tags(self, headers: chess.pgn.Headers) -> Dict[int, int]:
        """Extract STR tags (Seven Tag Roster) from PGN headers."""
        str_tags = {}
        for i, tag_name in enumerate(STR_TAG_NAMES):
            if tag_name in headers:
                value_hash = self.string_store.add_string(headers[tag_name])
                str_tags[i] = value_hash
        return str_tags
    
    def _extract_extra_tags(self, headers: chess.pgn.Headers) -> Dict[int, int]:
        """Extract non-STR tags from PGN headers."""
        str_tag_set = set(STR_TAG_NAMES)
        extra_tags = {}
        for tag_name, tag_value in headers.items():
            if tag_name not in str_tag_set:
                # Include all tags, preserve empty strings as-is
                name_hash = self.string_store.add_string(tag_name)
                value_hash = self.string_store.add_string(tag_value)
                extra_tags[name_hash] = value_hash
        return extra_tags
    
    def reconstruct_game(self, game_id: str) -> List[chess.Move]:
        """Reconstruct a game from its blobs."""
        if game_id not in self.game_registry:
            return []
        
        final_hash, meta_hash = self.game_registry[game_id]
        
        # First, walk backward to collect all blobs in reverse order
        blob_chain = []
        current_hash = final_hash
        while current_hash not in (0, INIT_BLOB_HASH):
            blob = self.packfile.get_blob(current_hash)
            if not blob:
                break
            blob_chain.append(blob)
            current_hash = blob.parent_hash
        
        # Reverse to get chronological order (first blob first)
        blob_chain.reverse()
        
        # Now decode all moves in order
        moves = []
        board = chess.Board()
        for blob in blob_chain:
            for move_packed in blob.moves:
                move = decode_move_packed(move_packed, board)
                if move:
                    moves.append(move)
                    board.push(move)
        
        return moves
    
    def reconstruct_game_pgn(self, game_id: str) -> chess.pgn.Game:
        """Reconstruct a complete game as PGN including headers and annotations."""
        if game_id not in self.game_registry:
            return None
        
        final_hash, meta_hash = self.game_registry[game_id]
        meta = self.metadata_store.get_metadata(meta_hash)
        
        # Create new game
        game = chess.pgn.Game()
        
        # Restore headers from STR tags
        str_tag_names = ['Event', 'Site', 'Date', 'Round', 'White', 'Black', 'Result']
        for tag_id, string_hash in meta.str_tags.items():
            if tag_id < len(str_tag_names):
                tag_name = str_tag_names[tag_id]
                tag_value = self.string_store.get_string(string_hash)
                if tag_value:
                    game.headers[tag_name] = tag_value
        
        # Restore extra tags
        for name_hash, value_hash in meta.extra_tags.items():
            tag_name = self.string_store.get_string(name_hash)
            tag_value = self.string_store.get_string(value_hash)
            if tag_name is not None and tag_value is not None:
                game.headers[tag_name] = tag_value
        
        # Reconstruct moves
        moves = self.reconstruct_game(game_id)
        node = game
        for i, move in enumerate(moves):
            node = node.add_variation(move)
            
            # Add annotations (comments, NAGs, variations) for this move
            for rec in meta.annotation_records:
                if rec.move_index == i:
                    if rec.record_type == RecordType.COMMENT:
                        comment_text = self.string_store.get_string(rec.text_hash)
                        if comment_text:
                            if rec.is_semicolon:
                                comment_text = '; ' + comment_text
                            node.comment = comment_text
                    elif rec.record_type == RecordType.NAG:
                        node.nags.add(rec.nag_code)
                    elif rec.record_type == RecordType.VARIATION:
                        # Recursively reconstruct variation
                        var_moves = self._reconstruct_variation_moves(rec.variation_move_hash)
                        var_node = node
                        for var_move in var_moves:
                            var_node = var_node.add_variation(var_move)
        
        return game
    
    def _reconstruct_variation_moves(self, move_hash: int) -> List[chess.Move]:
        """Reconstruct moves from a variation move hash."""
        moves = []
        current_hash = move_hash
        blob_chain = []
        
        # Walk backward to collect blobs
        while current_hash not in (0, INIT_BLOB_HASH):
            blob = self.packfile.get_blob(current_hash)
            if not blob:
                break
            blob_chain.append(blob)
            current_hash = blob.parent_hash
        
        # Reverse for chronological order
        blob_chain.reverse()
        
        # Decode moves
        board = chess.Board()
        for blob in blob_chain:
            for move_packed in blob.moves:
                move = decode_move_packed(move_packed, board)
                if move:
                    moves.append(move)
                    board.push(move)
        
        return moves
    
    def save(self):
        """Persist store to disk."""
        self.packfile.save()
        self.indexfile.build_from_packfile(self.packfile)
        self.indexfile.save()
        self.string_store.save()
        self.metadata_store.save()
        self._save_registry()
        self.source_store.save()


# ============================================================================
# TESTING
# ============================================================================

def main() -> None:
    """Command-line interface for ingesting PGN files."""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: ccamc.py <pgn_file> [output_dir]")
        sys.exit(1)
    
    pgn_file = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "ccamc_store"
    
    print(f"Loading games from {pgn_file}...")
    
    store = CCACMStore(output_dir)

    # Prepare source descriptor
    size_bytes, sha256_hex = _hash_file_sha256(pgn_file)
    imported_at = datetime.utcnow().isoformat()
    source_label = os.path.basename(pgn_file)
    source_entry = SourceEntry(
        label=source_label,
        imported_at=imported_at,
        byte_size=size_bytes,
        source_sha256_hex=sha256_hex,
    )
    source_hash = store.source_store.add(source_entry)
    
    games_loaded = 0
    with open(pgn_file, 'r') as f:
        while True:
            game = chess.pgn.read_game(f)
            if game is None:
                break
            
            store.ingest_game(game, f"game_{games_loaded}", source_hash=source_hash)
            games_loaded += 1
            
            if games_loaded % 1000 == 0:
                print(f"  Loaded {games_loaded} games...")
    
    print(f"Saving store...")
    store.save()
    
    print(f"\nStore statistics:")
    print(f"  Total games: {len(store.game_registry)}")
    print(f"  Total blobs: {len(store.packfile.blobs)}")
    print(f"  Pack file size: {len(store.packfile.blob_order) * 64} bytes")
    
    # Test reconstruction
    if store.game_registry:
        test_id = list(store.game_registry.keys())[0]
        moves = store.reconstruct_game(test_id)
        print(f"\nReconstructed {test_id}: {len(moves)} moves")


if __name__ == '__main__':
    main()
