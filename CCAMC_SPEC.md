================================================================================
SPECIFICATION: CHESS CONTENT-ADDRESSABLE MOVE-CHAIN (CCAMC) v1.0
================================================================================

1. OVERVIEW
-----------
The CCAMC is a high-density, backward-linked Merkle DAG designed to store 
millions of chess games with bit-perfect move-order reconstruction. It 
utilizes piece-agnostic coordinate encoding to maximize structural 
deduplication across opening and midgame sequences.

2. ATOMIC UNIT: PACKED UCI MOVE (16-BIT)
----------------------------------------
Stored as a Little-Endian unsigned 16-bit integer.

Bits    | Width | Field      | Range/Description
--------|-------|------------|--------------------------------------------------
00-05   | 6     | FromSq     | 0 (a1) to 63 (h8)
06-11   | 6     | ToSq       | 0 (a1) to 63 (h8)
12-14   | 3     | Promotion  | 0: None, 1: Q, 2: R, 3: B, 4: N
15      | 1     | Reserved   | Always 0

3. THE MOVE-BLOB STRUCTURE (64 BYTES)
-------------------------------------
Fixed-size blocks optimized for memory alignment and SSD page-caching.

Offset  | Size  | Field        | Description
--------|-------|--------------|-------------------------------------------------
0x00    | 8B    | ParentHash   | 64-bit Hash of previous blob:
        |       |              |   INIT_BLOB_HASH = starts from initial position
        |       |              |   ORPHAN_PARENT_HASH = orphan variation
        |       |              |   other = hash of previous blob
0x08    | 8B    | ZobristHash  | Board state hash AFTER moves in this blob
0x10    | 1B    | MoveCount    | Number of moves in payload (Max 22)
0x11    | 1B    | Flags        | b0: ECO Anchor, b1: Game End, b2-7: Reserved
0x12    | 44B   | MoveData     | Array of 22 Packed UCI Moves (2 bytes each)
0x3E    | 2B    | Meta/Result  | Optional (0: 1-0, 1: 0-1, 2: 1/2, 3: *)
--------------------------------------------------------------------------------
TOTAL: 64 BYTES


4. FILE ARCHITECTURE
--------------------

A. THE PACKFILE (.pack)
   - Header (16B): [Magic 'CHSS' 4B] [Ver 2B] [BlobCount 8B] [Padding 2B]
   - Body: Concatenated 64-byte Move-Blobs. Append-only.

B. THE INDEX (.idx)
   - Entry (16B): [BlobHash 8B] [ByteOffset 8B]
   - All entries must be sorted by BlobHash for binary search.
   - A 64KB Fan-out table in RAM points to the start of hash-prefixes 0x0000 
     through 0xFFFF in the index file to minimize disk seeks.

5. CORE LOGIC
-------------

SPECIAL INITIAL POSITION BLOB:
A well-known blob representing the starting position exists with:
- ParentHash: 0x0
- ZobristHash: (hash of initial position)
- MoveCount: 0
- Flags: 0
- MoveData: (empty)
- Result: 3 (unknown)
This blob's content-addressable hash is INIT_BLOB_HASH (computed normally).
All games starting from the initial position have their first blob's
ParentHash = INIT_BLOB_HASH.

ORPHAN VARIATION PARENT MARKER:
Variations starting mid-blob (without a real parent blob) use a well-known
parent hash computed as:
  ORPHAN_PARENT_HASH = XXHash64("ORPHAN_VARIATION_PARENT_MARKER")
This distinguishes orphan variations from the initial position and from
real blob chains. Collision with actual blob content is astronomically unlikely.

INGESTION:
1. For a new game, check if any existing blob chain matches the game's initial 
   move sequence.
2. If a matching sequence is found:
   a. Continue from that blob's hash as the parent.
   b. Create new blob(s) for the remaining moves.
3. If no match is found:
   a. Create new blob(s) starting from INIT_BLOB_HASH.
   b. Pack moves into blobs of up to 22 moves each.
4. Hash calculation: BlobHash = XXHash64(ParentHash || MoveData || ZobristHash)

OPTIMIZATION - WELL-KNOWN PARENT BLOBS:
Implementations MAY pre-create or lazily generate blobs for commonly occurring
opening sequences (e.g., ECO opening classifications, popular opening lines) to
maximize deduplication across large game collections. When ingesting a game:
1. Determine the longest matching well-known sequence (e.g., by matching the
   game's move sequence against a catalog of opening lines).
2. If the corresponding blob exists, use it as the parent for the game.
3. If not, create the blob(s) for that sequence and add them to the store.
4. Continue with normal ingestion for the remaining moves.

This approach allows games sharing common openings to automatically share
blob chains without requiring complex runtime shattering or reorganization.

RECONSTRUCTION:
1. Input: Game Identifier (Hash of the final Move-Blob).
2. Fetch Blob via Index -> Extract Moves -> Follow ParentHash.
3. Repeat until ParentHash == INIT_BLOB_HASH or ParentHash == ORPHAN_PARENT_HASH.
4. Reverse the total move list to restore chronological order.
5. Verify board state using ZobristHash at each jump to ensure integrity.

Note: ParentHash == ORPHAN_PARENT_HASH indicates an orphan variation blob; these
are only reconstructed when explicitly referenced by metadata, not as game roots.

6. GARBAGE COLLECTION
---------------------
Since blobs are immutable and copy-on-write creates orphaned blobs, periodic 
garbage collection is necessary:

MARK PHASE:
1. Maintain a Game Registry mapping GameID -> (FinalMoveHash, MetadataHash).
2. Starting from each game's final move blob, walk backward following ParentHash
   until reaching INIT_BLOB_HASH or ORPHAN_PARENT_HASH. Mark all reachable blobs.
3. Mark each game's metadata blob.
4. For each marked metadata blob, mark all variation move blobs referenced
   within (these may have ParentHash == ORPHAN_PARENT_HASH, making them orphans
   that are only reachable via metadata).
5. Mark the special INIT_BLOB_HASH blob (always kept).

SWEEP PHASE:
1. Scan the .pack file for unmarked blobs.
2. Write a new compacted .pack file containing only marked blobs.
3. Rebuild the .idx file with updated offsets.
4. Atomic swap: rename new files over old files.

OPTIMIZATION:
- Use a bloom filter to quickly identify candidates for garbage collection.
- Run GC offline or during low-traffic periods.
- Consider reference counting if game registry is small enough.

7. IMPLEMENTATION NOTES
-----------------------
- Hashing: Use XXHash64 or SipHash for the 64-bit content-addressable keys.
- Disk I/O: Use memory-mapped files (mmap) for both .pack and .idx.
- Scaling: Since it is backward-linked, the "Head" of the game is the only 
  pointer needed. Multiple games can share the same parent blobs.
- Game Registry: Maintain a separate file mapping GameID/Name to 
  (FinalMoveHash, MetadataHash) for reconstruction and GC. Both hashes are 
  required to fully reconstruct a game with annotations and layout.
- Zobrist Hashing: Compute incrementally during ingestion using standard 
  Zobrist tables for each (piece, square) combination.

8. METADATA EXTENSION
---------------------
To support PGN headers, comments, NAGs, variations, and layout preservation, 
CCAMC uses a lightweight metadata overlay that does NOT duplicate move data.

A. STRING STORE (.strings)
   - Binary key-value store: Hash(64-bit) -> UTF-8 bytes
   - Format: [Count:8B] then repeated [Hash:8B][Length:4B][UTF-8 Data:Length]
   - All text (headers, comments) is deduplicated via content-addressable hash
   - No JSON representation is stored; JSON dumps are for debugging only

B. METADATA BLOB (PER GAME)
   Each game has one metadata blob with sparse record-based storage.
   Only items that exist in the source are stored (no empty placeholders).
   
   STRUCTURE:
   
   1. BINDING: [FinalMoveHash:8B]
      - Binds this metadata blob to its move chain
   
   2. HEADERS SECTION:
      a) Seven Tag Roster (STR):
         [STR_Count:1B] (0-7)
         For each STR tag present:
           [TagID:1B] [ValueHash:8B]
         TagIDs: 0=Event, 1=Site, 2=Date, 3=Round, 4=White, 5=Black, 6=Result
      
      b) Additional Tags:
         [Extra_Count:2B]
         For each additional tag:
           [NameHash:8B] [ValueHash:8B]
   
   3. ANNOTATION RECORDS SECTION:
      [Record_Count:2B]
      For each annotation record (in mainline move order):
        [MoveIndex:varint] - Index into mainline move chain
        [RecordType:1B] - Type and flags packed:
          bits 0-2: Type (0=COMMENT, 1=NAG, 2=VARIATION, 3=NEWLINE)
          bit 3: (for COMMENT) 0=post, 1=pre
          bit 4: (for COMMENT) 0=brace {}, 1=semicolon ;
          bit 5: (for COMMENT/NEWLINE) newline_after flag
          bits 6-7: Reserved
        
        Type-specific payload:
          COMMENT:  [TextHash:8B]
          NAG:      [NAGCode:1B]
          VARIATION: [MoveHash:8B] [MetaHash:8B]
          NEWLINE:  (no payload - just marks line break after this move)

C. RECORD TYPE DETAILS
   
   COMMENT records:
   - Bit 3: position (0=after move, 1=before move)
   - Bit 4: delimiter (0=braces {}, 1=semicolon ;)
   - Bit 5: newline flag (1=newline after comment)
   - TextHash points to string store
   
   NAG records:
   - Single byte NAG code (0-255)
   
   VARIATION records:
   - MoveHash: final blob hash of variation's move chain
   - MetaHash: hash of variation's metadata blob (or 0 if no metadata)
   - Variations are first-class content-addressable entities
   - Fully deduplicated across games
   
   NEWLINE records:
   - Marks that a line break appears after this move
   - Multiple records per move allowed for multiple annotation types
   - Bit 5 used for consistency with comment newlines

D. PGN RECONSTRUCTION WITH LAYOUT PRESERVATION
   Emission algorithm:
   1. Write headers: STR in order, then additional tags alphabetically
   2. Walk mainline move chain in order:
      - For each move index, check for annotation records
      - Process records in order:
        * PRE comments before move
        * Write move number + SAN (derived from packed move)
        * NAGs as $n
        * POST comments after move
        * Variations as ( recursive_movetext )
        * NEWLINE records insert line break
      - Emit single space between tokens on same line
   3. Spaces normalized to single spaces (inter-token spacing not preserved)
   4. Line breaks and comment delimiters preserved exactly via flags

E. STORAGE OVERHEAD
   - Minimal games (no annotations): ~56 bytes for STR headers
   - Per comment: ~10 bytes (index + type + hash + flags)
   - Per NAG: ~3 bytes (index + type + code)
   - Per variation: ~18 bytes (index + type + two hashes)
   - Per newline: ~2 bytes (index + type)
   - String deduplication makes heavily annotated databases efficient

================================================================================