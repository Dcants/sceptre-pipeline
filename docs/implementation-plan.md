# Sceptre VITA-49 Live Ingestion Pipeline — Staged Implementation Plan

## Context

This document is the staged build plan for the live ingestion pipeline of a Sceptre SDR that streams IQ data over UDP using a VITA-49.2 subset (VRL disabled, no VRT trailer per the PDF — but see the wire-format corrections in Appendix A, which override the PDF where they conflict). The runtime goal is:

> **UDP packets → interpret → accumulate → emit `{numpy array + typed context dict}`** to downstream consumers (FFT / recording / audio).

The design is two threads:

- **Thread A (SOURCE):** socket *or* pickle-file → pushes raw packet bytes onto a bounded `raw_queue`. Dumb and fast; never parses.
- **Thread B (INTERPRETER + BUFFER):** pulls raw bytes, parses the header, decodes context, owns `current_context`, accumulates data payloads, flushes on triggers, and emits units to a downstream callback. The buffer lives inside Thread B — it is not its own thread.

**How to use this document.** Each stage below is a self-contained prompt you can hand to a CLI coding agent, with (1) objective, (2) files to create/modify, (3) an implementation prompt, and (4) acceptance criteria. Stages are independently testable and build on each other. **Hand Appendix A alongside every stage prompt.** Runtime code uses **stdlib + numpy only**; `pytest` is a dev-only dependency (the shipped library imports nothing but stdlib + numpy, enforced by a test). The interpreter and buffer are developed and tested **entirely offline** against the two existing captures via `ReplaySource` — no SDR attached.

### Conventions (established in Stage 0)
- **Package layout:** src-layout, `src/sceptre_pipeline/`.
- **Context-decode verification:** empirical against the two real recordings (`recordings/single_frequency.pkl`, `recordings/change_frequency.pkl`) as ground truth.
- **Emit path:** callback `on_emit(unit)`, pluggable so a queue-backed sink can drop in later.
- **Target:** Python ≥3.10 (dev env has 3.13 / numpy 2.3).

---

## ⚠️ Appendix A — Wire-Format Ground Truth (empirically verified; OVERRIDES the PDF/spec where they conflict)

> **Hand this appendix alongside every stage prompt.** These facts were verified by decoding `recordings/single_frequency.pkl` and `recordings/change_frequency.pkl` directly (both 3072 datagrams; one datagram = one VITA-49 packet). The PDF and the naive spec are *wrong* on several load-bearing points; a CLI agent that copies them verbatim will ship silently corrupt output.

**Word0 bit extraction (verified).** `word0 = struct.unpack(">I", b[:4])[0]`, bits numbered 0 = MSB:
| Field | Bits | Extraction | Data pkt | Ctx pkt |
|---|---|---|---|---|
| Packet Type | 0–3 | `(w0>>28)&0xF` | `1` (IF data) | `4` (IF context) |
| class_id flag | 4 | `(w0>>27)&1` | **`1`** | **`1`** |
| trailer flag | 5 | `(w0>>26)&1` | **`1`** | `0` |
| TSI | 8–9 | `(w0>>22)&3` | `1` (UTC) | `1` |
| TSF | 10–11 | `(w0>>20)&3` | `2` (RT ps) | `2` |
| Packet Counter | 12–15 | `(w0>>16)&0xF` | mod-16, per-stream | per-stream |
| Packet Size (words) | 16–31 | `w0&0xFFFF` | `2048` (=8192 B) | `16` (=64 B) |

Header remainder (big-endian): `stream_id`=`u32@4`, `int_ts`=`u32@8` (Unix s), `frac_ts`=`u64@12` (picoseconds). Header = **20 bytes** total. `time = int_ts + frac_ts / 1e12`.

**C1 — class_id and trailer flags are SET, not 0 (PDF says "Always 0" — FALSE).** Real packet layouts:
```
DATA (cid=1, trl=1):  [20B header][8B id-word][N×8B IQ samples][4B trailer]
CTX  (cid=1, trl=0):  [20B header][8B id-word][4B CIF][context fields...]
```
Derive payload boundaries from the flags — never assume payload starts at byte 20:
```python
start = 20 + (8 if class_id else 0)     # skip the 8-byte id-word
end   = len(b) - (4 if trailer else 0)  # drop the 4-byte trailer
```

**C2 — the naive `num_samples` formula is WRONG.** `floor((packet_size*4 - 20)/8) = 1021` and injects a garbage first sample (~2.19e15). Correct count comes from the trimmed body length:
```python
body = b[start:end]                             # 8160 bytes, evenly divisible by 8
num_samples = len(body) // bytes_per_sample     # = 1020  (verified)
```
Cross-check `len(b) == packet_size*4` and log any mismatch. (Confirmed self-consistent: the id-word is a picosecond counter advancing +1,632,000,000 ps/packet = 1.632 ms = 1020 samples @ 625 kHz.)

**C3 — the 8-byte id-word (`bytes[20:28]`) is a per-packet 64-bit picosecond counter, not a static Class ID.** Skip it; do not validate it as a constant, and do not treat its change as a stream error.

**C4 — only CIF bits 29/27/21/15 ever appear in the recordings.** Gain (23), Formatted GPS (14), ECEF Ephemeris (12) are **absent from both files** → their decode paths (incl. the gain `int16/128` convention) can only be tested with **synthetic** bytes, never the real fixtures.

**Context payload decode.** After header + id-word, read the 32-bit **Context Indicator Field (CIF)**, then walk present bits **31→0 (descending)**, advancing the offset by a fixed `FIELD_SIZE` for *every* present bit (decode only the ones you use, but advance past all so an unexpected optional field can't desync the walk):

```python
FIELD_SIZE = {29: 8, 27: 8, 23: 4, 21: 8, 15: 8, 14: 44, 12: 52}  # bytes
```
| CIF bit | Field | Size | Conversion | Verified value |
|---|---|---|---|---|
| 29 | Bandwidth | 8 (`int64`) | `raw / 2**20` Hz | 500,000.0 Hz |
| 27 | RF Ref Freq | 8 (`int64`) | `raw / 2**20` Hz | 97,300,000.0 → 103,700,000.0 |
| 23 | Gain | 4 (two `int16`) | each `raw/128` dB, summed (stage2=bits31:16, stage1=bits15:0) | *absent — synthetic only* |
| 21 | Sample Rate | 8 (`int64`) | `raw / 2**20` Hz | 625,000.0 Hz |
| 15 | Data Packet Payload Format | 8 (`u64`) | bitfield below | `0x2e0007df00000000` |
| 14 | Formatted GPS | 44 | radix-22 lat/lon (`/2**22` deg) | *absent — synthetic only* |
| 12 | ECEF Ephemeris | 52 | pos `/2**5` m, vel `/2**16` m/s | *absent — synthetic only* |

Verified real context: `CIF = 0x28208000`, present bits `[29, 27, 21, 15]`.

**Data Packet Payload Format — exact 64-bit windows (verified on `0x2e0007df00000000`):**
| Bits (63 = MSB) | Field | Value | Meaning |
|---|---|---|---|
| 62:61 | Real/Complex | `01` | complex cartesian |
| 60:56 | Data Item Format | `01110` (=14) | IEEE-754 single float |
| 43:38 | Item Packing Field Size **−1** | `31`→32 bits | container size |
| 37:32 | Data Item Size **−1** | `31`→32 bits | component size |

```python
is_complex       = ((u >> 61) & 0b11) == 0b01
data_item_format = (u >> 56) & 0x1F                # assert == 0b01110 (IEEE float)
component_bits   = ((u >> 38) & 0x3F) + 1          # 32
component_bytes  = component_bits // 8             # 4
bytes_per_sample = (2 if is_complex else 1) * component_bytes   # 8 — assert == 8, else fail loudly
```

**Endianness — the conversion order is mandatory (silent NaN trap otherwise).** Wire IQ is big-endian float32. In the buffer's single conversion:
```python
raw = b"".join(chunks)
arr = np.frombuffer(raw, dtype=">f4").astype(np.float32)  # BYTESWAP big→native HERE, first
if is_complex:
    arr = arr.view(np.complex64)                          # pair adjacent I,Q float32 → complex
```
`.view()` never swaps bytes — viewing `>f4`/raw as `complex64` directly yields garbage/NaN with **no exception**. Keep two distinct quantities: **`bytes_per_sample` = 8** (for trim math / sample counting) versus the numpy **component dtype `">f4"` = 4 B** (for `frombuffer`, which counts *components* — 2 per complex sample; `.view(complex64)` halves them).

**Fixed-point verdict vs the VITA-49.2 standard:** freq/rate/bandwidth `int64/2**20` and gain `int16/128` are both standard-correct; freq/rate/bw are confirmed clean on the wire, gain is convention-correct but untestable here.

---

## Package layout & conventions (Stage 0 establishes this)

```
sceptre-pipeline/
  pyproject.toml            # name=sceptre-pipeline, pkg=sceptre_pipeline, deps=[numpy], dev=[pytest], py>=3.10, src-layout
  README.md                 # overview, architecture diagram, quickstart, dev setup
  .gitignore                # __pycache__, .pytest_cache, build/, dist/, *.egg-info, .venv; keep the two named fixtures, ignore recordings/udp_capture_*.pkl
  src/sceptre_pipeline/
    __init__.py
    formats.py              # CIF bit map, FIELD_SIZE, DATA_ITEM_FORMAT_FLOAT=0b01110, radix consts, EXPECTED_BYTES_PER_SAMPLE=8
    queues.py               # BoundedRawQueue (drop-oldest-and-count), SHUTDOWN sentinel
    sources.py              # PacketSource(ABC), LiveSource, ReplaySource
    recorder.py             # Recorder, default_recording_path()
    interpreter.py          # Header, parse_header, decode_context, trim_data, Interpreter
    buffer.py               # IngestBuffer (routing, 4 triggers, single conversion)
    runtime.py              # Pipeline (wires Thread A + Thread B + on_emit callback)
    __main__.py             # CLI: --replay <pkl> | --live --host --port [--record]
  receiver/recieve_udp.py   # kept; refactored to import Recorder + write into recordings/
  recordings/               # single_frequency.pkl, change_frequency.pkl (fixtures, kept)
  tests/
    conftest.py             # recording-path fixtures + synthetic-packet builders
    test_queue.py test_sources_replay.py test_header.py test_context.py
    test_trim.py test_buffer_triggers.py test_runtime.py
```

Use `from __future__ import annotations` for `X | None` at runtime on 3.10. Avoid deprecated `datetime.utcfromtimestamp` (3.13).

---

## Stage 0 — Project scaffold

**Objective.** Turn the empty skeleton into an installable, testable src-layout package with deps, pytest, and fixtures pointing at the two real recordings. No pipeline logic.

**Files:** create `pyproject.toml`, `src/sceptre_pipeline/__init__.py`, `.gitignore`, `README.md`, `tests/conftest.py`.

**Implementation prompt.** *Fill `pyproject.toml` (PEP 621): project name `sceptre-pipeline`, package `sceptre_pipeline`, hatchling build backend with src-layout, `requires-python = ">=3.10"`, `dependencies = ["numpy"]`, `[project.optional-dependencies] dev = ["pytest"]`. Add `src/sceptre_pipeline/__init__.py` with a `__version__` and one-line docstring. Write `.gitignore` (Python defaults: `__pycache__/`, `*.pyc`, `.pytest_cache/`, `build/`, `dist/`, `*.egg-info/`, `.venv/`) — keep `recordings/single_frequency.pkl` and `recordings/change_frequency.pkl` tracked but ignore `recordings/udp_capture_*.pkl`. Write `README.md` covering the two-thread architecture, quickstart (`pip install -e .[dev]`; replay + live commands), and a note that runtime deps are stdlib + numpy only. In `tests/conftest.py` add fixtures: `recordings_dir`, `single_frequency_path`, `change_frequency_path`, a `load_capture(path)` helper, and stub builders `build_data_packet(...)`/`build_context_packet(...)` (filled in later stages) for synthetic bytes.*

**Acceptance criteria.**
- `pip install -e .[dev]` succeeds; `python -c "import sceptre_pipeline"` works.
- `pytest` runs green (a smoke test asserting the two recording fixtures exist and load).
- `.gitignore` keeps the two named fixtures and ignores `udp_capture_*.pkl`.

---

## Stage 1 — Source abstraction + bounded queue + recorder

**Objective.** One `PacketSource` interface, two implementations (`LiveSource`, `ReplaySource`) feeding a shared bounded `raw_queue`; a `Recorder` factored out of `recieve_udp.py`; `recieve_udp.py` refactored to write into a project-root `recordings/` dir and reuse the recorder. `.pkl` format stays byte-compatible.

**Files:** create `src/sceptre_pipeline/queues.py`, `sources.py`, `recorder.py`, `tests/test_queue.py`, `tests/test_sources_replay.py`; modify `receiver/recieve_udp.py`.

**Implementation prompt.** *Implement `BoundedRawQueue(maxsize)` backed by `collections.deque` + `threading.Condition`: `put(item)` is non-blocking and lossy — when full, `popleft()` the **oldest** and increment a `dropped` counter, then append and `notify()`; `get(timeout)` waits on the condition and returns the item, or `None` on timeout (this `None` is the age-poll hook). Do NOT use `deque(maxlen=)` (it drops without counting and gives no wake signal). Expose `dropped` read under the lock. Define a module-level `SHUTDOWN = object()` sentinel — distinct from the `None` timeout value.*

*Implement `PacketSource(ABC)` with `start()`/`stop()`. `LiveSource(host, port, raw_queue, stop: threading.Event, recorder=None, buffer_size=65535)` runs a socket loop in its own thread: `sock.settimeout(0.5)` so it observes `stop` promptly; on each `recvfrom`, fan out to **two independent sinks** — `raw_queue.put(data)` (lossy) and, if `recorder` is set, `recorder.append(time.time_ns(), addr, data)` (never affected by a queue drop). Use `with socket:` / try-finally. `ReplaySource(path, raw_queue, stop, pace=False)` loads the capture, iterates `packets`, pushes `packet[3]` (payload bytes); if `pace`, sleep by recorded `ts_ns` deltas; enqueue `SHUTDOWN` at EOF so Thread B flushes and exits.*

*Implement `Recorder(max_packets=None, max_bytes=None)`: `append(ts_ns, addr, data)` is O(1) in-memory and **bounded by a hard cap that stops appending + logs once** when reached (NOT a ring — recordings must stay complete); `save(path, start_time_ns, duration_seconds)` pickles `{"start_time_ns", "duration_seconds", "packets": [(ts_ns, ip, port, bytes), ...]}` with `HIGHEST_PROTOCOL`, byte-compatible with the existing format. `default_recording_path(root)` → `root/"recordings"/f"udp_capture_{timestamp}.pkl"`, creating the dir. Refactor `receiver/recieve_udp.py` so `default_output_filename()` resolves into the project-root `recordings/` dir and the capture path reuses `Recorder`; keep the CLI and pickle schema unchanged.*

**Acceptance criteria.**
- `BoundedRawQueue`: filling past `maxsize` evicts the **oldest**, increments `dropped`, keeps the newest; `get(timeout)` returns `None` on timeout, item otherwise.
- `ReplaySource` replays `single_frequency.pkl` fully (3072 items) and enqueues `SHUTDOWN` at EOF.
- `Recorder` round-trip: `save()` output re-loads equal to a source recording's structure (schema + a sample packet tuple match).
- **Decoupling:** a forced `raw_queue` overflow does not reduce `recorder` contents (independent-sinks test).
- `recieve_udp.py` writes to `recordings/`; existing captures still load.

### Stage 1 addendum — Live-path hardening (kernel receive buffer + bounded live recording)

> **Amends already-built Stage 1 code for real-socket operation.** The recordings never exercise the socket, so neither issue appears in replay tests. Apply before any live run; purely additive — offline/replay behavior is unchanged. (Independent of Stages 2.75/3/4 — can land any time before going live.)

**Objective.** Close two live-only gaps: (1) enlarge the kernel UDP receive buffer on `LiveSource` so a brief Thread-B stall (numpy conversion, GC) doesn't cause **kernel-level** packet loss the app can't see — `BoundedRawQueue.dropped` only counts drops *after* `recvfrom`; (2) bound live recording so an indefinite `--record` session can't grow memory without limit (the recorder enforces caps but defaults to none).

**Files:** modify `src/sceptre_pipeline/sources.py` (`LiveSource`) and the Stage 4 CLI recorder wiring in `src/sceptre_pipeline/__main__.py`; add `tests/test_live_hardening.py`.

**Implementation prompt.** *`SO_RCVBUF`: give `LiveSource.__init__` an `rcvbuf_bytes` param (default 8 MiB). In `_run`, right after creating the socket, `sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, rcvbuf_bytes)`, then read it back with `sock.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)` and `logger.info` the granted size — the OS may clamp below the request (Linux caps at `net.core.rmem_max` and reports a doubled value; Windows honors it directly). Never fail on a refused set — log a warning and continue. Rationale: at ~5 MB/s (≈ 8192 B × ~613 pkt/s) the OS default (~64 KiB Windows / ~208 KiB Linux ≈ 8–25 packets) overflows on any stall, dropping packets in the kernel where the app can't count them; the recordings look clean only because `recieve_udp.py` does almost no per-packet work. Bounded live recording: the recorder already stops-and-warns at `max_packets`/`max_bytes` — the gap is only the default. In the Stage 4 `--record` path, construct the `Recorder` with an explicit cap (add `--record-max-bytes`, default e.g. 512 MiB, and/or `--record-max-packets`) so an indefinite live session stays bounded; `recieve_udp.py`'s duration-bounded capture may keep the unbounded default. Log the recorder's `capped` state on shutdown.*

**Acceptance criteria.**
- `LiveSource` requests `SO_RCVBUF` (constructor-configurable, sane multi-MiB default) and logs the granted size; a test asserts the option is applied (and, where the OS honors it, the granted buffer ≥ a floor), and that a refused/clamped set logs a warning without crashing.
- The Stage 4 live `--record` path builds a **bounded** `Recorder` by default (non-None cap with no explicit flag), verified by a test; offline/duration-bounded capture via `recieve_udp.py` still works.
- No regression: all replay/offline tests unchanged; `LiveSource` still fans out to `raw_queue` (lossy) + recorder (decoupled).

---

## Stage 2 — Interpreter (header parse + context decode + data trim; owns current_context)

**Objective.** Parse the 20-byte header, decode context packets into a **typed** dict, trim data using the empirical layout (C1/C2), detect per-stream mod-16 gaps, and emit per-packet records. Hold `CURRENT_CONTEXT`; drop-with-warning any data arriving before the first context.

**Files:** create `src/sceptre_pipeline/formats.py`, `interpreter.py`, `tests/test_header.py`, `test_context.py`, `test_trim.py`.

**Implementation prompt.** *(Include Appendix A.) In `formats.py` put the CIF bit map, `FIELD_SIZE`, `DATA_ITEM_FORMAT_FLOAT=0b01110`, `EXPECTED_BYTES_PER_SAMPLE=8`, and radix constants. In `interpreter.py`:*
- *`Header` dataclass + `parse_header(b)` using the verified word0 extraction — including `class_id` and `trailer` flags — plus `stream_id`, `int_ts`, `frac_ts`, and `timestamp = int_ts + frac_ts/1e12`.*
- *`decode_context(b, hdr)`: offset = `20 + (8 if hdr.class_id else 0)`; read the 32-bit CIF; walk bits 31→0, advancing by `FIELD_SIZE[bit]` for every present bit; decode bits 29/27/23/21/15. Decode Data Packet Payload Format with the exact windows; **assert** `data_item_format == 0b01110` and `bytes_per_sample == 8`, failing loudly otherwise. Return a typed dict: `{rf_hz, sample_rate_hz, bandwidth_hz, gain_db?, is_complex, bytes_per_sample, data_item_format, component_dtype: ">f4"}`.*
- *`trim_data(b, hdr, bytes_per_sample)`: `start=20+(8 if hdr.class_id else 0)`, `end=len(b)-(4 if hdr.trailer else 0)`, `body=b[start:end]`, `num_samples=len(body)//bytes_per_sample`, return `(body[:num_samples*bytes_per_sample], num_samples)`. Cross-check `len(b)==hdr.packet_size*4`, log mismatch. Keep this pure and independent of the format assertion so it's testable on sub-32-bit formats.*
- *`Interpreter`: hold `_current_context` and per-stream `_last_counter[stream_id]`. `process(raw)`: parse header; if context → decode, update `_current_context`, return a context record; if data → if no context yet, return `None` + warn (startup edge case); compute `gap_before = prev is not None and counter != (prev+1)%16`; trim; return a data record. Record shape: `{type, metadata{stream_id, counter, timestamp, packet_size, dtype:">f4", bytes_per_sample, is_complex, gap_before}, context: dict|None, data: bytes|None}`.*

**Acceptance criteria.**
- **Header** (synthetic known bytes): `1e600800` → type 1, cid 1, trl 1, tsi 1, tsf 2, cnt 0, size 2048; `4b600010` → type 4, cid 1, trl 0, size 16.
- **Context** (real recording): asserts BW=500000.0, RF=97300000.0, SR=625000.0, `is_complex=True`, `bytes_per_sample=8`, `data_item_format=0b01110`.
- **Trim** (real data): exactly **1020** samples, `body % 8 == 0`, no garbage first sample; and a **synthetic sub-32-bit format** (e.g. real int16, 2 B/sample, payload padded to the next 32-bit word) is trimmed of its padding — this test **cannot** use the fixtures.
- **Format assertion** is verifiable independently of the trim arithmetic (craft a non-float / odd-size format → interpreter rejects it).
- **Gap** (synthetic counters `…3,5…`) → `gap_before=True`; **data-before-context** → `None` + warning, no crash.

---

## Stage 2.5 — Dynamic VITA-49 header parsing (both configs) + buffer flush contract

> **Corrective stage (run after Stages 0–2).** Fixes two issues found by decoding the real recordings. **It supersedes the header/timestamp model in Appendix A §C1–C3** — use the flag-driven model below. **The PDF is correct**: it documents Sceptre's *default* config (`class_id`=0, `trailer`=0 → 20-byte header, timestamps at offsets 8/12). The two recordings were captured with **`class_id` AND `trailer` enabled** (a valid, standard VITA-49 configuration), so their header is **28 bytes** with an 8-byte **Class ID *before* the timestamps**, plus a **4-byte trailer** on data packets. Earlier guidance ("skip 8 bytes *after* the 20-byte header, read timestamps at 8/12") silently read the Class ID as the timestamp and produced a 1970-garbage `start_timestamp`. The interpreter must parse **dynamically from the word0 flags** so one code path handles the recordings *and* live default-config Sceptre.

**Proof from `single_frequency.pkl` (data packet):** `bytes[8:16] = 00fffffa00160000` is identical across all packets → Class ID (OUI `0xfffffa` + class codes). `u32@16 = 1782443209` → `2026-06-26T03:06:49Z` → integer-seconds timestamp. `u64@20` advances by exactly `1,632,000,000` ps/packet (= 1.632 ms = 1020 samples @ 625 kHz) and stays `< 1e12` → fractional-picosecond timestamp.

**Objective.** (1) Replace fixed-offset header parsing in `interpreter.py` with a **flag-driven dynamic parser**: header length and every field offset are computed from the word0 `class_id`/`trailer`/TSI/TSF flags; the Class ID (when present) is skipped *before* the timestamps; the integer/fractional timestamps are read at their true offsets — fixing the garbage `start_timestamp`. The same code must yield a 28-byte header + trailer for the recordings and a 20-byte header + no trailer for default-config live Sceptre, with correct timestamps in both. (2) Lock the buffer **flush contract** so a size/age/gap flush never drops data or opens a time gap: `flush()` must **retain `_current_context`**; data is dropped only at cold start (before the first context packet). Ship a guard test that holds Stage 3 to it.

**Files to create/modify:**
- modify `src/sceptre_pipeline/interpreter.py` — `parse_header`, `decode_context`, `trim_data`, `Header`
- modify `src/sceptre_pipeline/formats.py` — word0 flag masks + a header-length helper / field-size constants
- modify `tests/test_header.py`, `tests/test_context.py`, `tests/test_trim.py` — assert against dynamic offsets (`hdr.header_len`), not hardcoded 20/28
- create `tests/test_dynamic_header.py` — both configs: real recording bytes (C=1,T=1) and synthetic default bytes (C=0,T=0)
- create `tests/test_buffer_flush_contract.py` — flush-retains-context guard, gated by `pytest.importorskip("sceptre_pipeline.buffer")`

**Implementation prompt.** *(Include Appendix A, but note this stage OVERRIDES its §C1–C3 header/timestamp offsets with the flag-driven model here. §C4 field-decode, fixed-point, endianness, and the 1020-sample trim remain in force.)*

*Part A — dynamic header. Rewrite `parse_header(b)` so all offsets are derived from word0; do not hardcode 20 or 28:*
```python
w0      = int.from_bytes(b[:4], "big")
ptype   = (w0 >> 28) & 0xF          # 1 = IF data, 4 = IF context
has_cid = bool((w0 >> 27) & 1)      # bit 4  — Class ID present
has_trl = bool((w0 >> 26) & 1)      # bit 5  — trailer present (data pkts)
tsi     = (w0 >> 22) & 3            # bits 8-9   integer-timestamp mode
tsf     = (w0 >> 20) & 3            # bits 10-11 fractional-timestamp mode
counter = (w0 >> 16) & 0xF
psize   =  w0 & 0xFFFF              # 32-bit words incl. header

has_stream_id = ptype in (1, 3, 4, 5)   # Sceptre uses 1 & 4; both carry a Stream ID
off = 4
if has_stream_id:
    stream_id = int.from_bytes(b[off:off+4], "big"); off += 4
class_id = None
if has_cid:
    class_id = b[off:off+8]; off += 8        # OUI + class codes, BEFORE the timestamps
int_ts = frac_ps = None
if tsi:
    int_ts  = int.from_bytes(b[off:off+4], "big"); off += 4
if tsf:
    frac_ps = int.from_bytes(b[off:off+8], "big"); off += 8
header_len = off                              # 20 (default) or 28 (class_id set)
timestamp  = (int_ts or 0) + (frac_ps or 0) / 1e12
```
*The `Header` dataclass carries `header_len`, `has_trailer`, `class_id`, `int_ts`, `frac_ps`, `timestamp`, plus the existing fields (`packet_type`, `counter`, `packet_size`, `stream_id`). Keep presence flag-/type-driven, never a magic constant.*

*`decode_context(b, hdr)`: the 32-bit CIF now begins at `hdr.header_len` (was `20 + 8`). Everything after it — the descending `FIELD_SIZE` walk, the fixed-point conversions, and the Data-Packet-Payload-Format decode yielding `bytes_per_sample`/`is_complex` — is unchanged.*

*`trim_data(b, hdr, bytes_per_sample)`: `start = hdr.header_len`; `end = len(b) - (4 if hdr.has_trailer else 0)`; `body = b[start:end]`; `num_samples = len(body) // bytes_per_sample`; return `(body[:num_samples*bytes_per_sample], num_samples)`. Cross-check `len(b) == hdr.packet_size * 4` and log a mismatch. Do NOT reintroduce `floor((packet_size*4 - 20)/bps)`.*

*Keep `bytes_per_sample`/`is_complex` sourced dynamically from the context Data-Payload-Format field (already implemented in Stage 2). Leave the "must be complex float32 / 8 bytes" check as a loud assertion, but isolate it so a future RAW/real-float Sceptre stream can relax it without touching the trim math ("dynamic payload").*

*Part B — buffer flush contract (spec + guard). Amend the Stage 3 requirements: `IngestBuffer.flush()` resets `_chunks`, `_num_samples`, `_start_timestamp`, `_first_arrival` and MUST keep `_current_context`. Data is dropped ONLY when `_current_context is None` (cold start, before the first context packet). No flush trigger — size, real-context-change, age, or gap — may set `_current_context = None`. This guarantees that after a size/age/gap flush, the very next data packet is accepted immediately under the retained context and its window is contiguous in sample time (no "waiting for a context packet" gap). Write `tests/test_buffer_flush_contract.py` with `buffer = pytest.importorskip("sceptre_pipeline.buffer")` so it self-activates once Stage 3 lands: force a size-triggered flush, then push a data record and assert (a) it is accumulated, not dropped; (b) a subsequent flush emits it under the still-latched context; (c) its window's `start_timestamp` equals that data packet's own timestamp.*

**Acceptance criteria.**
- **Recording header (C=1, T=1):** `parse_header` on a real data packet → `header_len=28`, `has_trailer=True`, `class_id=00fffffa00160000`, `int_ts=1782443209` (`2026-06-26`), `frac_ps` increasing by `1_632_000_000` per packet; `timestamp`/`start_timestamp` is a real 2026 instant, **not** 1970.
- **Default header (C=0, T=0):** on synthetic bytes with `class_id`/`trailer` cleared and a 20-byte header, `parse_header` → `header_len=20`, `has_trailer=False`, timestamps read at offsets 8/12 — same code path, no special-casing.
- **Context/trim still correct on recordings:** BW=500000.0, RF=97300000.0 → 103700000.0, SR=625000.0, `is_complex=True`, `bytes_per_sample=8`; data trims to exactly **1020** samples, `len(body) % 8 == 0`, no garbage first sample.
- **Offsets are flag-driven:** flipping `class_id` or `trailer` in synthetic bytes shifts `header_len` / `payload_end` accordingly; grep shows no offset hardcoded to `20` or `28` in the parse path.
- **Buffer flush contract:** `test_buffer_flush_contract.py` passes once `buffer.py` exists (and skips cleanly until then) — a post-flush data packet is accepted, emitted under the retained context, with a contiguous `start_timestamp`; no trigger nulls `_current_context`.
- Full suite green: `pytest` (existing Stage 0–2 tests updated to dynamic offsets, all passing).

---

## Stage 2.75 — Harden the interpreter for live bytes (dynamic format, defensive parse, complete CIF0 walk, per-stream routing)

> **Corrective/hardening stage (run after Stage 2.5, before Stage 3).** Stages 2/2.5 decode the two recordings perfectly but lock the pipeline to that *one* configuration in three ways that are liabilities against **live** bytes: `decode_payload_format` **raises** on any format but complex-float32/8-byte; `Interpreter.process` lets any parse exception **propagate** (killing Thread B); and the CIF walk **breaks** on any present bit outside `FIELD_SIZE`. This stage makes the interpreter **degrade gracefully instead of crashing or corrupting**. It **supersedes Appendix A §C4's** loud "assert `data_item_format==0b01110` and `bytes_per_sample==8`" — that becomes a *supported/unsupported classification* (§C4's bit windows, radix, endianness, and the 1020-sample trim remain in force). It also **amends the Stage 3 flush contract** (Part D), because making the sample format dynamic opens two silent-corruption paths in the buffer unless Stage 3 changes in lockstep. Empirical ground truth is unchanged; the recordings still decode byte-identically.
>
> **Multi-stream is native.** The Sceptre format multiplexes multiple streams on one socket (each with its own `stream_id`), so the interpreter keeps **per-stream context** (`_contexts[stream_id]`) and the Stage 3 buffer layer routes **one accumulator per `stream_id`**. A single-stream capture is simply the N=1 case — nothing is dropped for being "the wrong stream." A `max_streams` cap (default 64) bounds memory against spoofed/foreign `stream_id` floods: once the cap is hit, packets bearing a *new* `stream_id` are dropped-and-counted (rate-limited), never silently folded into another stream.
>
> **Honesty note on coverage:** only complex-float32 is empirically confirmed (it's all the recordings contain). The int / unsigned / float64 Data-Item-Format code mappings below are from the VITA-49.2 standard, not yet validated against a real Sceptre capture of those formats. The **safety** property (never crash; flag unknowns unsupported) holds regardless; the exact non-float dtype mappings should be re-confirmed against a real capture before trusting them in production.

**Objective.** Four coordinated changes so the interpreter survives and correctly labels whatever a live SDR (or a noisy socket) actually sends:
(A) turn `decode_payload_format` into a **dynamic classifier** — map the Data Packet Payload Format field to `(is_complex, component numpy dtype, bytes_per_sample, supported)` across the realistic Sceptre range (RAW signed/unsigned int8/16/32, IEEE float32/float64, real or complex-cartesian) and return `supported=False` (never raise) for unsupportable layouts (non-byte-aligned item sizes, link-efficient packing, event/channel tags, complex-polar, VRT/reserved formats);
(B) make `process` **defensive** (catch/count/rate-limit every per-packet failure, never propagate) and hold **per-stream context** (`_contexts[stream_id]`, bounded by `max_streams`); harden `trim_data` against oversized/truncated datagrams;
(C) make the CIF0 walk **complete and stop-safe** (full field table, 0-width bit 31, preserve already-decoded fields on a variable/unknown bit);
(D) **amend the Stage 3 buffer contract** so dynamic formats can't silently corrupt (flush on any change to the sample interpretation; treat an unsupported context as inert).

**Files to create/modify:**
- modify `src/sceptre_pipeline/interpreter.py` — dynamic `decode_payload_format`; defensive `Interpreter.process` (`self.errors`, `self.dropped`, rate-limited logging, **per-stream context** `_contexts[stream_id]` bounded by `max_streams`); robust `decode_context`; harden `trim_data` length handling (keep it pure).
- modify `src/sceptre_pipeline/formats.py` — complete CIF0 `FIELD_SIZE`; `SUPPORTED_ITEM_FORMATS`/`SUPPORTED_ITEM_BITS`→dtype maps; item-format code constants; `VARIABLE_LENGTH_CIF_BITS`/`CIF_ENABLE_BITS`; `CONTEXT_FIELD_CHANGE_BIT = 31`.
- create `tests/test_payload_format.py` — every supported + unsupported format word.
- create `tests/test_process_defensive.py` — enumerated malformed/foreign datagrams: counters increment, `None` returned, nothing raised, log rate-limited.
- create `tests/test_stream_routing.py` — interleaved multi-`stream_id` traffic keeps per-stream context/counters separate; a spoofed-stream flood is bounded by `max_streams`.
- modify `tests/test_context.py` — bit-31-set, unknown-high-bit (preserve-decoded), full CIF0 sizes; both 20B/28B header configs still correct on the recordings.
- create `tests/test_buffer_format_contract.py` — Stage 3 amendments, gated by `pytest.importorskip("sceptre_pipeline.buffer")`.

**Implementation prompt.** *(Include Appendix A. This stage OVERRIDES §C4's loud complex-float32 assertion with a supported/unsupported classification, and OVERRIDES the Stage 3 `flush_fields` default in Part D. §C4's bit windows, the fixed-point radix, the mandatory `astype(float32)`-before-`view(complex64)` endianness order, and the 1020-sample trim remain in force; the Stage 2.5 flag-driven header model is unchanged.)*

*Part A — dynamic `decode_payload_format(u: int) -> dict`. Decode the exact 64-bit windows (bit 63 = MSB): `packing_method=(u>>63)&1`; `rc_type=(u>>61)&0b11` (00 real, 01 complex-cartesian, 10 complex-polar, 11 reserved); `data_item_format=(u>>56)&0x1F`; `event_tag=(u>>52)&0x7`; `channel_tag=(u>>48)&0xF`; `item_packing_bits=((u>>38)&0x3F)+1`; `item_bits=((u>>32)&0x3F)+1` (both stored as value−1). Classify — do NOT raise — with: `supported = packing_method==0 and rc_type in {0b00,0b01} and data_item_format in SUPPORTED_ITEM_FORMATS and item_bits in SUPPORTED_ITEM_BITS and item_packing_bits==item_bits and event_tag==0 and channel_tag==0`. In `formats.py` build the maps: signed fixed-point `0b00000`→`>i1/>i2/>i4`; unsigned fixed-point `0b10000`→`>u1/>u2/>u4`; IEEE single `0b01110`(32)→`>f4`; IEEE double `0b01111`(64)→`>f8` (keep IEEE-half `0b01101` UNSUPPORTED — code assignment uncertain). `is_complex=(rc_type==0b01)`; `component_bytes=item_bits//8`; `bytes_per_sample=(2 if is_complex else 1)*component_bytes`. On `supported=False`, rate-limited-log the offending fields once and return `component_dtype=None, bytes_per_sample=None`. Always return `{is_complex, rc_type, data_item_format, packing_method, item_bits, item_packing_bits, event_tag, channel_tag, component_dtype, bytes_per_sample, supported}`. In `decode_context` at CIF bit 15, copy `supported/component_dtype/bytes_per_sample/is_complex/data_item_format` into the context dict.*

*Part B — defensive `process` + per-stream context + trim hardening. Add `self.errors=0`, `self.dropped=0`, `self._contexts={}` (per-stream context, keyed by `stream_id`), and a rate limiter (log first N per reason, then every Kth) so a foreign-traffic flood can't flood the log. Wrap the whole `process` body in `try/except Exception`: on exception `self.errors+=1`, rate-limited `logger.warning("dropping unparseable datagram (%d bytes): %s", len(raw), exc)` (never full-rate `logger.exception`), return `None`. Keep policy drops (`self.dropped`) distinct from thrown-exception errors (`self.errors`). Per-stream context: replace the single `_current_context` with `self._contexts: dict[int, dict]` keyed by `stream_id` (keep the existing per-stream `_last_counter`). On a context packet, store `self._contexts[stream_id] = ctx`. On a data packet, look up `ctx = self._contexts.get(stream_id)` and decode against THAT stream's context. Bound both maps with `max_streams` (default 64): a packet bearing a `stream_id` not already tracked, once the cap is reached, is dropped-and-counted (rate-limited) so a spoofed-stream flood can't grow memory unbounded. Unsupported/undecodable data: on a data packet, if that stream has no context yet (`ctx is None`, startup) → drop; if `ctx.get("supported") is False` or `bytes_per_sample is None` → drop-and-count only this data packet (its context record was still emitted, so downstream knows the stream is present-but-unsupported). Harden `trim_data` (keep it pure): `end = min(len(b), hdr.packet_size*4) - (TRAILER_SIZE if hdr.has_trailer else 0)` so a datagram longer than `packet_size*4` (concatenated / jumbo frame) never ingests trailing bytes as samples; if `len(b) < hdr.packet_size*4`, raise `ValueError` (caught by the guard as truncation) rather than silently short-reading. Inputs that must survive without raising, each a test: short packet; truncated CIF/field; unsupported-but-valid format; unknown `packet_type∉{1,4}`; foreign/random bytes (a second *valid* `stream_id` is **routed**, not dropped — see `tests/test_stream_routing.py`). Stage-4 note: `Pipeline.run` also wraps `interpreter.process(item)` in its own `try/except: interpreter.errors+=1; continue` as defense-in-depth behind the interpreter's primary guard.*

*Part C — complete, stop-safe CIF0 walk. Extend `FIELD_SIZE` (bytes) to the full CIF0 fixed set: 30 Reference Point Id 4, 29 Bandwidth 8, 28 IF Ref Freq 8, 27 RF Ref Freq 8, 26 RF Ref Freq Offset 8, 25 IF Band Offset 8, 24 Reference Level 4, 23 Gain 4, 22 Over-Range Count 4, 21 Sample Rate 8, 20 Timestamp Adjustment 8, 19 Timestamp Calibration 4, 18 Temperature 4, 17 Device Id 8, 16 State/Event 4, 15 Data Payload Format 8, 14 Formatted GPS 44, 13 Formatted INS 44, 12 ECEF Ephemeris 52, 11 Relative Ephemeris 52, 10 Ephemeris Ref Id 4. Walk bits 31→0: bit 31 (Context Field Change Indicator) is **0-width** → set `ctx["context_field_change"]=True`, advance 0, never abort; a bit in `FIELD_SIZE` → bounds-check `offset+size<=len(b)` (raise → caught as truncation), decode if known, always `offset+=size`; a variable-length bit (9 GPS-ASCII, 8 Context-Association-Lists), a CIF-enable bit (7/3/2/1), or any reserved/unknown bit → `logger.warning("CIF0 bit %d is variable/unknown; stopping walk, %d fields decoded", bit, len(ctx)); break` **without discarding `ctx`** (all higher-bit fields already decoded survive). This replaces the current `FIELD_SIZE.get(bit) is None → break`.*

*Part D — Stage 3 buffer contract amendments (apply when building Stage 3; supersedes Stage 3's `flush_fields` default and its single-buffer model). Route **one `IngestBuffer` per `stream_id`** via a small `BufferRouter` (owns `dict[stream_id, IngestBuffer]`, bounded by `max_streams`) so multiplexed streams never share an accumulator; the rules below are per-stream-buffer. Because Part A makes byte interpretation vary independently of `rf_hz`/`data_item_format`, each buffer MUST flush whenever its **sample interpretation** changes, not just frequency/rate. Change the flush identity to `(rf_hz, sample_rate_hz, component_dtype, is_complex)` — note `component_dtype` is `>i2` for BOTH real and complex int16, so `is_complex` is required, and `bytes_per_sample` is then implied. A real-int16→complex-int16 change (identical `data_item_format`) MUST flush. Unsupported context (`supported is False` / `component_dtype is None`): the buffer adopts it for labeling but stays **inert** — accepts no data, never calls `np.dtype(None)` (which would silently become float64) — until a supported context arrives. Amplitude/precision policy (document + stamp): emitted arrays are funneled to `float32`/`complex64`, so integer formats emerge **un-normalized** (raw counts) and float64 is **downcast** (lossy); put `component_dtype`, `is_complex`, and `bytes_per_sample` into every emitted unit's metadata so downstream can normalize/upcast — normalization itself is a downstream/DSP concern, out of scope here. Counters: `self.errors`/`self.dropped` are single-writer (Thread B); GIL-atomic int reads are fine, no cross-thread lock needed.*

**Deferred (named, not silently dropped):** integer-amplitude normalization by full-scale; splitting a datagram that carries more than one VITA-49 packet. Each is a follow-up; this stage makes them safe-by-drop, not corrupt-by-silence. (Multi-stream routing is now **in scope** — see the per-stream context here and the `BufferRouter` in Stage 3.)

**Existing-code deltas — DO NOT SKIP (Stages 0–2.5 tests encode the OLD fail-loud contract).** *This stage inverts §C4's "raise on non-complex-float32" into a classification, so three built tests in `tests/test_context.py` currently assert the wrong contract and MUST be converted (not preserved by re-adding the raise): `test_format_assertion_rejects_non_float`, `test_format_assertion_rejects_odd_size`, and `test_format_assertion_via_context_packet` — each should now assert `supported is False` (and that `decode_payload_format`/`decode_context`/`process` do **not** raise) instead of expecting a `ValueError`. The `_current_context` → `_contexts[stream_id]` refactor also breaks the `Interpreter.current_context` property and its use in `test_interpreter_adopts_context_and_returns_record`: replace the property with a per-stream accessor (e.g. `context_for(stream_id)`) and update the test. Finally, extend `conftest.encode_payload_format_u64` with `packing_method`/`event_tag`/`channel_tag` params so the unsupported-layout tests (link-efficient, tagged) can be built — the flag-driven `build_context_packet`/`build_data_packet` already take `stream_id`, so multi-stream test packets need no new scaffolding.*

**Acceptance criteria.**
- **Supported formats decode dynamically:** synthetic payload-format words give the right `(is_complex, component_dtype, bytes_per_sample, supported=True)` for real/complex × {int8 `>i1`, int16 `>i2`, int32 `>i4`, float32 `>f4`, float64 `>f8`}, and unsigned int → `>u1/>u2/>u4`.
- **Recorded format unchanged:** `0x2e0007df00000000` → `is_complex=True, component_dtype=">f4", bytes_per_sample=8, supported=True`.
- **Unsupported layouts degrade, never raise:** 12-bit item size, `packing_method=1`, `event_tag>0`/`channel_tag>0`, `item_packing_bits>item_bits`, complex-polar (`rc=0b10`), reserved `rc=0b11`, VRT/reserved format → `supported=False`, `component_dtype=None`, logged once, no exception.
- **Only affected data is dropped:** an unsupported context is still emitted as a context record; its data packets are dropped-and-counted on `self.dropped`; a later supported context re-enables flow. `trim_data` stays pure.
- **`process` never propagates:** short packet, truncated CIF/field, unsupported-but-valid format, unknown packet type, and foreign/random bytes each return `None`, increment `self.errors` (exceptions) or `self.dropped` (policy) appropriately, raise nothing; a stress test of hundreds of random datagrams keeps the interpreter alive with a **bounded log** and **bounded per-stream maps**.
- **Per-stream routing:** interleaved packets on distinct `stream_id`s keep separate per-stream context and counters (never folded into one accumulator); a single-stream capture is the N=1 case with nothing dropped; a spoofed-stream flood beyond `max_streams` is dropped-and-counted with `_contexts`/`_last_counter` bounded.
- **Datagram-length hardening:** an oversized datagram (`len > packet_size*4`, e.g. two concatenated packets) trims to exactly `packet_size*4` (no trailing bytes ingested as samples); an under-length datagram with valid flags is dropped-and-counted, not short-read.
- **CIF0 walk robust:** a context with **bit 31 set** plus bits 29/27/21/15 still decodes all four fields (bit 31 adds 0, records `context_field_change=True`, no abort); a context that sets an **unknown/variable high bit** after known fields **preserves every already-decoded higher-bit field** and stops safely; `FIELD_SIZE` covers the full CIF0 fixed set (bits 30–10).
- **Both header configs still correct on the recordings:** 28-byte+trailer (recordings) and a synthetic 20-byte default both parse; BW=500000, RF=97.3M→103.7M, SR=625000, complex, 8 B/sample, exactly **1020** samples, `len(body)%8==0`.
- **Stage 3 amendments (importorskip-guarded until `buffer.py` exists):** two contexts with identical `data_item_format` but different `is_complex`/`bytes_per_sample` **flush between them**; an unsupported context leaves the buffer **inert** (no `np.dtype(None)`, no data accepted, no Thread-B crash on the next flush); every emitted unit's metadata carries `component_dtype`/`is_complex`/`bytes_per_sample`.
- **No regressions:** grep shows the CIF walk keyed off `hdr.header_len`/`FIELD_SIZE`, payload decode via the dynamic classifier (no `raise ValueError` for unsupported-but-valid formats), `trim_data` free of format assertions. Full `pytest` green (all Stage 0–2.5 tests unchanged + the new ones).

---

## Stage 3 — Ingest buffer + per-stream router (routing, accumulation, four flush triggers, single conversion)

**Objective.** Turn the interpreter's per-packet records into emitted `{samples, context, …}` units. One `IngestBuffer` accumulates a single stream; a thin `BufferRouter` owns one buffer per `stream_id` so multiplexed streams never mix. Flush on the four triggers; convert once. Implement all four correctness gotchas exactly, **plus the Stage 2.75 Part D amendments** (flush on any sample-interpretation change; an unsupported context is inert; stamp format into emitted metadata).

**Files:** create `src/sceptre_pipeline/buffer.py` (`IngestBuffer` + `BufferRouter`), `tests/test_buffer_triggers.py`, `tests/test_buffer_router.py`.

**State (per `IngestBuffer`):** `_chunks: list[bytes]`, `_num_samples`, `_component_dtype/_bytes_per_sample/_is_complex` (latched from the first chunk of a window), `_start_timestamp` (packet timestamp of first chunk), `_first_arrival` (`time.monotonic()`), `_current_context` (separate from accumulation), `_stream_id`, `_inert: bool`.

**Implementation prompt.** *(Include Appendix A + the Stage 2.75 Part D amendments.) `IngestBuffer(emit, max_samples, max_age_s, stream_id, flush_fields=("rf_hz","sample_rate_hz","component_dtype","is_complex"))` — the flush identity includes `component_dtype`+`is_complex` (Part D) so a real→complex or bit-depth change flushes even when `rf_hz`/`data_item_format` are unchanged (`component_dtype` alone is `>i2` for both real and complex int16, so `is_complex` is required).*
- *`push(record)` routing. `type=="context"`: compute a **real diff** over `flush_fields` only (ignore counter/timestamp, which always change); if different **and** `_chunks` non-empty → `flush()` **BEFORE** adopting; then set `_current_context = record["context"]`. If the new context is **unsupported** (`context["supported"] is False` or its `component_dtype is None`), adopt it for labeling but set `_inert = True` — accept no data, never build `np.dtype(None)` — until a supported context clears it. `type=="data"`: if `_current_context is None` or `_inert`, drop; if `record["metadata"]["gap_before"]` and `_chunks` non-empty → `flush()` first; if first chunk of a window, latch `_component_dtype`/`_is_complex`/`_bytes_per_sample` from `record["metadata"]` and `_start_timestamp`/`_first_arrival`; append `record["data"]`; bump `_num_samples`; if `_num_samples >= max_samples` → `flush()`.*
- *`maybe_flush_on_age()`: `if self._chunks and time.monotonic() - self._first_arrival >= max_age_s: self.flush()`.*
- *`flush()`: if `_chunks` empty, return. `raw = b"".join(self._chunks)`; `arr = np.frombuffer(raw, dtype=self._component_dtype).astype(np.float32)`; `if self._is_complex: arr = arr.view(np.complex64)`; `emit({"samples": arr, "context": self._current_context, "start_timestamp": self._start_timestamp, "num_samples": self._num_samples, "stream_id": self._stream_id, "component_dtype": self._component_dtype, "is_complex": self._is_complex, "bytes_per_sample": self._bytes_per_sample})`; reset `_chunks/_num_samples/_start_timestamp/_first_arrival` but **KEEP** `_current_context`. (Amplitude/precision per Part D: integer formats emerge un-normalized and float64 is downcast to float32 — the stamped `component_dtype` lets downstream normalize/upcast.)*
- *`BufferRouter(emit, max_samples, max_age_s, flush_fields, max_streams=64)` owns `dict[stream_id, IngestBuffer]`: `push(record)` reads `sid = record["metadata"]["stream_id"]`, lazily creates a buffer for a new `sid` (dropping-and-counting new streams past `max_streams`), and forwards; `maybe_flush_on_age()` calls it on **every** buffer; `flush_all()` drains all buffers on shutdown.*

**The four gotchas — each must appear in a test (per-buffer):**
1. **Context-change ordering:** flush (labeled OLD context) → adopt new → new data accrues under new context.
2. **Periodic context is not a change:** identical heartbeat over `flush_fields` must NOT flush.
3. **Age trigger is polled, not reactive** (via `maybe_flush_on_age()` in the dequeue loop).
4. **Counter gap breaks contiguity:** a mod-16 gap flushes before appending, so every emitted array is gapless.

**Acceptance criteria.**
- **Periodic identical context** (`single_frequency`, 6 identical context packets) → **zero** context-triggered flushes.
- **Freq change** (`change_frequency`) → the boundary-spanning array is labeled **RF=97.3 MHz (old)**; the next window is labeled 103.7 MHz.
- **Format change flushes (Part D):** two contexts with equal `data_item_format` but different `is_complex`/`bytes_per_sample` (e.g. real-int16 → complex-int16) flush between them; no mixed-dtype window is ever converted.
- **Unsupported context is inert (Part D):** a `supported=False` context is adopted for labeling but accepts no data and never calls `np.dtype(None)`; a later supported context clears `_inert` and resumes flow with no Thread-B crash.
- **Size trigger** flushes at exactly `max_samples`; **age trigger** fires with a monkeypatched `monotonic` and NO new packets; **gap** flushes at the boundary.
- **Per-stream routing:** interleaved records on two `stream_id`s land in two separate buffers, each labeled/flushed independently; every emitted unit carries `stream_id`, `component_dtype`, `is_complex`, `bytes_per_sample`; new streams past `max_streams` are dropped-and-counted.
- **Single conversion correctness:** emitted array is native-endian `complex64`, `abs() < ~16`, no NaN (proves astype-before-view). Include a note/test that view-before-astype produces NaN.

---

## Stage 4 — Runtime wiring + demo consumer

**Objective.** A `Pipeline` that starts Thread A and runs the Thread B loop (dequeue + `SHUTDOWN` handling + age poll + interpret + push), delivering emitted units to an `on_emit` callback; plus a `python -m sceptre_pipeline` CLI for replay and live, with a demo consumer. Clean startup/shutdown.

**Files:** create `src/sceptre_pipeline/runtime.py`, `__main__.py`, `tests/test_runtime.py`.

**Implementation prompt.** *(Include Appendix A.) `Pipeline(source, raw_queue, interpreter, router, on_emit, poll_interval=0.1)` where `router` is the Stage 3 `BufferRouter`. `run()`: start the source thread, then loop `while not stop.is_set()`: `item = raw_queue.get(timeout=poll_interval)`; if `item is SHUTDOWN` → `router.flush_all(); break`; if `item is None` → `router.maybe_flush_on_age(); continue`; else `record = interpreter.process(item)` **wrapped in its own `try/except` (Stage 2.75 defense-in-depth: `interpreter.errors += 1; continue`)**; `if record: router.push(record)`; then `router.maybe_flush_on_age()`. `stop()`: set the `Event`, join threads with a timeout, log the final `raw_queue.dropped` and `interpreter.errors`/`interpreter.dropped` counts. In `__main__.py`, an argparse CLI wires `BoundedRawQueue`, `Interpreter`, `BufferRouter(on_emit=demo, max_samples, max_age_s, max_streams)`, and either `ReplaySource(--replay PATH [--pace])` or `LiveSource(--live --host --port [--record])`. The demo `on_emit` prints `stream_id`, `num_samples`, `samples.shape`, `samples.dtype`, `context["rf_hz"]`, `context["sample_rate_hz"]`, and `start_timestamp`.*

**Use a distinct `SHUTDOWN` object (not `None`)** so "timeout → poll age" and "shutdown → drain+exit" never get confused; ensure `ReplaySource` enqueues it at EOF and the loop **flushes the final partial window of every stream** (via `router.flush_all()`) before exiting.

**Acceptance criteria.**
- `python -m sceptre_pipeline --replay recordings/single_frequency.pkl` runs end-to-end, emits ≈ 3066 × 1020 samples across windows (minus a final partial), and exits cleanly (threads join; final window flushed).
- `--replay recordings/change_frequency.pkl` shows two RF-labeled segments (97.3 then 103.7 MHz).
- Integration test asserts total emitted sample count (within the final-partial tolerance) and clean shutdown with no hang.
- Optional live smoke test: a loopback UDP sender feeds `LiveSource`; the pipeline emits and stops on the `Event`.

---

## Cross-cutting correctness checklist (repeat in every relevant stage prompt + its acceptance criteria)

- **C1/C2/C3:** derive payload bounds from the `class_id`/`trailer` flags; `num_samples` from trimmed body length (1020), not the PDF formula (1021); skip the 8-byte id-word.
- **C4:** gain/GPS/ephemeris are synthetic-test-only; the "context decode asserts fixed-point" test asserts BW/RF/SR/DataFormat against real data only.
- **Endianness:** `astype(float32)` (byteswap) **before** `view(complex64)`; keep `bytes_per_sample` (8) vs component dtype `">f4"` (4) distinct.
- **FIELD_SIZE walk:** advance past every present CIF bit, decode only known ones.
- **Sentinel vs timeout:** `SHUTDOWN = object()` ≠ `None`.
- **Final flush** on EOF/shutdown; **age uses `time.monotonic()`**; packet timestamp is authoritative for sample time only.
- **Context ordering + heartbeat** checked together: flush-before-adopt fires only on a real `flush_fields` diff.

---

## Verification (end-to-end)

1. `pip install -e .[dev]` then `pytest` — all stage tests green (header/context/trim/queue/buffer/runtime), including real-recording assertions and synthetic-bytes tests.
2. `python -m sceptre_pipeline --replay recordings/single_frequency.pkl` — observe steady-state emits at RF 97.3 MHz / SR 625 kHz; total ≈ 3,127,320 samples; clean exit.
3. `python -m sceptre_pipeline --replay recordings/change_frequency.pkl` — observe the 97.3 → 103.7 MHz transition with the old-context-labeled boundary window.
4. `python receiver/recieve_udp.py --duration 2` (or loopback sender) — confirm captures land in `recordings/` and reload; confirm `LiveSource` fan-out records while the live path stays lossy/bounded.