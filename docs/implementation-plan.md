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

## Stage 3 — Ingest buffer (routing, accumulation, four flush triggers, single conversion)

**Objective.** Route per-packet records into accumulated byte-chunks + a context dict; flush on four triggers; convert once. Implement all four correctness gotchas exactly.

**Files:** create `src/sceptre_pipeline/buffer.py`, `tests/test_buffer_triggers.py`.

**State:** `_chunks: list[bytes]`, `_num_samples`, `_dtype/_bytes_per_sample/_is_complex` (latched from first chunk of a window), `_start_timestamp` (packet timestamp of first chunk), `_first_arrival` (`time.monotonic()`), `_current_context` (separate from accumulation).

**Implementation prompt.** *(Include Appendix A.) `IngestBuffer(emit, max_samples, max_age_s, flush_fields=("rf_hz","sample_rate_hz","data_item_format"))`.*
- *`push(record)` routing. `type=="context"`: compute a **real diff** over `flush_fields` only (ignore counter/timestamp, which always change); if different **and** `_chunks` non-empty → `flush()` **BEFORE** adopting; then set `_current_context = record["context"]`. A data-format change is a hard flush. `type=="data"`: if `_current_context is None`, drop; if `record.metadata["gap_before"]` and `_chunks` non-empty → `flush()` first; if first chunk of window, latch dtype/`_start_timestamp`/`_first_arrival`; append `record["data"]`; bump `_num_samples`; if `_num_samples >= max_samples` → `flush()`.*
- *`maybe_flush_on_age()`: `if self._chunks and time.monotonic() - self._first_arrival >= max_age_s: self.flush()`.*
- *`flush()`: if `_chunks` empty, return. `raw = b"".join(self._chunks)`; `arr = np.frombuffer(raw, dtype=self._dtype).astype(np.float32)`; `if self._is_complex: arr = arr.view(np.complex64)`; `emit({"samples": arr, "context": self._current_context, "start_timestamp": self._start_timestamp, "num_samples": self._num_samples})`; reset `_chunks/_num_samples/_start_timestamp/_first_arrival` but **KEEP** `_current_context`.*

**The four gotchas — each must appear in a test:**
1. **Context-change ordering:** flush (labeled OLD context) → adopt new → new data accrues under new context.
2. **Periodic context is not a change:** identical heartbeat over `flush_fields` must NOT flush.
3. **Age trigger is polled, not reactive** (via `maybe_flush_on_age()` in the dequeue loop).
4. **Counter gap breaks contiguity:** a mod-16 gap flushes before appending, so every emitted array is gapless.

**Acceptance criteria.**
- **Periodic identical context** (`single_frequency`, 6 identical context packets) → **zero** context-triggered flushes.
- **Freq change** (`change_frequency`) → the boundary-spanning array is labeled **RF=97.3 MHz (old)**; the next window is labeled 103.7 MHz.
- **Size trigger** flushes at exactly `max_samples`; **age trigger** fires with a monkeypatched `monotonic` and NO new packets; **gap** flushes at the boundary.
- **Single conversion correctness:** emitted array is native-endian `complex64`, `abs() < ~16`, no NaN (proves astype-before-view). Include a note/test that view-before-astype produces NaN.

---

## Stage 4 — Runtime wiring + demo consumer

**Objective.** A `Pipeline` that starts Thread A and runs the Thread B loop (dequeue + `SHUTDOWN` handling + age poll + interpret + push), delivering emitted units to an `on_emit` callback; plus a `python -m sceptre_pipeline` CLI for replay and live, with a demo consumer. Clean startup/shutdown.

**Files:** create `src/sceptre_pipeline/runtime.py`, `__main__.py`, `tests/test_runtime.py`.

**Implementation prompt.** *(Include Appendix A.) `Pipeline(source, raw_queue, interpreter, buffer, on_emit, poll_interval=0.1)`. `run()`: start the source thread, then loop `while not stop.is_set()`: `item = raw_queue.get(timeout=poll_interval)`; if `item is SHUTDOWN` → `buffer.flush(); break`; if `item is None` → `buffer.maybe_flush_on_age(); continue`; else `record = interpreter.process(item)`; `if record: buffer.push(record)`; then `buffer.maybe_flush_on_age()`. `stop()`: set the `Event`, join threads with a timeout, log the final `raw_queue.dropped` count. In `__main__.py`, an argparse CLI wires `BoundedRawQueue`, `Interpreter`, `IngestBuffer(on_emit=demo, max_samples, max_age_s)`, and either `ReplaySource(--replay PATH [--pace])` or `LiveSource(--live --host --port [--record])`. The demo `on_emit` prints `num_samples`, `samples.shape`, `samples.dtype`, `context["rf_hz"]`, `context["sample_rate_hz"]`, and `start_timestamp`.*

**Use a distinct `SHUTDOWN` object (not `None`)** so "timeout → poll age" and "shutdown → drain+exit" never get confused; ensure `ReplaySource` enqueues it at EOF and the loop **flushes the final partial window** before exiting.

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
5. Ground truth: the two recordings are authoritative; any decode disagreeing with BW=500k / RF=97.3M→103.7M / SR=625k / 1020 samples-per-packet is a bug.
