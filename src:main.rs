use anyhow::{Context, Result};
use arrow::array::{Date32Builder, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};
use serde::Deserialize;
use std::cmp::Ordering;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Arc;

const MS_PER_SEC: i64 = 1_000;
const MS_PER_MIN: i64 = 60 * MS_PER_SEC;
const MS_PER_HOUR: i64 = 60 * MS_PER_MIN;
const MS_PER_DAY: i64 = 24 * MS_PER_HOUR;
const SESSION_GAP_MS: i64 = 30 * MS_PER_MIN; // 1,800,000 ms
const ATTRIBUTION_WINDOW_MS: i64 = 7 * MS_PER_DAY; // 604,800,000 ms
const LATE_EVENT_LIMIT_MS: i64 = 24 * MS_PER_HOUR; // 86,400,000 ms

#[derive(Deserialize)]
struct Manifest {
    window_start: String,
    window_end: String,
}

// ============================================================================
// Strict CSV Line Splitter (`\n` is the ONLY line terminator; `\r` is data)
// ============================================================================

/// Splits a single line (without trailing `\n`) into up to `N` unquoted field slices.
/// Returns `None` if malformed:
/// - empty line
/// - wrong field count
/// - quote anywhere except wrapping an entire field (`"..."`)
/// - internal quotes (rules state: "No value ever holds a comma, a quote or a newline, quoted or not")
fn parse_csv_line<const N: usize>(mut line: &[u8]) -> Option<[&[u8]; N]> {
    if line.is_empty() {
        return None;
    }

    let mut out = [&[][..]; N];
    let mut idx = 0;

    loop {
        if idx >= N {
            return None;
        }

        let (field, rest) = match memchr_comma(line) {
            Some(pos) => (&line[..pos], Some(&line[pos + 1..])),
            None => (line, None),
        };

        // Check quoting rules on `field`
        if field.len() >= 2 && field[0] == b'"' && field[field.len() - 1] == b'"' {
            let inner = &field[1..field.len() - 1];
            if inner.contains(&b'"') {
                return None;
            }
            out[idx] = inner;
        } else {
            if field.contains(&b'"') {
                return None;
            }
            out[idx] = field;
        }

        idx += 1;
        match rest {
            Some(r) => line = r,
            None => break,
        }
    }

    if idx != N {
        return None;
    }
    Some(out)
}

#[inline(always)]
fn memchr_comma(s: &[u8]) -> Option<usize> {
    s.iter().position(|&b| b == b',')
}

/// Reads lines delimited strictly by `\n` (NOT `\r\n`, so `\r` stays attached to the last field).
fn for_each_lf_line<F>(path: &Path, mut cb: F) -> Result<()>
where
    F: FnMut(usize, &[u8]),
{
    let file = File::open(path).with_context(|| format!("opening {:?}", path))?;
    let mut reader = BufReader::with_capacity(256 * 1024, file);
    let mut buf = Vec::with_capacity(4096);
    let mut line_num = 0usize;

    loop {
        buf.clear();
        let bytes_read = reader.read_until(b'\n', &mut buf)?;
        if bytes_read == 0 {
            break;
        }
        if buf.last() == Some(&b'\n') {
            buf.pop();
        }
        cb(line_num, &buf);
        line_num += 1;
    }
    Ok(())
}

// ============================================================================
// Strict Timestamp & Number Parsers
// ============================================================================

#[inline(always)]
fn parse_2_digits(b: &[u8]) -> Option<i32> {
    if b.len() < 2 || !b[0].is_ascii_digit() || !b[1].is_ascii_digit() {
        return None;
    }
    Some(((b[0] - b'0') * 10 + (b[1] - b'0')) as i32)
}

#[inline(always)]
fn parse_4_digits(b: &[u8]) -> Option<i32> {
    if b.len() < 4
        || !b[0].is_ascii_digit()
        || !b[1].is_ascii_digit()
        || !b[2].is_ascii_digit()
        || !b[3].is_ascii_digit()
    {
        return None;
    }
    Some(
        ((b[0] - b'0') as i32) * 1000
            + ((b[1] - b'0') as i32) * 100
            + ((b[2] - b'0') as i32) * 10
            + ((b[3] - b'0') as i32),
    )
}

#[inline(always)]
fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

#[inline(always)]
fn days_in_month(year: i32, month: i32) -> i32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap_year(year) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

/// Howard Hinnant's civil-from-days / days-from-civil algorithm (days since 1970-01-01).
#[inline(always)]
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = y - if m <= 2 { 1 } else { 0 };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400; // [0, 399]
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146_097 + doe - 719_468
}

/// Parses F1 (`YYYY-MM-DDTHH:MM:SSZ`), F2 (`YYYY-MM-DDTHH:MM:SS+HH:MM` / `-HH:MM`),
/// or F3 (ASCII digits only, epoch ms).
/// Returns `Some(epoch_ms)` if syntactically and calendar-valid, `None` if malformed.
fn parse_timestamp_ms(raw: &[u8]) -> Option<i64> {
    if raw.is_empty() {
        return None;
    }

    // F3: digits only
    if raw.iter().all(|b| b.is_ascii_digit()) {
        // Long strings (> i64::MAX) are valid F3 syntax, just outside any window.
        // Saturate to i64::MAX so window comparison cleanly rejects them.
        let mut acc: i128 = 0;
        for &b in raw {
            acc = acc * 10 + (b - b'0') as i128;
            if acc > i64::MAX as i128 {
                return Some(i64::MAX);
            }
        }
        return Some(acc as i64);
    }

    // F1 (len 20) or F2 (len 25): YYYY-MM-DDTHH:MM:SS...
    if raw.len() != 20 && raw.len() != 25 {
        return None;
    }
    if raw[4] != b'-'
        || raw[7] != b'-'
        || raw[10] != b'T'
        || raw[13] != b':'
        || raw[16] != b':'
    {
        return None;
    }

    let year = parse_4_digits(&raw[0..4])?;
    let month = parse_2_digits(&raw[5..7])?;
    let day = parse_2_digits(&raw[8..10])?;
    let hour = parse_2_digits(&raw[11..13])?;
    let minute = parse_2_digits(&raw[14..16])?;
    let second = parse_2_digits(&raw[17..19])?;

    if !(1..=12).contains(&month) {
        return None;
    }
    let max_day = days_in_month(year, month);
    if !(1..=max_day).contains(&day) {
        return None;
    }
    if !(0..=23).contains(&hour) || !(0..=59).contains(&minute) || !(0..=59).contains(&second) {
        return None;
    }

    let offset_ms: i64 = if raw.len() == 20 {
        if raw[19] != b'Z' {
            return None;
        }
        0
    } else {
        let sign: i64 = match raw[19] {
            b'+' => 1,
            b'-' => -1,
            _ => return None,
        };
        if raw[22] != b':' {
            return None;
        }
        let off_h = parse_2_digits(&raw[20..22])?;
        let off_m = parse_2_digits(&raw[23..25])?;
        if !(0..=23).contains(&off_h) || !(0..=59).contains(&off_m) {
            return None;
        }
        sign * ((off_h as i64) * MS_PER_HOUR + (off_m as i64) * MS_PER_MIN)
    };

    let days = days_from_civil(year as i64, month as i64, day as i64);
    let local_ms = days
        .checked_mul(MS_PER_DAY)?
        .checked_add((hour as i64) * MS_PER_HOUR)?
        .checked_add((minute as i64) * MS_PER_MIN)?
        .checked_add((second as i64) * MS_PER_SEC)?;

    // Subtract offset to get UTC
    local_ms.checked_sub(offset_ms)
}

/// Parses purchase price_cents: ASCII digits only, value in 1..=10,000,000.
#[inline(always)]
fn parse_price_cents(raw: &[u8]) -> Option<i64> {
    if raw.is_empty() || !raw.iter().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let mut val: i64 = 0;
    for &b in raw {
        val = val.saturating_mul(10).saturating_add((b - b'0') as i64);
        if val > 10_000_000 {
            return None;
        }
    }
    if val >= 1 {
        Some(val)
    } else {
        None
    }
}

// ============================================================================
// Connected Components (Disjoint Set Union) for Linked IDs
// ============================================================================

struct UserDsu {
    id_to_idx: FxHashMap<Box<[u8]>, u32>,
    idx_to_id: Vec<Box<[u8]>>,
    parent: Vec<u32>,
    min_id_idx: Vec<u32>,
}

impl UserDsu {
    fn new() -> Self {
        Self {
            id_to_idx: FxHashMap::default(),
            idx_to_id: Vec::new(),
            parent: Vec::new(),
            min_id_idx: Vec::new(),
        }
    }

    #[inline]
    fn get_or_insert(&mut self, id: &[u8]) -> u32 {
        if let Some(&idx) = self.id_to_idx.get(id) {
            return idx;
        }
        let idx = self.idx_to_id.len() as u32;
        let boxed: Box<[u8]> = id.into();
        self.id_to_idx.insert(boxed.clone(), idx);
        self.idx_to_id.push(boxed);
        self.parent.push(idx);
        self.min_id_idx.push(idx);
        idx
    }

    fn find(&mut self, mut x: u32) -> u32 {
        let mut root = x;
        while self.parent[root as usize] != root {
            root = self.parent[root as usize];
        }
        while self.parent[x as usize] != root {
            let next = self.parent[x as usize];
            self.parent[x as usize] = root;
            x = next;
        }
        root
    }

    fn union(&mut self, a: &[u8], b: &[u8]) {
        let ia = self.get_or_insert(a);
        let ib = self.get_or_insert(b);
        let ra = self.find(ia);
        let rb = self.find(ib);
        if ra == rb {
            return;
        }
        let min_a = self.min_id_idx[ra as usize];
        let min_b = self.min_id_idx[rb as usize];
        let best_min = if self.idx_to_id[min_a as usize].as_ref()
            <= self.idx_to_id[min_b as usize].as_ref()
        {
            min_a
        } else {
            min_b
        };
        self.parent[rb as usize] = ra;
        self.min_id_idx[ra as usize] = best_min;
    }

    /// Freezes DSU into an immutable lookup table:
    /// maps any linked `user_id` -> canonical person `user_id` (byte-wise smallest in component).
    fn finalize(mut self) -> FxHashMap<Box<[u8]>, Arc<str>> {
        let n = self.idx_to_id.len();
        let mut root_to_canonical: FxHashMap<u32, Arc<str>> = FxHashMap::default();
        let mut resolved = FxHashMap::with_capacity_and_hasher(n, Default::default());

        for i in 0..n {
            let root = self.find(i as u32);
            let canonical = root_to_canonical.entry(root).or_insert_with(|| {
                let min_idx = self.min_id_idx[root as usize] as usize;
                let s = String::from_utf8_lossy(&self.idx_to_id[min_idx]);
                Arc::from(s.as_ref())
            });
            resolved.insert(self.idx_to_id[i].clone(), Arc::clone(canonical));
        }
        resolved
    }
}

// ============================================================================
// Data Structures for Events & Clicks
// ============================================================================

#[derive(Clone, Copy, PartialEq, Eq)]
enum EventKind {
    View,
    Cart,
    Purchase,
    Invalid,
}

#[derive(Clone)]
struct RawEvent {
    ingest_ts: i64,
    ts: Option<i64>,
    user_id: Box<[u8]>,
    kind: EventKind,
    product_id: Box<[u8]>,
    price_cents: i64, // -1 if invalid for purchase
}

#[derive(Clone)]
struct RawClick {
    user_id: Box<[u8]>,
    campaign_id: Box<[u8]>,
    ts: i64,
}

#[derive(Clone)]
struct ValidEvent {
    ts: i64,
    read_order: u64,
    is_purchase: bool,
    category_idx: u32,
    price_cents: i64,
}

#[derive(Clone)]
struct ValidClick {
    click_id: Box<[u8]>,
    campaign_idx: u32,
    ts: i64,
}

#[derive(Default, Clone)]
struct UserDayAgg {
    sessions: i64,
    events: i64,
    revenue_cents: i64,
    cat_revenue: FxHashMap<u32, i64>,
}

#[derive(Default, Clone, Copy)]
struct CampaignDayAgg {
    sessions: i64,
    purchasing_sessions: i64,
    revenue_cents: i64,
    assisted_sessions: i64,
}

// ============================================================================
// File Discovery Helper
// ============================================================================

fn list_csv_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    if !dir.exists() {
        return Ok(files);
    }
    for entry in fs::read_dir(dir).with_context(|| format!("reading dir {:?}", dir))? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            if let Some(ext) = path.extension() {
                if ext == "csv" {
                    files.push(path);
                }
            }
        }
    }
    files.sort();
    Ok(files)
}

// ============================================================================
// Main Pipeline
// ============================================================================

fn main() -> Result<()> {
    let data_dir = Path::new("/data");
    let output_dir = Path::new("/output");

    // 1. Read manifest.json
    let manifest_bytes = fs::read(data_dir.join("manifest.json"))?;
    let manifest: Manifest = serde_json::from_slice(&manifest_bytes)?;
    let window_start_ms =
        parse_timestamp_ms(manifest.window_start.as_bytes()).context("invalid window_start")?;
    let window_end_ms =
        parse_timestamp_ms(manifest.window_end.as_bytes()).context("invalid window_end")?;
    let click_window_start_ms = window_start_ms - ATTRIBUTION_WINDOW_MS;

    // 2. Load products.csv (assign compact integer IDs to categories, plus "unknown")
    let mut categories: Vec<Arc<str>> = Vec::new();
    let mut cat_name_to_idx: FxHashMap<Box<[u8]>, u32> = FxHashMap::default();
    let mut product_to_cat: FxHashMap<Box<[u8]>, u32> = FxHashMap::default();

    let unknown_idx = 0u32;
    categories.push(Arc::from("unknown"));
    cat_name_to_idx.insert(b"unknown".to_vec().into_boxed_slice(), unknown_idx);

    for_each_lf_line(&data_dir.join("products.csv"), |line_num, line| {
        if line_num == 0 {
            return;
        }
        if let Some([prod_id, cat]) = parse_csv_line::<2>(line) {
            if !prod_id.is_empty() && !cat.is_empty() {
                let cat_idx = if let Some(&idx) = cat_name_to_idx.get(cat) {
                    idx
                } else {
                    let idx = categories.len() as u32;
                    let s = String::from_utf8_lossy(cat);
                    categories.push(Arc::from(s.as_ref()));
                    cat_name_to_idx.insert(cat.into(), idx);
                    idx
                };
                product_to_cat.insert(prod_id.into(), cat_idx);
            }
        }
    })?;

    // 3. Load campaigns.csv
    let mut campaign_ids: Vec<Arc<str>> = Vec::new();
    let mut campaign_channels: Vec<Arc<str>> = Vec::new();
    let mut campaign_to_idx: FxHashMap<Box<[u8]>, u32> = FxHashMap::default();

    let organic_idx = 0u32;
    campaign_ids.push(Arc::from("organic"));
    campaign_channels.push(Arc::from("organic"));

    for_each_lf_line(&data_dir.join("campaigns.csv"), |line_num, line| {
        if line_num == 0 {
            return;
        }
        if let Some([camp_id, channel]) = parse_csv_line::<2>(line) {
            if !camp_id.is_empty() && !channel.is_empty() {
                let idx = campaign_ids.len() as u32;
                campaign_ids.push(Arc::from(String::from_utf8_lossy(camp_id).as_ref()));
                campaign_channels.push(Arc::from(String::from_utf8_lossy(channel).as_ref()));
                campaign_to_idx.insert(camp_id.into(), idx);
            }
        }
    })?;

    // 4. Parse links/*.csv and build Connected Components
    let link_files = list_csv_files(&data_dir.join("links"))?;
    let mut dsu = UserDsu::new();
    for path in &link_files {
        for_each_lf_line(path, |line_num, line| {
            if line_num == 0 {
                return;
            }
            if let Some([u_a, u_b]) = parse_csv_line::<2>(line) {
                if !u_a.is_empty() && !u_b.is_empty() {
                    dsu.union(u_a, u_b);
                }
            }
        })?;
    }
    let linked_user_map = dsu.finalize();

    // Helper closure to resolve any user_id bytes to canonical person string
    let resolve_person = |uid: &[u8]| -> Arc<str> {
        if let Some(canon) = linked_user_map.get(uid) {
            Arc::clone(canon)
        } else {
            Arc::from(String::from_utf8_lossy(uid).as_ref())
        }
    };

    // 5. Parallel parse & per-file dedup of events/*.csv
    // Columns: event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents
    let event_files = list_csv_files(&data_dir.join("events"))?;
    let per_file_events: Vec<FxHashMap<Box<[u8]>, (u64, RawEvent)>> = event_files
        .par_iter()
        .enumerate()
        .map(|(file_idx, path)| -> Result<FxHashMap<Box<[u8]>, (u64, RawEvent)>> {
            let mut local_map: FxHashMap<Box<[u8]>, (u64, RawEvent)> = FxHashMap::default();
            let base_order = (file_idx as u64) << 40;

            for_each_lf_line(path, |line_num, line| {
                if line_num == 0 {
                    return;
                }
                let Some([event_id, user_id, ts_raw, ingest_raw, ev_type, prod_id, price_raw]) =
                    parse_csv_line::<7>(line)
                else {
                    return;
                };
                if event_id.is_empty() {
                    return;
                }
                let Some(ingest_ts) = parse_timestamp_ms(ingest_raw) else {
                    return;
                };

                // Check if we already have an earlier or equal ingest_ts in this file
                if let Some((prev_order, existing)) = local_map.get(event_id) {
                    if existing.ingest_ts < ingest_ts {
                        return;
                    }
                    if existing.ingest_ts == ingest_ts
                        && *prev_order <= base_order + (line_num as u64)
                    {
                        return;
                    }
                }

                let kind = match ev_type {
                    b"view" => EventKind::View,
                    b"cart" => EventKind::Cart,
                    b"purchase" => EventKind::Purchase,
                    _ => EventKind::Invalid,
                };
                let price_cents = if kind == EventKind::Purchase {
                    parse_price_cents(price_raw).unwrap_or(-1)
                } else {
                    0
                };

                let raw_ev = RawEvent {
                    ingest_ts,
                    ts: parse_timestamp_ms(ts_raw),
                    user_id: user_id.into(),
                    kind,
                    product_id: prod_id.into(),
                    price_cents,
                };
                local_map.insert(event_id.into(), (base_order + (line_num as u64), raw_ev));
            })?;

            Ok(local_map)
        })
        .collect::<Result<Vec<_>>>()?;

    // Merge event maps across files: smallest ingest_ts wins BEFORE validation!
    let mut global_events: FxHashMap<Box<[u8]>, (u64, RawEvent)> = FxHashMap::default();
    for file_map in per_file_events {
        for (ev_id, (order, ev)) in file_map {
            match global_events.get_mut(&ev_id) {
                Some((existing_order, existing_ev)) => {
                    if ev.ingest_ts < existing_ev.ingest_ts
                        || (ev.ingest_ts == existing_ev.ingest_ts && order < *existing_order)
                    {
                        *existing_order = order;
                        *existing_ev = ev;
                    }
                }
                None => {
                    global_events.insert(ev_id, (order, ev));
                }
            }
        }
    }

    // Validate kept events, drop late events (> 24h), and group by canonical person
    let mut person_events: FxHashMap<Arc<str>, Vec<ValidEvent>> = FxHashMap::default();
    for (_ev_id, (read_order, ev)) in global_events {
        if ev.user_id.is_empty() || ev.product_id.is_empty() {
            continue;
        }
        if ev.kind == EventKind::Invalid {
            continue;
        }
        if ev.kind == EventKind::Purchase && ev.price_cents < 1 {
            continue;
        }
        let Some(ts) = ev.ts else {
            continue;
        };
        if ts < window_start_ms || ts >= window_end_ms {
            continue;
        }
        // Drop late events: ingest_ts - ts > 24 hours
        if ev.ingest_ts - ts > LATE_EVENT_LIMIT_MS {
            continue;
        }

        let cat_idx = *product_to_cat.get(&ev.product_id).unwrap_or(&unknown_idx);
        let person = resolve_person(&ev.user_id);

        person_events.entry(person).or_default().push(ValidEvent {
            ts,
            read_order,
            is_purchase: ev.kind == EventKind::Purchase,
            category_idx: cat_idx,
            price_cents: ev.price_cents,
        });
    }

    // 6. Parallel parse & dedup of clicks/*.csv
    // Columns: click_id,user_id,campaign_id,ts
    let click_files = list_csv_files(&data_dir.join("clicks"))?;
    let per_file_clicks: Vec<FxHashMap<Box<[u8]>, RawClick>> = click_files
        .par_iter()
        .map(|path| -> Result<FxHashMap<Box<[u8]>, RawClick>> {
            let mut local_map: FxHashMap<Box<[u8]>, RawClick> = FxHashMap::default();
            for_each_lf_line(path, |line_num, line| {
                if line_num == 0 {
                    return;
                }
                let Some([click_id, user_id, campaign_id, ts_raw]) = parse_csv_line::<4>(line)
                else {
                    return;
                };
                if click_id.is_empty() || local_map.contains_key(click_id) {
                    return;
                }
                // Parse ts now (or keep invalid marker; copies of a click are identical)
                let ts = parse_timestamp_ms(ts_raw).unwrap_or(i64::MIN);
                local_map.insert(
                    click_id.into(),
                    RawClick {
                        user_id: user_id.into(),
                        campaign_id: campaign_id.into(),
                        ts,
                    },
                );
            })?;
            Ok(local_map)
        })
        .collect::<Result<Vec<_>>>()?;

    let mut global_clicks: FxHashMap<Box<[u8]>, RawClick> = FxHashMap::default();
    for file_map in per_file_clicks {
        for (cid, click) in file_map {
            global_clicks.entry(cid).or_insert(click);
        }
    }

    // Validate clicks and group by canonical person (only needed for people who have events)
    let mut person_clicks: FxHashMap<Arc<str>, Vec<ValidClick>> = FxHashMap::default();
    for (click_id, click) in global_clicks {
        if click.user_id.is_empty() {
            continue;
        }
        if click.ts < click_window_start_ms || click.ts >= window_end_ms {
            continue;
        }
        let Some(&camp_idx) = campaign_to_idx.get(&click.campaign_id) else {
            continue;
        };
        let person = resolve_person(&click.user_id);
        if person_events.contains_key(&person) {
            person_clicks.entry(person).or_default().push(ValidClick {
                click_id,
                campaign_idx: camp_idx,
                ts: click.ts,
            });
        }
    }

    // 7. Parallel Sessionization, Attribution, Assists, and Aggregation per Person
    let people_work: Vec<(Arc<str>, Vec<ValidEvent>, Vec<ValidClick>)> = person_events
        .into_iter()
        .map(|(person, evs)| {
            let cls = person_clicks.remove(&person).unwrap_or_default();
            (person, evs, cls)
        })
        .collect();

    let per_person_results: Vec<(
        Arc<str>,
        FxHashMap<i32, UserDayAgg>,
        FxHashMap<(u32, i32), CampaignDayAgg>,
    )> = people_work
        .into_par_iter()
        .map(|(person, mut evs, mut cls)| {
            // Sort events by ts ascending, preserving read order on ties
            evs.sort_by(|a, b| a.ts.cmp(&b.ts).then_with(|| a.read_order.cmp(&b.read_order)));

            // Sort clicks by ts ascending, then click_id descending so the LAST item at a given ts
            // has the byte-wise smallest click_id!
            cls.sort_by(|a, b| {
                a.ts.cmp(&b.ts)
                    .then_with(|| b.click_id.cmp(&a.click_id))
            });

            let mut user_days: FxHashMap<i32, UserDayAgg> = FxHashMap::default();
            let mut camp_days: FxHashMap<(u32, i32), CampaignDayAgg> = FxHashMap::default();

            let mut left_click_ptr = 0usize;
            let mut right_click_ptr = 0usize;
            let mut assist_seen: FxHashSet<u32> = FxHashSet::default();

            let mut i = 0usize;
            while i < evs.len() {
                // Start a new session at evs[i]
                let session_start_ts = evs[i].ts;
                let mut prev_distinct_ts = session_start_ts;
                let mut sess_events = 0i64;
                let mut sess_revenue = 0i64;
                let mut sess_has_purchase = false;

                // UTC date in days since 1970-01-01 (floor division for negative epochs too)
                let start_day = session_start_ts.div_euclid(MS_PER_DAY) as i32;
                let u_agg = user_days.entry(start_day).or_default();

                while i < evs.len() {
                    let ev = &evs[i];
                    if ev.ts != prev_distinct_ts {
                        if ev.ts - prev_distinct_ts > SESSION_GAP_MS {
                            break;
                        }
                        prev_distinct_ts = ev.ts;
                    }

                    sess_events += 1;
                    if ev.is_purchase {
                        sess_has_purchase = true;
                        sess_revenue += ev.price_cents;
                        *u_agg.cat_revenue.entry(ev.category_idx).or_insert(0) += ev.price_cents;
                    }
                    i += 1;
                }

                u_agg.sessions += 1;
                u_agg.events += sess_events;
                u_agg.revenue_cents += sess_revenue;

                // Advance click sliding window to [session_start_ts - 7d, session_start_ts]
                let min_click_ts = session_start_ts - ATTRIBUTION_WINDOW_MS;
                while right_click_ptr < cls.len() && cls[right_click_ptr].ts <= session_start_ts {
                    right_click_ptr += 1;
                }
                while left_click_ptr < right_click_ptr && cls[left_click_ptr].ts < min_click_ts {
                    left_click_ptr += 1;
                }

                if left_click_ptr < right_click_ptr {
                    // Best click is at right_click_ptr - 1 (largest ts, smallest byte-wise click_id)
                    let winning_camp = cls[right_click_ptr - 1].campaign_idx;
                    let c_agg = camp_days.entry((winning_camp, start_day)).or_default();
                    c_agg.sessions += 1;
                    if sess_has_purchase {
                        c_agg.purchasing_sessions += 1;
                    }
                    c_agg.revenue_cents += sess_revenue;

                    // Assists: every distinct campaign with a qualifying click in [min_click_ts, session_start_ts]
                    assist_seen.clear();
                    for c in &cls[left_click_ptr..right_click_ptr] {
                        if assist_seen.insert(c.campaign_idx) {
                            camp_days
                                .entry((c.campaign_idx, start_day))
                                .or_default()
                                .assisted_sessions += 1;
                        }
                    }
                } else {
                    // Credit to organic (never assists)
                    let c_agg = camp_days.entry((organic_idx, start_day)).or_default();
                    c_agg.sessions += 1;
                    if sess_has_purchase {
                        c_agg.purchasing_sessions += 1;
                    }
                    c_agg.revenue_cents += sess_revenue;
                }
            }

            (person, user_days, camp_days)
        })
        .collect();

    // 8. Merge campaign_daily across people & write both Parquet tables
    let mut global_camp_days: FxHashMap<(u32, i32), CampaignDayAgg> = FxHashMap::default();
    for (_person, _ud, cd_map) in &per_person_results {
        for (&key, agg) in cd_map {
            let entry = global_camp_days.entry(key).or_default();
            entry.sessions += agg.sessions;
            entry.purchasing_sessions += agg.purchasing_sessions;
            entry.revenue_cents += agg.revenue_cents;
            entry.assisted_sessions += agg.assisted_sessions;
        }
    }

    write_user_daily_parquet(
        &output_dir.join("user_daily"),
        &per_person_results,
        &categories,
    )?;
    write_campaign_daily_parquet(
        &output_dir.join("campaign_daily"),
        &global_camp_days,
        &campaign_ids,
        &campaign_channels,
    )?;

    Ok(())
}

// ============================================================================
// Parquet Writers
// ============================================================================

fn write_user_daily_parquet(
    out_dir: &Path,
    rows: &[(
        Arc<str>,
        FxHashMap<i32, UserDayAgg>,
        FxHashMap<(u32, i32), CampaignDayAgg>,
    )],
    categories: &[Arc<str>],
) -> Result<()> {
    fs::create_dir_all(out_dir)?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("events", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("top_category", DataType::Utf8, true),
    ]));

    let file = File::create(out_dir.join("part-00000.parquet"))?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

    let batch_cap = 65_536;
    let mut user_b = StringBuilder::with_capacity(batch_cap, batch_cap * 16);
    let mut day_b = Date32Builder::with_capacity(batch_cap);
    let mut sess_b = Int64Builder::with_capacity(batch_cap);
    let mut ev_b = Int64Builder::with_capacity(batch_cap);
    let mut rev_b = Int64Builder::with_capacity(batch_cap);
    let mut cat_b = StringBuilder::with_capacity(batch_cap, batch_cap * 16);
    let mut count = 0usize;

    for (person, user_days, _) in rows {
        for (&day, agg) in user_days {
            user_b.append_value(person);
            day_b.append_value(day);
            sess_b.append_value(agg.sessions);
            ev_b.append_value(agg.events);
            rev_b.append_value(agg.revenue_cents);

            if agg.cat_revenue.is_empty() {
                cat_b.append_null();
            } else {
                let mut best_cat_name: Option<&str> = None;
                let mut best_rev = i64::MIN;
                for (&cat_idx, &rev) in &agg.cat_revenue {
                    let cname = categories[cat_idx as usize].as_ref();
                    match rev.cmp(&best_rev) {
                        Ordering::Greater => {
                            best_rev = rev;
                            best_cat_name = Some(cname);
                        }
                        Ordering::Equal => {
                            if let Some(prev) = best_cat_name {
                                if cname.as_bytes() < prev.as_bytes() {
                                    best_cat_name = Some(cname);
                                }
                            } else {
                                best_cat_name = Some(cname);
                            }
                        }
                        Ordering::Less => {}
                    }
                }
                cat_b.append_value(best_cat_name.unwrap());
            }

            count += 1;
            if count == batch_cap {
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(user_b.finish()),
                        Arc::new(day_b.finish()),
                        Arc::new(sess_b.finish()),
                        Arc::new(ev_b.finish()),
                        Arc::new(rev_b.finish()),
                        Arc::new(cat_b.finish()),
                    ],
                )?;
                writer.write(&batch)?;
                count = 0;
            }
        }
    }

    if count > 0 {
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(user_b.finish()),
                Arc::new(day_b.finish()),
                Arc::new(sess_b.finish()),
                Arc::new(ev_b.finish()),
                Arc::new(rev_b.finish()),
                Arc::new(cat_b.finish()),
            ],
        )?;
        writer.write(&batch)?;
    }

    writer.close()?;
    Ok(())
}

fn write_campaign_daily_parquet(
    out_dir: &Path,
    camp_days: &FxHashMap<(u32, i32), CampaignDayAgg>,
    campaign_ids: &[Arc<str>],
    campaign_channels: &[Arc<str>],
) -> Result<()> {
    fs::create_dir_all(out_dir)?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("campaign_id", DataType::Utf8, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("purchasing_sessions", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("assisted_sessions", DataType::Int64, false),
    ]));

    let file = File::create(out_dir.join("part-00000.parquet"))?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

    let batch_cap = 65_536;
    let mut cid_b = StringBuilder::with_capacity(batch_cap, batch_cap * 16);
    let mut chan_b = StringBuilder::with_capacity(batch_cap, batch_cap * 16);
    let mut day_b = Date32Builder::with_capacity(batch_cap);
    let mut sess_b = Int64Builder::with_capacity(batch_cap);
    let mut purch_sess_b = Int64Builder::with_capacity(batch_cap);
    let mut rev_b = Int64Builder::with_capacity(batch_cap);
    let mut assist_b = Int64Builder::with_capacity(batch_cap);
    let mut count = 0usize;

    for (&(camp_idx, day), agg) in camp_days {
        cid_b.append_value(&campaign_ids[camp_idx as usize]);
        chan_b.append_value(&campaign_channels[camp_idx as usize]);
        day_b.append_value(day);
        sess_b.append_value(agg.sessions);
        purch_sess_b.append_value(agg.purchasing_sessions);
        rev_b.append_value(agg.revenue_cents);
        assist_b.append_value(agg.assisted_sessions);

        count += 1;
        if count == batch_cap {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(cid_b.finish()),
                    Arc::new(chan_b.finish()),
                    Arc::new(day_b.finish()),
                    Arc::new(sess_b.finish()),
                    Arc::new(purch_sess_b.finish()),
                    Arc::new(rev_b.finish()),
                    Arc::new(assist_b.finish()),
                ],
            )?;
            writer.write(&batch)?;
            count = 0;
        }
    }

    if count > 0 {
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(cid_b.finish()),
                Arc::new(chan_b.finish()),
                Arc::new(day_b.finish()),
                Arc::new(sess_b.finish()),
                Arc::new(purch_sess_b.finish()),
                Arc::new(rev_b.finish()),
                Arc::new(assist_b.finish()),
            ],
        )?;
        writer.write(&batch)?;
    }

    writer.close()?;
    Ok(())
}