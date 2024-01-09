use bstr::ByteSlice;
use memmap::MmapOptions;
use rayon::prelude::*;
use std::collections::HashMap;
use std::convert::TryInto;
use std::ops::{Shl, Shr};
use std::{array, io};
use std::{collections::BTreeMap, fs::File};

#[repr(C, align(64))]
struct Stats {
    hash: u64,
    name_len: u32,
    count: u32,
    sum: i32,
    min: i16,
    max: i16,
    name: [u8; 104],
}

impl Default for Stats {
    fn default() -> Self {
        Self {
            name: array::from_fn(|_| 0u8),
            hash: Default::default(),
            count: Default::default(),
            sum: Default::default(),
            min: Default::default(),
            max: Default::default(),
            name_len: Default::default(),
        }
    }
}

struct FinalStats {
    count: u32,
    sum: i32,
    min: i16,
    max: i16,
}

fn main() -> io::Result<()> {
    let path = "measurements.txt";
    let chunk_count: usize = std::thread::available_parallelism().unwrap().into();
    let file = File::open(path).unwrap();
    let mmap = unsafe { MmapOptions::new().map(&file).unwrap() };
    let fsize = mmap.len();

    let chunk_start_offsets = {
        let mut chunk_start_offsets = Vec::with_capacity(chunk_count);
        chunk_start_offsets.push(0);
        for chunk_index in 1..chunk_count {
            let chunk_start = fsize * chunk_index / chunk_count;
            let newline_pos = memchr::memchr(b'\n', &mmap[chunk_start..]).unwrap();
            chunk_start_offsets.push(chunk_start + newline_pos + 1);
        }
        chunk_start_offsets
    };
    let mut chunks = Vec::new();
    for i in 0..chunk_start_offsets.len() {
        let chunk_start = chunk_start_offsets[i];
        let chunk_end = if i < chunk_start_offsets.len() - 1 {
            chunk_start_offsets[i + 1]
        } else {
            fsize
        };
        if chunk_start < chunk_end {
            chunks.push((chunk_start, chunk_end));
        }
    }

    const HASHTABLE_SIZE: usize = 32_768;

    let stats = chunks
        .par_iter()
        .map(|&(start, limit)| {
            let mut hashtable: Vec<Stats> = Vec::with_capacity(HASHTABLE_SIZE);
            for _ in 0..HASHTABLE_SIZE {
                hashtable.push(Stats::default());
            }
            let mut record_tail = &mmap[start..limit];
            loop {
                let pos_of_semicolon = record_tail.find_byte(b';').unwrap();
                let name = &record_tail[..pos_of_semicolon];
                let hash = hash(record_tail, pos_of_semicolon);
                let temperature_tail = &record_tail[pos_of_semicolon + 1..];
                let (temperature, pos_of_next_line) = parse_temperature(temperature_tail);
                let mut hashtable_index = hash as usize % HASHTABLE_SIZE;
                loop {
                    let stats = &mut hashtable[hashtable_index];
                    let name_len = stats.name_len as usize;
                    if stats.hash == hash
                        && name_len == name.len()
                        && &stats.name[..name_len] == name
                    {
                        stats.count += 1;
                        stats.sum += temperature as i32;
                        stats.min = stats.min.min(temperature);
                        stats.max = stats.max.max(temperature);
                        break;
                    }
                    if stats.hash != 0 {
                        hashtable_index = (hashtable_index + 1) % HASHTABLE_SIZE;
                        continue;
                    }
                    stats.hash = hash;
                    stats.name_len = pos_of_semicolon as u32;
                    stats.count = 1;
                    stats.sum = temperature as i32;
                    stats.min = temperature;
                    stats.max = temperature;
                    stats.name[..name.len()].copy_from_slice(name);
                    break;
                }
                if pos_of_next_line >= temperature_tail.len() {
                    break;
                }
                record_tail = &temperature_tail[pos_of_next_line..];
            }
            hashtable
        })
        .fold(
            || HashMap::<String, FinalStats>::with_capacity(16_384),
            |mut totals, hashtable| {
                for stats in hashtable {
                    if stats.hash == 0 {
                        continue;
                    }
                    let Stats { name_len, name, count, sum, min, max, .. } = stats;
                    totals
                        .entry(String::from_utf8_lossy(&name[..name_len as usize]).into_owned())
                        .and_modify(|totals| {
                            totals.count += count;
                            totals.sum += sum;
                            totals.min = (totals.min).min(min);
                            totals.max = (totals.max).max(max);
                        })
                        .or_insert(FinalStats { count, sum, min, max });
                }
                totals
            },
        )
        .reduce(
            || HashMap::<String, FinalStats>::with_capacity(16_384),
            |mut totals, stats_map| {
                for (name, FinalStats { count, sum, min, max }) in stats_map {
                    totals
                        .entry(name)
                        .and_modify(|totals| {
                            totals.count += count;
                            totals.sum += sum;
                            totals.min = (totals.min).min(min);
                            totals.max = (totals.max).max(max);
                        })
                        .or_insert(FinalStats { count, sum, min, max });
                }
                totals
            },
        );

    let mut sorted = BTreeMap::new();
    sorted.extend(stats);
    print!("{{");
    let mut on_first = true;
    for (city, FinalStats { count, sum, min, max, .. }) in sorted {
        let (count, sum, min, max) = (count as f32, sum, min, max);
        if on_first {
            on_first = false;
        } else {
            print!(", ");
        }
        print!(
            "{}={:.1}/{:.1}/{:.1}",
            city,
            (min as f64) / 10.0,
            ((sum as f64) / (count as f64)).round() / 10.0,
            (max as f64) / 10.0
        );
    }
    println!("}}");
    Ok(())
}

fn hash(name_tail: &[u8], pos_of_semicolon: usize) -> u64 {
    let seed: u64 = 0x51_7c_c1_b7_27_22_0a_95;
    let rot_dist = 17;

    let block = if name_tail.len() >= 8 {
        let block = u64::from_le_bytes(name_tail[0..8].try_into().unwrap());
        let shift_distance = 8 * 0.max(8 - pos_of_semicolon as i32);
        // Mask out bytes not belonging to name
        let mask = (!0u64).shr(shift_distance);
        block & mask
    } else {
        let mut buf = [0u8; 8];
        let copy_len = pos_of_semicolon.min(8);
        buf[..copy_len].copy_from_slice(&name_tail[..copy_len]);
        u64::from_le_bytes(buf)
    };
    let mut hash = block;
    hash = hash.wrapping_mul(seed);
    hash = hash.rotate_left(rot_dist);
    if hash != 0 {
        hash
    } else {
        1
    }
}

fn parse_temperature(chars: &[u8]) -> (i16, usize) {
    if chars.len() >= 8 {
        parse_temperature_swar(chars)
    } else {
        parse_temperature_simple(chars)
    }
}

fn parse_temperature_swar(chars: &[u8]) -> (i16, usize) {
    let word = i64::from_le_bytes(chars[0..8].try_into().unwrap());
    let negated = !word;
    let dot_pos = (negated & 0x10101000).trailing_zeros();
    let mut signed: i64 = negated.shl(59);
    signed = signed.shr(63);
    let remove_sign_mask = !(signed & 0xFF);
    let digits = (word & remove_sign_mask).shl(28 - dot_pos) & 0x0F000F0F00;
    let abs_value = (digits.wrapping_mul(0x640a0001)).shr(32) & 0x3FFi64;
    let temperature = (abs_value ^ signed) - signed;
    (temperature as i16, (dot_pos / 8 + 3) as usize)
}

fn parse_temperature_simple(chars: &[u8]) -> (i16, usize) {
    let mut i = 0;
    let sign = if chars[i] == b'-' {
        i += 1;
        -1
    } else {
        1
    };
    let mut temperature: i16 = (chars[i] - b'0') as i16;
    i += 1;
    if chars[i] == b'.' {
        i += 1;
    } else {
        temperature = 10 * temperature + (chars[i] - b'0') as i16;
        i += 2;
    }
    (sign * (10 * temperature + (chars[i] - b'0') as i16), i + 2)
}
