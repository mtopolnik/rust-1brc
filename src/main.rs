use rayon::prelude::*;
use rustc_hash::FxHashMap;
use std::io::{self, prelude::*};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{BufRead, BufReader},
};

struct Stats {
    count: u32,
    sum: f32,
    min: f32,
    max: f32,
}

const CHUNK_COUNT: u64 = 8;

fn main() {
    let path = "../../java/1brc/measurements_small.txt";

    let fsize = fs::metadata(path).unwrap().len();
    let mut reader = BufReader::new(File::open(path).unwrap());
    let mut chunk_start_offsets = [0u64; CHUNK_COUNT as usize];
    let mut buf = Vec::with_capacity(32);
    for chunk_index in 1..CHUNK_COUNT {
        let chunk_start = fsize * chunk_index / CHUNK_COUNT;
        reader.seek(io::SeekFrom::Start(chunk_start)).unwrap();
        buf.clear();
        let count = reader.read_until(b'\n', &mut buf).unwrap() as u64;
        chunk_start_offsets[chunk_index as usize] = chunk_start + count;
    }
    let mut chunks = Vec::new();
    for i in 0..chunk_start_offsets.len() {
        let chunk_start = chunk_start_offsets[i];
        let chunk_end = if i < chunk_start_offsets.len() - 1 {
            chunk_start_offsets[i + 1]
        } else {
            fsize
        };
        let mut f = fs::File::open(&path).unwrap();
        f.seek(io::SeekFrom::Start(chunk_start)).unwrap();
        let chunk = io::BufReader::new(f.take(chunk_end - chunk_start));
        chunks.push(chunk);
    }

    let stats = chunks
        .iter_mut()
        .map(|reader| {
            println!("start chunk");
            let mut name_buf = Vec::with_capacity(32);
            let mut val_buf = Vec::with_capacity(32);

            let mut stats =
                FxHashMap::<Vec<u8>, Stats>::with_capacity_and_hasher(1024, Default::default());
            loop {
                name_buf.clear();
                val_buf.clear();
                if reader.read_until(b';', &mut name_buf).unwrap() == 0 {
                    break;
                }
                name_buf.pop();
                if reader.read_until(b'\n', &mut val_buf).unwrap() == 0 {
                    println!("name_buf {:?}", name_buf);
                    panic!("name without value");
                }
                val_buf.pop();
                let temperature: f32 = (&String::from_utf8_lossy(&val_buf)).parse().unwrap();
                if let Some(Stats { count, sum, min, max }) = stats.get_mut(&name_buf) {
                    *count += 1;
                    *sum += temperature;
                    *min = min.min(temperature);
                    *max = max.max(temperature);
                } else {
                    stats.insert(
                        name_buf.clone(),
                        Stats {
                            count: 1,
                            sum: temperature,
                            min: temperature,
                            max: temperature,
                        },
                    );
                }
            }
            stats
        })
        .reduce(|mut totals, stats| {
            for (city, city_stats) in stats {
                let Stats { count, sum, min, max } = city_stats;
                if let Some(Stats {
                    count: total_count,
                    sum: total_sum,
                    min: total_min,
                    max: total_max,
                }) = totals.get_mut(&city)
                {
                    *total_count += count;
                    *total_sum += sum;
                    *total_min = total_min.min(min);
                    *total_max = total_max.max(max);
                } else {
                    totals.insert(city, city_stats);
                }
            }
            totals
        })
        .unwrap();

    let mut sorted = BTreeMap::new();
    sorted.extend(stats);
    print!("{{");
    let mut on_first = true;
    for (city, Stats { count, sum, min, max }) in sorted {
        let (count, sum, min, max) = (count as f32, sum, min, max);
        if on_first {
            on_first = false;
        } else {
            print!(", ");
        }
        print!(
            "{}={:.1}/{:.1}/{:.1}",
            String::from_utf8_lossy(&city),
            min,
            sum / count,
            max
        );
    }
    println!("}}");
}
