use rayon::prelude::*;
use rustc_hash::FxHashMap;
use std::io::{self, prelude::*};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::BufRead,
};

struct Stats {
    count: u32,
    sum: f32,
    min: f32,
    max: f32,
}

const CHUNK_COUNT: u64 = 8;

fn main() -> io::Result<()> {
    let path = "../../java/1brc/measurements-1b.txt";
    let fsize = fs::metadata(path).unwrap().len();

    let chunk_start_offsets = {
        let mut f = File::open(path)?;
        let mut chunk_start_offsets = [0u64; CHUNK_COUNT as usize];
        for chunk_index in 1..CHUNK_COUNT {
            let chunk_start = fsize * chunk_index / CHUNK_COUNT;
            f.seek(io::SeekFrom::Start(chunk_start))?;
            let newline_pos = f
                .try_clone()?
                .bytes()
                .enumerate()
                .find_map(|(i, b)| (b.unwrap() == b'\n').then_some(i as u64))
                .unwrap();
            chunk_start_offsets[chunk_index as usize] = chunk_start + newline_pos + 1;
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
        let mut f = fs::File::open(&path)?;
        f.seek(io::SeekFrom::Start(chunk_start))?;
        let chunk = io::BufReader::new(f.take(chunk_end - chunk_start));
        chunks.push(chunk);
    }

    let stats = chunks
        .par_iter_mut()
        .map(|reader| {
            let mut name_buf = Vec::with_capacity(32);
            let mut val_buf = Vec::with_capacity(32);
            let mut stats =
                FxHashMap::<Vec<u8>, Stats>::with_capacity_and_hasher(1024, Default::default());
            loop {
                name_buf.clear();
                if reader.read_until(b';', &mut name_buf).unwrap() == 0 {
                    break;
                }
                name_buf.pop();

                val_buf.clear();
                if reader.read_until(b'\n', &mut val_buf).unwrap() == 0 {
                    panic!("name without value: {name_buf:?}");
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
        .reduce(
            || FxHashMap::<Vec<u8>, Stats>::with_capacity_and_hasher(1024, Default::default()),
            |mut totals, stats| {
                for (city, city_stats) in stats {
                    let Stats { count, sum, min, max } = city_stats;
                    totals
                        .entry(city)
                        .and_modify(
                            |Stats {
                                 count: total_count,
                                 sum: total_sum,
                                 min: total_min,
                                 max: total_max,
                             }| {
                                *total_count += count;
                                *total_sum += sum;
                                *total_min = total_min.min(min);
                                *total_max = total_max.max(max);
                            },
                        )
                        .or_insert(city_stats);
                }
                totals
            },
        );

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
    Ok(())
}
