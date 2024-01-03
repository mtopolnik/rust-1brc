use rustc_hash::FxHashMap;
use std::{
    borrow::Cow,
    collections::BTreeMap,
    fs::File,
    io::{BufRead, BufReader},
};

struct Stats {
    count: u32,
    sum: f32,
    min: f32,
    max: f32,
}

fn main() {
    let mut reader = BufReader::new(File::open("../../java/1brc/measurements_small.txt").unwrap());
    let mut stats = FxHashMap::<Vec<u8>, Stats>::with_capacity_and_hasher(1024, Default::default());
    let mut name_buf = Vec::with_capacity(32);
    let mut val_buf = Vec::with_capacity(32);
    loop {
        name_buf.clear();
        val_buf.clear();
        if reader.read_until(b';', &mut name_buf).unwrap() == 0 {
            break;
        }
        name_buf.pop();
        if reader.read_until(b'\n', &mut val_buf).unwrap() == 0 {
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
