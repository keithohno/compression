extern crate lz4;
extern crate zstd;

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;
use zstd::block::Compressor;

const BUF_SIZE: usize = 4096;
const SPLITS: [usize; 5] = [1, 2, 4, 8, 16];

enum CType {
    Zstd,
    Lz4,
}

fn zstd_compress(reader: Arc<Mutex<BufReader<File>>>) -> thread::JoinHandle<()> {
    let handle = thread::spawn(move || {
        let mut comp_total: [u128; 5] = [0; 5];
        let mut compressor = Compressor::new();
        let mut total: u128 = 0;
        let mut dest: [u8; 20000] = [0; 20000];
        loop {
            let mut buffer: [u8; BUF_SIZE] = [0; BUF_SIZE];
            let length;
            {
                let mut reader = reader.lock().expect("ERR: mutex lock error");
                let buf = reader.fill_buf().expect("ERR: file read error");
                for i in 0..buf.len() {
                    buffer[i] = buf[i];
                }
                length = buf.len();
                reader.consume(length);
            }
            for (j, &div) in SPLITS.iter().enumerate() {
                let new_l = length / div;
                for i in 0..div {
                    comp_total[j] += compressor
                        .compress_to_buffer(&buffer[i * new_l..(i + 1) * new_l], &mut dest, 3)
                        .expect("ERR: compression") as u128;
                }
            }
            total += length as u128;

            if length == 0 {
                break;
            }
        }
        println!("TOTAL: {}", total);
        for (i, &item) in comp_total.iter().enumerate() {
            println!("{}: {}", i, item);
        }
    });
    return handle;
}

fn lz4_compress(reader: Arc<Mutex<BufReader<File>>>) -> thread::JoinHandle<()> {
    let handle = thread::spawn(move || {
        let mut comp_total: [u128; 5] = [0; 5];
        let mut total: u128 = 0;
        loop {
            let mut buffer: [u8; BUF_SIZE] = [0; BUF_SIZE];
            let length;
            {
                let mut reader = reader.lock().expect("ERR: mutex lock error");
                let buf = reader.fill_buf().expect("ERR: file read error");
                for i in 0..buf.len() {
                    buffer[i] = buf[i];
                }
                length = buf.len();
                reader.consume(length);
            }
            for (j, &div) in SPLITS.iter().enumerate() {
                let new_l = length / div;
                for i in 0..div {
                    comp_total[j] +=
                        lz4::block::compress(&buffer[i * new_l..(i + 1) * new_l], None, false)
                            .unwrap()
                            .len() as u128;
                }
            }
            total += length as u128;

            if length == 0 {
                break;
            }
        }
        println!("TOTAL: {}", total);
        for (i, &item) in comp_total.iter().enumerate() {
            println!("{}: {}", i, item);
        }
    });
    return handle;
}

fn compress(path: &str, ctype: CType) {
    let core = File::open(path).expect("ERR: could not find core file");
    let reader = BufReader::with_capacity(BUF_SIZE, core);
    let reader = Arc::new(Mutex::new(reader));
    let start = Instant::now();
    let mut handles = Vec::new();
    for _ in 0..6 {
        match ctype {
            CType::Zstd => handles.push(zstd_compress(Arc::clone(&reader))),
            CType::Lz4 => handles.push(lz4_compress(Arc::clone(&reader))),
        }
    }
    for h in handles {
        h.join().expect("ERR: thread join error");
    }
    let duration = start.elapsed();
    match ctype {
        CType::Zstd => println!("zstd: {:?}", duration),
        CType::Lz4 => println!("lz4: {:?}", duration),
    }
}

fn main() {
    compress("/home/keith/compression/out/core", CType::Zstd);
    compress("/home/keith/compression/out/core", CType::Lz4);
}
