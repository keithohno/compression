extern crate lz4;
extern crate zstd;

use std::collections::HashMap;
use std::fmt::{self, Display, Formatter};
use std::fs::File;
use std::io::Write;
use std::io::{BufRead, BufReader};
use std::sync::{Arc, Mutex};
use std::thread;

const BUF_SIZE: usize = 4096;
const SPLITS: [usize; 5] = [1, 2, 4, 8, 16];

#[derive(Copy, Clone)]
pub enum CType {
    Zstd,
    Lz4,
}

impl Display for CType {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                CType::Zstd => "zstd",
                CType::Lz4 => "lz4",
            }
        )
    }
}

fn thread_compress(
    ctype: CType,
    reader: Arc<Mutex<BufReader<File>>>,
) -> thread::JoinHandle<HashMap<usize, u128>> {
    let handle = thread::spawn(move || {
        let mut comp_total: [u128; 5] = [0; 5];
        let mut total: u128 = 0;
        let mut zstd_compressor = zstd::block::Compressor::new();
        let mut zstd_dest: [u8; 20000] = [0; 20000];
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
                    comp_total[j] += match ctype {
                        CType::Lz4 => {
                            lz4::block::compress(&buffer[i * new_l..(i + 1) * new_l], None, false)
                                .unwrap()
                                .len()
                        }
                        CType::Zstd => zstd_compressor
                            .compress_to_buffer(
                                &buffer[i * new_l..(i + 1) * new_l],
                                &mut zstd_dest,
                                3,
                            )
                            .expect("ERR: compression"),
                    } as u128;
                }
            }
            total += length as u128;

            if length == 0 {
                break;
            }
        }
        let mut size_info = HashMap::new();
        for i in 0..SPLITS.len() {
            size_info.insert(BUF_SIZE / SPLITS[i], comp_total[i]);
        }
        size_info.insert(0, total);
        size_info
    });
    return handle;
}

pub fn compress(core: File, ctype: CType) -> HashMap<usize, u128> {
    let reader = BufReader::with_capacity(BUF_SIZE, core);
    let reader = Arc::new(Mutex::new(reader));
    let mut handles = Vec::new();
    for _ in 0..6 {
        handles.push(thread_compress(ctype, Arc::clone(&reader)))
    }
    let mut size_info_tot = HashMap::new();
    for i in 0..SPLITS.len() {
        size_info_tot.insert(BUF_SIZE / SPLITS[i], 0);
    }
    size_info_tot.insert(0, 0);
    for h in handles {
        let size_info = h.join().expect("ERR: thread join error");
        for (&k, v) in &size_info {
            size_info_tot.insert(k, v + size_info_tot.get(&k).expect("ERR: hashmap"));
        }
    }
    size_info_tot
}

pub fn report_file(out: &mut File, info: HashMap<usize, u128>) {
    for (&k, v) in &info {
        out.write(format!("{: <4} {}\n", k, v).as_bytes())
            .expect("ERR: file write error");
    }
}

pub fn report(info: HashMap<usize, u128>) {
    for (&k, v) in &info {
        println!("{: <4} {}", k, v);
    }
}
