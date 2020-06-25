use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};

const HIST_SIZE: usize = 50;

struct ZeroInfo {
    z_hist: [u128; HIST_SIZE],
    nz_hist: [u128; HIST_SIZE],
    z_tot_hist: [u128; HIST_SIZE],
    nz_tot_hist: [u128; HIST_SIZE],
}

impl ZeroInfo {
    fn report(self) {
        for i in 1..HIST_SIZE {
            println!("z {} {}", i, self.z_hist[i]);
        }
        for i in 1..HIST_SIZE {
            println!("nz {} {}", i, self.nz_hist[i]);
        }
        for i in 1..HIST_SIZE {
            println!("zt {} {}", i, self.z_tot_hist[i]);
        }
        for i in 1..HIST_SIZE {
            println!("nzt {} {}", i, self.nz_tot_hist[i]);
        }
    }
    fn report_file(self, file: &mut File) {
        for i in 1..HIST_SIZE {
            file.write(format!("z {} {}\n", i, self.z_hist[i]).as_bytes())
                .expect("ERR: file write error");
        }
        for i in 1..HIST_SIZE {
            file.write(format!("nz {} {}\n", i, self.nz_hist[i]).as_bytes())
                .expect("ERR: file write error");
        }
        for i in 1..HIST_SIZE {
            file.write(format!("zt {} {}\n", i, self.z_tot_hist[i]).as_bytes())
                .expect("ERR: file write error");
        }
        for i in 1..HIST_SIZE {
            file.write(format!("nzt {} {}\n", i, self.nz_tot_hist[i]).as_bytes())
                .expect("ERR: file write error");
        }
    }
}

fn count_zeros(file: File) -> ZeroInfo {
    let mut reader = BufReader::with_capacity(10000, file);
    let mut z_hist: [u128; HIST_SIZE] = [0; HIST_SIZE];
    let mut nz_hist: [u128; HIST_SIZE] = [0; HIST_SIZE];
    let mut z_tot_hist: [u128; HIST_SIZE] = [0; HIST_SIZE];
    let mut nz_tot_hist: [u128; HIST_SIZE] = [0; HIST_SIZE];
    let mut z_current = 0;
    let mut nz_current = 0;
    loop {
        let buffer = reader.fill_buf().expect("ERR: file read error");
        let length = buffer.len();
        for &b in buffer {
            if b == 0 {
                z_current += 1;
                if nz_current != 0 {
                    if nz_current >= HIST_SIZE {
                        nz_tot_hist[HIST_SIZE - 1] += nz_current as u128;
                        nz_hist[HIST_SIZE - 1] += 1;
                    } else {
                        nz_tot_hist[nz_current] += nz_current as u128;
                        nz_hist[nz_current] += 1;
                    }
                    nz_current = 0;
                }
            } else {
                nz_current += 1;
                if z_current != 0 {
                    if z_current >= HIST_SIZE {
                        z_tot_hist[HIST_SIZE - 1] += z_current as u128;
                        z_hist[HIST_SIZE - 1] += 1;
                    } else {
                        z_tot_hist[z_current] += z_current as u128;
                        z_hist[z_current] += 1;
                    }
                    z_current = 0;
                }
            }
        }

        if length == 0 {
            break;
        }
        reader.consume(length);
    }
    ZeroInfo {
        z_hist,
        nz_hist,
        z_tot_hist,
        nz_tot_hist,
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let core = File::open(args.get(1).expect("ERR: please specify core file"))
        .expect("ERR: could not find core file");
    let info = count_zeros(core);
    if args.len() > 2 {
        let mut out = File::create(&args[2]).expect("ERR: could not create output file");
        info.report_file(&mut out);
    } else {
        info.report();
    }
}
