use std::env;
use std::fs::File;
use std::io::{self, BufRead, BufReader};

const HIST_SIZE: usize = 50;

fn count_zeros() -> io::Result<()> {
    let file = File::open(format!(
        "{}/out/core",
        env::var("COMPRESSION_HOME").expect("ERR: could not find COMPRESSION_HOME")
    ))?;
    let mut reader = BufReader::with_capacity(10000, file);
    let mut z_hist: [u128; HIST_SIZE] = [0; HIST_SIZE];
    let mut nz_hist: [u128; HIST_SIZE] = [0; HIST_SIZE];
    let mut z_tot_hist: [u128; HIST_SIZE] = [0; HIST_SIZE];
    let mut nz_tot_hist: [u128; HIST_SIZE] = [0; HIST_SIZE];
    let mut z_current = 0;
    let mut nz_current = 0;
    loop {
        let buffer = reader.fill_buf()?;
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
    for i in 1..HIST_SIZE {
        println!("z {} {}", i, z_hist[i]);
    }
    for i in 1..HIST_SIZE {
        println!("nz {} {}", i, nz_hist[i]);
    }
    for i in 1..HIST_SIZE {
        println!("zt {} {}", i, z_tot_hist[i]);
    }
    for i in 1..HIST_SIZE {
        println!("nzt {} {}", i, nz_tot_hist[i]);
    }

    Ok(())
}

fn main() -> io::Result<()> {
    count_zeros()
}
