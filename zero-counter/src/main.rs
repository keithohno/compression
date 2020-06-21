use std::fs::File;
use std::io::{self, BufRead, BufReader};

fn count_zeros() -> io::Result<()> {
    let file = File::open("core")?;
    let mut reader = BufReader::with_capacity(10000, file);
    let mut counter: [u128; 50] = [0; 50];
    let mut total: u128 = 0;
    let mut zero_seq_len: usize = 0;
    loop {
        let buffer = reader.fill_buf()?;
        let length = buffer.len();
        for &b in buffer {
            if b == 0 {
                zero_seq_len += 1;
                total += 1;
            } else {
                if zero_seq_len > 49 {
                    counter[49] += zero_seq_len as u128;
                } else {
                    counter[zero_seq_len] += zero_seq_len as u128;
                }
                zero_seq_len = 0;
            }
        }

        if length == 0 {
            break;
        }
        reader.consume(length);
    }
    println!("SPLIT");
    println!("{}", total);
    println!("SPLIT");
    for i in 0..counter.len() {
        println!("{}", counter[i]);
    }

    Ok(())
}

fn main() -> io::Result<()> {
    count_zeros()
}
