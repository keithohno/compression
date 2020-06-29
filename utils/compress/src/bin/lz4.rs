use std::env;
use std::fs::File;

fn main() {
    let args: Vec<String> = env::args().collect();
    let core = File::open(args.get(1).expect("ERR: please specify core file"))
        .expect("ERR: could not find core file");
    let comp_info = lib::compress(core, lib::CType::Lz4);

    if args.len() > 2 {
        let mut out = File::create(&args[2]).expect("ERR: could not create output file");
        lib::report_file(&mut out, comp_info);
    } else {
        lib::report(comp_info);
    }
}
