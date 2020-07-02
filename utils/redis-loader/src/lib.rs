#![allow(dead_code)]

extern crate rand;
extern crate rand_distr;
extern crate redis;
extern crate serde;
extern crate serde_json;

use std::env;
use std::num::Wrapping;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Instant;

use serde::{Deserialize, Serialize};

use rand::{rngs::StdRng, RngCore, SeedableRng};
use rand_distr::{Distribution, Normal, Poisson, Uniform};

const FNV_OFFSET: Wrapping<usize> = Wrapping(0xCBF29CE484222325);
const FNV_PRIME: Wrapping<usize> = Wrapping(1099511628211);
const FNV_MASK: Wrapping<usize> = Wrapping(0xff);

fn fnv(val: usize) -> usize {
    let mut val = Wrapping(val);
    let mut hash = FNV_OFFSET;
    for _ in 0..8 {
        let octet = val & FNV_MASK;
        val = val >> 8;

        hash = hash ^ octet;
        hash *= FNV_PRIME;
    }
    hash.0
}

enum Task {
    Set(usize),
    SetMulti(usize),
    Quit,
}

struct Manager {
    workers: Vec<Worker>,
    tx: mpsc::Sender<Task>,
}

impl Manager {
    fn new(n: usize, addr: &str, params: WorkloadParams) -> Self {
        let (tx, rx) = mpsc::channel();
        let rx = Arc::new(Mutex::new(rx));
        let mut workers = Vec::new();
        for i in 0..n {
            workers.push(Worker::new(String::from(addr), rx.clone(), params.clone()));
            workers[i].run();
        }
        Self { workers, tx }
    }
    fn dispatch(&self, task: Task) {
        self.tx.send(task).expect("ERR: channel send set");
    }
}

impl Drop for Manager {
    fn drop(&mut self) {
        for _ in &self.workers {
            self.tx.send(Task::Quit).expect("ERR: channel send quit");
        }
        for w in &mut self.workers {
            w.quit();
        }
    }
}

struct Worker {
    addr: String,
    thread: Option<thread::JoinHandle<()>>,
    rx: Arc<Mutex<mpsc::Receiver<Task>>>,
    params: WorkloadParams,
}

impl Worker {
    fn new(addr: String, rx: Arc<Mutex<mpsc::Receiver<Task>>>, params: WorkloadParams) -> Self {
        Self {
            addr,
            thread: None,
            rx,
            params,
        }
    }
    fn run(&mut self) {
        let mut client = Client::new(&self.addr, self.params.clone());
        let rx = Arc::clone(&self.rx);
        let handle = thread::spawn(move || loop {
            let task = rx
                .lock()
                .expect("ERR: mutex lock")
                .recv()
                .expect("ERR: channel recv");
            match task {
                Task::Set(key) => client.set(key),
                Task::SetMulti(n) => client.set_multi(n),
                Task::Quit => {
                    client.query();
                    client.summarize();
                    break;
                }
            };
        });
        self.thread = Some(handle);
    }
    fn quit(&mut self) {
        match self.thread.take() {
            Some(handle) => handle.join().expect("ERR: thread join"),
            _ => {}
        };
    }
}

struct Client {
    conn: redis::Connection,
    rng: StdRng,
    pipe: redis::Pipeline,
    pipe_cap: usize,
    pipe_vol: usize,
    params: WorkloadParams,
    keys: Uniform<usize>,
    uni_dist: Uniform<usize>,
    cons_dist: u64,
    norm_dist: Normal<f64>,
    pois_dist: Poisson<f64>,
    total_bytes: usize,
    total_reqs: usize,
}

impl Client {
    fn new(addr: &str, params: WorkloadParams) -> Self {
        let conn = redis::Client::open(addr)
            .expect(&format!("ERR: bad address `{}`", addr))
            .get_connection()
            .expect("ERR: could not establish connection with redis server");
        let rng = StdRng::from_entropy();
        let pipe_cap = 16;
        let pipe = redis::Pipeline::with_capacity(pipe_cap);
        let keys = Uniform::from(0..params.record_count);
        let uni_dist = Uniform::from(
            params.field_av - params.field_range / 2..params.field_av + params.field_range / 2 + 1,
        );
        let norm_dist = Normal::new(params.field_av as f64, params.field_std as f64)
            .expect("ERR: bad distributions params");
        let pois_dist =
            Poisson::new(params.field_av as f64).expect("ERR: bad distributions params");
        let cons_dist = params.field_av as u64;
        Self {
            conn,
            rng,
            pipe,
            pipe_cap,
            pipe_vol: 0,
            params,
            keys,
            uni_dist,
            cons_dist,
            norm_dist,
            pois_dist,
            total_bytes: 0,
            total_reqs: 0,
        }
    }
    fn flush_all(&mut self) {
        redis::cmd("FLUSHALL")
            .query::<()>(&mut self.conn)
            .expect("ERR: redis flushall");
    }
    fn set(&mut self, key: usize) {
        let key = format!("{}{}", self.params.key_prefix, fnv(key));
        if self.params.field_count == 0 {
            let val = self.build_field();
            self.pipe.set(key, val);
        } else {
            let val = self.build_set();
            self.pipe.hset_multiple(key, &val);
        }
        self.total_reqs += 1;
        self.query_if_full();
    }
    fn set_multi(&mut self, n: usize) {
        for _ in 0..n {
            let key = self.keys.sample(&mut self.rng);
            self.set(key);
        }
    }
    fn query_if_full(&mut self) {
        self.pipe_vol += 1;
        if self.pipe_vol > self.pipe_cap {
            self.query()
        }
    }
    fn query(&mut self) {
        self.pipe
            .query::<()>(&mut self.conn)
            .expect("ERR: bad redis query");
        self.pipe.clear();
        self.pipe_vol = 0;
    }
    fn build_set(&mut self) -> Vec<(String, Vec<u8>)> {
        let mut ret: Vec<(String, Vec<u8>)> = Vec::new();
        for i in 0..self.params.field_count {
            ret.push((
                format!("{}{}", self.params.field_prefix, i),
                self.build_field(),
            ))
        }
        ret
    }
    fn build_field(&mut self) -> Vec<u8> {
        let field_size = match self.params.field_dist {
            'u' => self.uni_dist.sample(&mut self.rng) as u64,
            'n' => {
                let val = self.norm_dist.sample(&mut self.rng).round();
                if val < 0.0 {
                    0
                } else if val > 200.0 {
                    200
                } else {
                    val as u64
                }
            }
            'p' => self.pois_dist.sample(&mut self.rng),
            _ => self.cons_dist,
        } as usize;
        let field_vol = (field_size as f64 * self.params.field_density) as usize;
        let mut bytes = vec![0; field_vol];
        self.rng.fill_bytes(&mut bytes);
        bytes.append(&mut vec![0; field_size - field_vol]);
        self.total_bytes += field_size;
        bytes
    }
    fn summarize(&self) {
        println!(
            "average bytes: {}   total bytes: {}   total reqs: {}",
            self.total_bytes as f64 / self.total_reqs as f64,
            self.total_bytes,
            self.total_reqs
        )
    }
}

#[derive(Serialize, Deserialize, Clone)]
struct WorkloadParams {
    record_count: usize,
    operation_count: usize,
    field_count: usize,
    field_dist: char,
    field_range: usize,
    field_av: usize,
    field_density: f64,
    field_std: usize,
    field_prefix: String,
    key_prefix: String,
}

fn load_config() -> WorkloadParams {
    let args: Vec<String> = env::args().collect();
    let config = std::fs::read_to_string(args.get(1).expect("ERR: please specify config file"))
        .expect("ERR: could not find config file");
    serde_json::from_str(&config).expect("ERR: bad config file")
}

pub fn workload_all() {
    println!("Main pid : {}", std::process::id());
    let params = load_config();
    let start = Instant::now();
    let m = Manager::new(6, "redis://127.0.0.1/", params.clone());
    for i in 0..params.record_count {
        m.dispatch(Task::Set(i));
    }
    for _ in 0..(params.operation_count / 1000) {
        m.dispatch(Task::SetMulti(1000));
    }
    drop(m);
    let duration = start.elapsed();
    println!("redis-load: {:?}", duration);
}

pub fn workload_load() {
    let params = load_config();
    let start = Instant::now();
    let m = Manager::new(6, "redis://127.0.0.1/", params.clone());
    for i in 0..params.record_count {
        m.dispatch(Task::Set(i));
    }
    drop(m);
    let duration = start.elapsed();
    println!("redis-load: {:?}", duration);
}

pub fn workload_run() {
    let params = load_config();
    let start = Instant::now();
    let m = Manager::new(6, "redis://127.0.0.1/", params.clone());
    for _ in 0..(params.operation_count / 1000) {
        m.dispatch(Task::SetMulti(1000));
    }
    drop(m);
    let duration = start.elapsed();
    println!("redis-load: {:?}", duration);
}
