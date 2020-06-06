#![allow(dead_code)]

extern crate rand;
extern crate redis;
extern crate serde;
extern crate serde_json;

use std::num::Wrapping;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Instant;

use serde::{Deserialize, Serialize};

use rand::distributions::{Distribution, Uniform};
use rand::{rngs::StdRng, RngCore, SeedableRng};

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
    SetMulti(std::ops::Range<usize>),
    Quit,
}

struct Manager {
    workers: Vec<Worker>,
    tx: mpsc::Sender<Task>,
}

impl Manager {
    fn new(n: usize, addr: &str, record_params: RecordParams) -> Self {
        let mut c = Client::new("redis://127.0.0.1/", record_params);
        c.flush_all();
        let (tx, rx) = mpsc::channel();
        let rx = Arc::new(Mutex::new(rx));
        let mut workers = Vec::new();
        for i in 0..n {
            workers.push(Worker::new(String::from(addr), rx.clone(), record_params));
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
    record_params: RecordParams,
}

impl Worker {
    fn new(
        addr: String,
        rx: Arc<Mutex<mpsc::Receiver<Task>>>,
        record_params: RecordParams,
    ) -> Self {
        Self {
            addr,
            thread: None,
            rx,
            record_params,
        }
    }
    fn run(&mut self) {
        let mut client = Client::new(&self.addr, self.record_params);
        let rx = Arc::clone(&self.rx);
        let handle = thread::spawn(move || loop {
            let task = rx
                .lock()
                .expect("ERR: mutex lock")
                .recv()
                .expect("ERR: channel recv");
            match task {
                Task::Set(key) => client.set(key),
                Task::SetMulti(range) => client.set_multi(range),
                Task::Quit => {
                    client.query();
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
    record_params: RecordParams,
    uni_dist: Uniform<usize>,
    cons_dist: usize,
}

impl Client {
    fn new(addr: &str, record_params: RecordParams) -> Self {
        let conn = redis::Client::open(addr)
            .expect(&format!("ERR: bad address `{}`", addr))
            .get_connection()
            .expect("ERR: could not establish connection with redis server");
        let rng = StdRng::from_entropy();
        let pipe_cap = 16;
        let pipe = redis::Pipeline::with_capacity(pipe_cap);
        let uni_dist = Uniform::from(record_params.field_cap_min..record_params.field_cap_max);
        Self {
            conn,
            rng,
            pipe,
            pipe_cap,
            pipe_vol: 0,
            record_params,
            uni_dist,
            cons_dist: record_params.field_cap,
        }
    }
    fn flush_all(&mut self) {
        redis::cmd("FLUSHALL")
            .query::<()>(&mut self.conn)
            .expect("ERR: redis flushall");
    }
    fn set(&mut self, key: usize) {
        let key = format!("user{}", fnv(key));
        if self.record_params.field_count == 0 {
            let val = self.build_field();
            self.pipe.set(key, val);
        } else {
            let val = self.build_set();
            self.pipe.hset_multiple(key, &val);
        }
        self.query_if_full();
    }
    fn set_multi(&mut self, range: std::ops::Range<usize>) {
        for i in range {
            self.set(i);
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
        for i in 0..self.record_params.field_count {
            ret.push((format!("field{}", i), self.build_field()))
        }
        ret
    }
    fn build_field(&mut self) -> Vec<u8> {
        let field_cap = match self.record_params.field_cap_dist {
            'u' => self.uni_dist.sample(&mut self.rng),
            _ => self.cons_dist,
        };
        let field_vol = (field_cap as f64 * self.record_params.field_density) as usize;
        let mut bytes = vec![0; field_vol];
        self.rng.fill_bytes(&mut bytes);
        bytes.append(&mut vec![0; field_cap - field_vol]);
        bytes
    }
}

#[derive(Serialize, Deserialize)]
struct WorkloadParams {
    record_count: usize,
    operation_count: usize,
    record_params: RecordParams,
}

#[derive(Serialize, Deserialize, Copy, Clone)]
struct RecordParams {
    field_count: usize,
    field_cap_dist: char,
    field_cap_min: usize,
    field_cap_max: usize,
    field_cap: usize,
    field_density: f64,
}

fn main() {
    let config = std::fs::read_to_string("config.json").expect("ERR: no config file");
    let workload: WorkloadParams = serde_json::from_str(&config).expect("ERR: bad config file");
    run_workload(workload);
}

fn run_workload(workload: WorkloadParams) {
    let start = Instant::now();
    let m = Manager::new(6, "redis://127.0.0.1/", workload.record_params);
    for i in 0..workload.record_count {
        m.dispatch(Task::Set(i));
    }

    let mut rng = StdRng::from_entropy();
    let keys = Uniform::from(0..workload.record_count);
    for _ in 0..workload.operation_count {
        m.dispatch(Task::Set(keys.sample(&mut rng)));
    }
    drop(m);
    let duration = start.elapsed();
    println!("time: {:?}", duration);
}
