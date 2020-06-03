extern crate rand;
extern crate redis;

use std::num::Wrapping;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Instant;

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
    Quit,
}

struct Manager {
    workers: Vec<Worker>,
    tx: mpsc::Sender<Task>,
}

impl Manager {
    fn new(n: usize, addr: &str) -> Self {
        let (tx, rx) = mpsc::channel();
        let rx = Arc::new(Mutex::new(rx));
        let mut workers = Vec::new();
        for i in 0..n {
            workers.push(Worker::new(String::from(addr), rx.clone()));
            workers[i].run();
        }
        Manager { workers, tx }
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
}

impl Worker {
    fn new(addr: String, rx: Arc<Mutex<mpsc::Receiver<Task>>>) -> Self {
        Worker {
            addr,
            thread: None,
            rx,
        }
    }
    fn run(&mut self) {
        let mut client = Client::new(&self.addr);
        let rx = Arc::clone(&self.rx);
        let handle = thread::spawn(move || loop {
            match rx
                .lock()
                .expect("ERR: mutex lock")
                .recv()
                .expect("ERR: channel recv")
            {
                Task::Set(key) => client.set(key),
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
}

impl Client {
    fn new(addr: &str) -> Self {
        let conn = redis::Client::open(addr)
            .expect(&format!("ERR: bad address `{}`", addr))
            .get_connection()
            .expect("ERR: could not establish connection with redis server");
        let rng = StdRng::from_entropy();
        let pipe_cap = 16;
        let pipe = redis::Pipeline::with_capacity(pipe_cap);
        Client {
            conn,
            rng,
            pipe,
            pipe_cap,
            pipe_vol: 0,
        }
    }
    fn set(&mut self, key: usize) {
        let val = self.val();
        self.pipe.hset_multiple(format!("user{}", fnv(key)), &val);
        self.query_if_full();
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
    fn val(&mut self) -> Vec<(String, Vec<u8>)> {
        let mut ret: Vec<(String, Vec<u8>)> = Vec::new();
        for i in 0..10 {
            let mut bytes = vec![0; 100];
            self.rng.fill_bytes(&mut bytes);
            ret.push((format!("field{}", i), bytes));
        }
        ret
    }
}

fn main() {
    let start = Instant::now();
    workload();
    let duration = start.elapsed();
    println!("time: {:?}", duration);
}

fn workload() {
    let m = Manager::new(15, "redis://127.0.0.1/");
    for i in 0..50000 {
        m.dispatch(Task::Set(i));
    }
}
