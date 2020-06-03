extern crate redis;

use std::time::Instant;
use std::thread;
use std::sync::{mpsc, Arc, Mutex};
use redis::{Commands, RedisResult};

enum Task {
    Set(usize),
    Quit
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
        self.tx.send(task).unwrap();
    }
}

impl Drop for Manager {
    fn drop(&mut self) {
        for _ in &self.workers {
            self.tx.send(Task::Quit).unwrap();
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
            rx
        }
    }
    fn run(&mut self){
        let mut client = Client::new(&self.addr);
        let rx = Arc::clone(&self.rx);
        let handle = thread::spawn(move || loop {
            match rx.lock().unwrap().recv().unwrap() {
                Task::Set(key) => client.set(key).unwrap(),
                Task::Quit => break
            };
        });
        self.thread = Some(handle);
    }
    fn quit(&mut self){
        match self.thread.take() {
            Some(handle) => handle.join().unwrap(),
            _ => {}
        };
    }
}

struct Client {
    conn: redis::Connection
}

impl Client {
    fn new(addr: &str) -> Self {
        let conn = redis::Client::open(addr)
            .expect(&format!("ERR: bad address `{}`", addr))
            .get_connection()
            .expect("ERR: could not establish connection with redis server");
        Client { conn }

    }
    fn set(&mut self, key: usize) -> RedisResult<()> {
        self.conn.set(key, key)
    }
}

fn main(){
    let start = Instant::now();
    workload();
    let duration = start.elapsed();
    println!("time: {:?}", duration);
}

fn workload() {
    let m = Manager::new(6, "redis://127.0.0.1/");
    for i in 0..5000 {
        m.dispatch(Task::Set(i));
    }
}