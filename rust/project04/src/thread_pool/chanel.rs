use std::sync::mpsc;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;

use super::ThreadPool;
use crate::Result;

enum Message {
    NewJob(Job),
    Terminate,
}

pub struct ChanelThreadPool {
    workers: Vec<Worker>,
    sender: mpsc::Sender<Message>,
}

trait FnBox {
    fn call_box(self: Box<Self>);
}

impl<F: FnOnce()> FnBox for F {
    fn call_box(self: Box<F>) {
        (*self)()
    }
}

type Job = Box<dyn FnBox + Send + 'static>;

impl ThreadPool for ChanelThreadPool {
    /// create thread pool
    ///
    /// the number of worker
    ///
    /// # Panics
    ///
    /// `new` if size <= 0, will panic。
    fn new(size: u32) -> Result<Self> {
        assert!(size > 0);

        let (sender, receiver) = mpsc::channel();

        let receiver = Arc::new(Mutex::new(receiver));

        let mut workers = Vec::with_capacity(size as usize);

        for id in 0..size {
            workers.push(Worker::new(id as usize, Arc::clone(&receiver)));
        }

        Ok(ChanelThreadPool { workers, sender })
    }

    fn spawn<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let job = Box::new(f);

        self.sender.send(Message::NewJob(job)).unwrap();
    }
}

impl Drop for ChanelThreadPool {
    fn drop(&mut self) {
        info!("Sending terminate message to all workers.");

        for _ in &mut self.workers {
            self.sender.send(Message::Terminate).unwrap();
        }

        info!("Shutting down all workers.");

        for worker in &mut self.workers {
            info!("Shutting down worker {}", worker.id);

            if let Some(thread) = worker.thread.take() {
                thread.join().unwrap();
            }
        }
    }
}

struct Worker {
    id: usize,
    thread: Option<thread::JoinHandle<()>>,
}

impl Worker {
    fn new(id: usize, receiver: Arc<Mutex<mpsc::Receiver<Message>>>) -> Worker {
        let thread = thread::spawn(move || loop {
            let message = receiver.lock().unwrap().recv().unwrap();

            match message {
                Message::NewJob(job) => {
                    debug!("Worker {} got a job; executing.", id);

                    job.call_box();
                }
                Message::Terminate => {
                    info!("Worker {} was told to terminate.", id);

                    break;
                }
            }
        });

        Worker {
            id,
            thread: Some(thread),
        }
    }
}

#[cfg(test)]
mod test_chanel_thread_pool {
    use super::*;

    #[test]
    fn test_exec() {
        let pool = ChanelThreadPool::new(4).unwrap();
        for i in 0..4 {
            pool.spawn(move || {
                println!("hello {}", i);
            });
        }
    }
}
