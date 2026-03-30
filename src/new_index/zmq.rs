use crossbeam_channel::Sender;

use crate::util::spawn_thread;

pub enum ZmqEvent {
    Sequence,
}

pub fn start(url: &str, zmq_event_notify: Sender<ZmqEvent>) {
    log::debug!("Starting ZMQ thread");
    let ctx = zmq::Context::new();
    let subscriber: zmq::Socket = ctx.socket(zmq::SUB).expect("failed creating subscriber");
    subscriber
        .connect(url)
        .expect("failed connecting subscriber");

    subscriber
        .set_subscribe(b"sequence")
        .expect("failed subscribing to sequence");

    spawn_thread("zmq", move || loop {
        match subscriber.recv_multipart(0) {
            Ok(data) => match (data.get(0), data.get(1)) {
                (Some(topic), Some(data)) => {
                    if &topic[..] == b"sequence" {
                        log::debug!("New ZMQ sequence event");
                        if let Err(e) = zmq_event_notify.send(ZmqEvent::Sequence) {
                            log::error!("Failed to send ZMQ event: {e}");
                        }
                    }
                }
                _ => (),
            },
            Err(e) => log::warn!("recv_multipart error: {e:?}"),
        }
    });
}
