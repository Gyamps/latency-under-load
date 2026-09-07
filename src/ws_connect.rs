// This module handles the websocket connection.
// It is an isolated connection manager. The underlying
// network connection is decoupled from that of the
// application logic so the rest of the application
// doesn't have to deal with reconnects and the like.
//
// The `supervisor` handles all of this. A Client side
// is exposed so parts of the application that need to
// connect to some websocket or subscribe to an exsiting
// one can do so.
// The supervisor task is a single tokio background task
// running in an infinite loop and is the only component
// allowed to open, close, write to, or read from the
// raw `WebSocketStream`.

use futures_util::{SinkExt, StreamExt};
use rand::RngExt;
use std::{collections::VecDeque, time::Duration};
use tokio::sync::{broadcast, mpsc};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use url::Url;

#[derive(Debug, Clone)]
pub enum ServerEvent {
    Connected,
    Disconnected { reason: String },
    Message(String),
}

#[derive(Debug, Clone)]
pub struct ClientMsg {
    pub payload: String,
    pub seq: &'static str,
}

pub struct Client {
    tx: mpsc::Sender<ClientMsg>,
    events: broadcast::Sender<ServerEvent>,
}

// public API
// user-facing `Client` hides supervisor
impl Client {
    pub async fn connect(url: &str) -> anyhow::Result<Self> {
        let url = Url::parse(url)?;
        let (tx, rx) = mpsc::channel(128);
        let (event_tx, _) = broadcast::channel(100_000);
        let events = event_tx.clone();
        tokio::spawn(async move {
            supervisor(url, rx, events).await;
        });
        Ok(Self {
            tx,
            events: event_tx,
        })
    }

    pub async fn send(
        &self,
        payload: String,
        seq: &'static str,
    ) -> Result<(), mpsc::error::SendError<ClientMsg>> {
        self.tx.send(ClientMsg { payload, seq }).await
    }

    pub fn subscribe(&self) -> broadcast::Receiver<ServerEvent> {
        self.events.subscribe()
    }
}

fn backoff(attempt: u32) -> Duration {
    let base_ms = 250u64;
    let cap_ms = 30_000u64;
    let exp = base_ms.saturating_mul(1u64 << attempt.min(7));
    let capped = exp.min(cap_ms);
    let jitter: f64 = rand::rng().random_range(0.7..=1.3);
    Duration::from_millis((capped as f64 * jitter) as u64)
}


// The core supervisor is one `loop` with three phases:
// - connect
// - run
// - sleep-and-retry
async fn supervisor(
    url: Url,
    mut rx: mpsc::Receiver<ClientMsg>,
    events: broadcast::Sender<ServerEvent>,
) {
    let mut attempt: u32 = 0;
    let mut in_flight: VecDeque<ClientMsg> = VecDeque::new();

    loop {
        tracing::info!(attempt, "connecting");
        let (ws, _resp) = match connect_async(url.as_str()).await {
            Ok(pair) => pair,
            Err(e) => {
                let wait = backoff(attempt);
                tracing::warn!(?e, ?wait, "connect failed");
                attempt = attempt.saturating_add(1);
                tokio::time::sleep(wait).await;
                continue;
            }
        };

        attempt = 0;
        let _ = events.send(ServerEvent::Connected);
        let (mut sink, mut stream) = ws.split();

        let pending = std::mem::take(&mut in_flight);
        // replay anything we buffered during the outage
        for msg in pending {
            let text_frame = Message::Text(msg.payload.clone().into());

            if sink.send(text_frame).await.is_err() {
                tracing::warn!(seq = msg.seq, "replay failed mid-flight");
                in_flight.push_back(msg);
                break;
            }
        }

        let reason = run_connection(&mut sink, &mut stream, &mut rx, &events, &mut in_flight).await;
        let _ = events.send(ServerEvent::Disconnected { reason });

        let wait = backoff(attempt);
        attempt = attempt.saturating_add(1);
        tokio::time::sleep(wait).await;
    }
}



// Multiplex 3 sources:
// - outbound caller messages which will be very small
// - inbound server frames: the main part of our application
// which has large amounts of data coming in continuously
// - periodic ping timer: if server is quiet (no stream of data),
// we use this to basically make sure all is well 
async fn run_connection(
    sink: &mut (impl SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin),
    stream: &mut (impl StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin),
    rx: &mut mpsc::Receiver<ClientMsg>,
    events: &broadcast::Sender<ServerEvent>,
    in_flight: &mut VecDeque<ClientMsg>,
) -> String {
    let mut ping_tick = tokio::time::interval(Duration::from_secs(15));
    ping_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last_pong = tokio::time::Instant::now();

    loop {
        tokio::select! {
            biased;     // Disable random branching by `select!`

            Some(msg) = rx.recv() => {
                in_flight.push_back(msg.clone());
                match sink.send(Message::Text(msg.payload.into())).await {
                    Ok(()) => {
                        in_flight.retain(|m| m.seq != msg.seq);
                    }
                    Err(e) => {
                        return format!("send failed: {e}");
                    }
                }
            }

            frame = stream.next() => {
                match frame {
                    Some(Ok(Message::Text(t))) => {
                        let _ = events.send(ServerEvent::Message(t.to_string()));
                    }
                    Some(Ok(Message::Pong(_))) => {
                        last_pong = tokio::time::Instant::now();
                    }
                    Some(Ok(Message::Ping(p))) => {
                        if sink.send(Message::Pong(p)).await.is_err() {
                            return "pong send failed".into();
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => return "peer closed".into(),
                    Some(Err(e)) => return format!("stream error: {e}"),
                    _ => {}
                }
            }

            _ = ping_tick.tick() => {
                if last_pong.elapsed() > Duration::from_secs(30) {
                    return "pong deadline exceeded".into();
                }
                if sink.send(Message::Ping(bytes::Bytes::new())).await.is_err() {
                    return "ping send failed".into();
                }
            }
        }
    }
}

