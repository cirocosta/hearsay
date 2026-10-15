use bytes::Bytes;
use rumors::link::routed::{Config, Dial, Endpoint, Incoming, Listen, RoutedLink};
use rumors::{Joined, Peer, Retire, Rumors};
use serde_json::{json, Value};
use std::{
    io::{self, Write},
    net::SocketAddr,
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::{TcpListener, TcpStream},
};
#[derive(Clone)]
struct TcpDial;
impl Dial for TcpDial {
    type Addr = SocketAddr;
    type Conn = TcpStream;
    async fn dial(&self, addr: &SocketAddr) -> io::Result<TcpStream> {
        let c = TcpStream::connect(addr).await?;
        c.set_nodelay(true)?;
        Ok(c)
    }
}
struct TcpListen(TcpListener);
impl Listen for TcpListen {
    type Conn = TcpStream;
    async fn accept(&mut self) -> io::Result<TcpStream> {
        let c = self.0.accept().await?.0;
        c.set_nodelay(true)?;
        Ok(c)
    }
}
struct State {
    endpoint: Endpoint<TcpDial>,
    incoming: Incoming<TcpDial>,
    link: Option<RoutedLink<TcpDial>>,
    peer: Option<Rumors<Bytes>>,
}
impl State {
    fn peer(&self) -> Result<&Rumors<Bytes>, String> {
        self.peer.as_ref().ok_or("no live peer".into())
    }
    async fn command(&mut self, c: Value) -> Result<Value, String> {
        let op = c["op"].as_str().ok_or("missing op")?;
        match op {
            "connect" => {
                let addr = c["address"]
                    .as_str()
                    .ok_or("missing address")?
                    .parse()
                    .map_err(|e| format!("{e}"))?;
                let (_, link) = self
                    .endpoint
                    .link(addr)
                    .await
                    .map_err(|e| format!("{e:?}"))?;
                self.link = Some(link);
                Ok(json!({"ok":true}))
            }
            "accept" => {
                let (_, link) = self.incoming.accept().await.ok_or("router closed")?;
                self.link = Some(link);
                Ok(json!({"ok":true}))
            }
            "bootstrap" => {
                if self.peer.is_some() {
                    return Err("bootstrap requires no live peer".into());
                }
                let link = self.link.as_mut().ok_or("no link")?;
                let mut builder = Peer::<Bytes>::bootstrap();
                if std::env::var("RUST_INTEROP_MINIMAL").as_deref() == Ok("1") {
                    builder = builder.sync_memory_budget(1).target_message_size(0);
                }
                match builder.join(link).await {
                    Joined::Joined { peer } => {
                        self.peer = Some(peer.into_rumors());
                        Ok(json!({"ok":true}))
                    }
                    result => Err(format!("{result:?}")),
                }
            }
            "gossip" => {
                let peer = self.peer.as_ref().ok_or("no peer")?;
                let link = self.link.as_mut().ok_or("no link")?;
                let result = peer.gossip_once(link).await.map_err(|e| format!("{e:?}"))?;
                Ok(json!({"ok":true,"version":hex::encode(result.converged.encode())}))
            }
            "send" => {
                let bytes = hex::decode(c["payload"].as_str().ok_or("missing payload")?)
                    .map_err(|e| e.to_string())?;
                let v = self
                    .peer()?
                    .send(Bytes::from(bytes))
                    .map_err(|e| format!("{e:?}"))?;
                Ok(json!({"ok":true,"version":hex::encode(v.encode())}))
            }
            "redact" => {
                let bytes = hex::decode(c["version"].as_str().ok_or("missing version")?)
                    .map_err(|e| e.to_string())?;
                let v = before::Version::decode(&bytes[..]).map_err(|e| e.to_string())?;
                self.peer()?.redact(&v);
                Ok(json!({"ok":true}))
            }
            "list" => {
                let snap = self.peer()?.snapshot();
                let mut records=snap.iter().map(|(v,p)|json!({"version":hex::encode(v.encode()),"payload":hex::encode(p.as_ref())})).collect::<Vec<_>>();
                records.sort_by_key(|r| r["version"].as_str().unwrap().to_string());
                Ok(
                    json!({"ok":true,"version":hex::encode(snap.latest().encode()),"records":records}),
                )
            }
            "retire" => {
                let link = self.link.as_mut().ok_or("no link")?;
                let peer = self
                    .peer
                    .take()
                    .ok_or("no peer")?
                    .try_into_peer()
                    .await
                    .ok_or("peer reclamation failed")?;
                match peer.retire(link).await {
                    Retire::Retired => Ok(json!({"ok":true,"status":"retired"})),
                    Retire::Declined { peer } => {
                        self.peer = Some(peer.into_rumors());
                        Ok(json!({"ok":true,"status":"declined"}))
                    }
                    Retire::Recovered { peer, error } => {
                        self.peer = Some(peer.into_rumors());
                        Err(format!("recovered {error:?}"))
                    }
                    Retire::Uncertain { error } => Err(format!("uncertain {error:?}")),
                }
            }
            _ => Err(format!("unknown op {op}")),
        }
    }
}
fn emit(v: Value) {
    println!("{v}");
    io::stdout().flush().unwrap()
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (endpoint, incoming, router) =
        Endpoint::new(TcpListen(listener), address, TcpDial, Config::default())?;
    tokio::spawn(router);
    let seed = std::env::args().nth(1).as_deref() == Some("seed");
    let mut state = State {
        endpoint,
        incoming,
        link: None,
        peer: if seed {
            let mut peer = Peer::<Bytes>::seed();
            if std::env::var("RUST_INTEROP_MINIMAL").as_deref() == Ok("1") {
                peer = peer.sync_memory_budget(1).target_message_size(0);
            }
            Some(peer.into_rumors())
        } else {
            None
        },
    };
    emit(json!({"ok":true,"address":address.to_string(),"mode":if seed{"seed"}else{"bootstrap"}}));
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Some(line) = lines.next_line().await? {
        let response = match serde_json::from_str(&line) {
            Err(e) => json!({"ok":false,"error":e.to_string()}),
            Ok(command) => {
                match tokio::time::timeout(Duration::from_secs(20), state.command(command)).await {
                    Ok(Ok(v)) => v,
                    Ok(Err(e)) => json!({"ok":false,"error":e}),
                    Err(_) => json!({"ok":false,"error":"command timeout"}),
                }
            }
        };
        emit(response);
    }
    Ok(())
}
