use mysql::*;
use mysql::prelude::*;
use std::sync::mpsc::{sync_channel, SyncSender, TryRecvError};
use std::net::{TcpListener, TcpStream, SocketAddr};
use std::io::{ErrorKind, Write};
use std::collections::VecDeque;
use std::time::{Instant, Duration};
use popol;

type Result<R, E> = std::result::Result<R, E>;

fn main() -> Result<(), Box<dyn std::error::Error>> {

	let mut args = std::env::args().skip(1);
	let listen_port = args.next().map(|s| s.parse()).unwrap_or(Ok(8888))?;
	let mysql_port = args.next().map(|s| s.parse()).unwrap_or(Ok(3306))?;
	let server_id = args.next().map(|s| s.parse()).unwrap_or(Ok(100))?;

	/// The identifier we'll use with `popol` to figure out the source of an event.
	/// The `K` in `Sources<K>`.
	#[derive(Eq, PartialEq, Clone)]
	enum Source {
		/// An event from a connected peer.
		Peer(SocketAddr),
		/// An event on the listening socket. Most probably a new peer connection.
		Listener,
		Waker,
	}
	let mut sources = popol::Sources::new();

	let (tx, rx) = sync_channel(4);
	let waker = popol::Waker::new(&mut sources, Source::Waker)?;

	std::thread::spawn(move || {
		if let Err(e) = receive_binlog(waker, tx, mysql_port, server_id) {
			println!("error receiving binary log: {:?}", e);
		}
	});


	// server

	let listener = TcpListener::bind(format!("0.0.0.0:{}", listen_port))?;
	let mut events = popol::Events::new();

	// Register the listener socket, using the corresponding identifier.
	sources.register(Source::Listener, &listener, popol::interest::READ);

	// It's important to set the socket in non-blocking mode. This allows
	// us to know when to stop accepting connections.
	listener.set_nonblocking(true)?;

	let mut clients = Vec::with_capacity(4);
	let mut last_gtid : Option<Gtid> = None;

	let mut delay_list = VecDeque::<(Instant, Gtid)>::new();

	// bootstrap: ST= uuid:from-to,uuid:from-to
	// update: I1= uuid:id
	// update: I2= id

	'a: loop {
		// Wait for something to happen on our sources.
		if let Some((until, _)) = delay_list.front() {
			sources.wait_timeout(&mut events, until.duration_since(Instant::now()))?;
		} else {
			sources.wait(&mut events)?;
		}

		while let Some((until, _)) = delay_list.front() {
			if until > &Instant::now() {
				break;
			}
			handle_gtid(delay_list.pop_front().unwrap().1, &mut last_gtid, &clients)?;
		}

		for (key, event) in events.iter() {
			match key {
				Source::Listener => loop {
					// Accept as many connections as we can.
					let (mut conn, addr) = match listener.accept() {
						Ok((conn, addr)) => (conn, addr),
						Err(e) if e.kind() == ErrorKind::WouldBlock => break,
						Err(e) => return Err(e)?,
					};
					// Register the new peer using the `Peer` variant of `Source`.
					sources.register(
						Source::Peer(addr),
						&conn,
						popol::interest::READ
					);
					if let Some(last) = last_gtid.as_ref() {
						conn.write(format!("ST={}_{}:{}-{}\n", last.domain, last.server, last.id, last.id).as_bytes())?;
					}
					clients.push(conn);

					println!("got {} clients", clients.len());
				}
				Source::Peer(addr) => {
					println!("{} has data to be read. event: {:?}", addr, event);
					clients.retain(|c| { println!("peer is {:?} {:?}", c.peer_addr(), addr); c.peer_addr().ok().map(|a| &a == addr) == Some(false) });
					sources.unregister(key);
				}
				Source::Waker => {
					match rx.try_recv() {
						Ok(gtid) => {
							if server_id == 100 {
								delay_list.push_back((Instant::now() + Duration::from_secs(30), gtid));
							} else {
								handle_gtid(gtid, &mut last_gtid, &clients)?;
							}
						},
						Err(TryRecvError::Empty) => (),
						Err(TryRecvError::Disconnected) => break 'a,
					};
				}
			}
		}
	}
	
	Ok(())
}

fn handle_gtid(gtid: Gtid, last_gtid: &mut Option<Gtid>, clients: &[TcpStream]) -> Result<(), Box<dyn std::error::Error>> {
	println!("got new id {}", gtid);
	let msg = match last_gtid {
		Some(last) if gtid.domain == last.domain && gtid.server == last.server =>
			format!("I2={}\n", gtid.id),
		_ =>
			format!("I1={}_{}:{}\n", gtid.domain, gtid.server, gtid.id),
	};
	for mut client in clients.iter() {
		client.write(msg.as_bytes())?;
	}
	*last_gtid = Some(gtid);

	Ok(())
}

#[derive(Debug, Clone)]
struct Gtid {
	domain: u32,
	server: u32,
	id: u64,
}

impl std::str::FromStr for Gtid {
	type Err = &'static str;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let map_err = |_| "invalid GTID syntax";
		let mut split = s.split('-');
		if let (Some(domain), Some(server), Some(id)) = (split.next(), split.next(), split.next()) {
			Ok(Gtid {
				domain: domain.parse().map_err(map_err)?,
				server: server.parse().map_err(map_err)?,
				id: id.parse().map_err(map_err)?
			})
		} else {
			Err("invalid GTID syntax")
		}
	}
}

use std::fmt::{Display, Formatter};
impl Display for Gtid {
	fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
		write!(f, "{}-{}-{}", self.domain, self.server, self.id)
	}
}

fn receive_binlog(waker: popol::Waker, tx: SyncSender<Gtid>, port: u16, server_id: u32) -> Result<(), Box<dyn std::error::Error>> {
	let url = format!("mysql://replication_user:replication@localhost:{}/?prefer_socket=false", port);
	let mut connection = Conn::new(&url[..])?;

	let mut gtid : Gtid = connection.query_first::<String, _>("select @@gtid_binlog_pos")?.unwrap().parse()?;
	println!("starting from: {:?}", gtid);
	//let uuid = 

	connection.query_drop("set @mariadb_slave_capability=4")?;
	connection.query_drop(format!("set @slave_connect_state='{}'", gtid))?;
	connection.query_drop("set @slave_gtid_strict_mode=1")?;

	let mut binlog = connection.get_binlog_stream(BinlogRequest::new(server_id))?;
	// .with_flags(BinlogDumpFlags::BINLOG_DUMP_NON_BLOCK))?; <- this closes the connection if it would block :(

	tx.send(gtid.clone())?;
	waker.wake().unwrap();

	while let Some(entry) = binlog.next() {
		match entry {
			Ok(entry) => {
				// TODO detect repliation domain or server id change (uuid in mysql terms)
				println!("type: {:?}, server: {}", entry.header().event_type(), entry.header().server_id());
				if entry.header().event_type() == Err(mysql::binlog::UnknownEventType(0xa2)) {
					gtid.id = u64::from_le_bytes(entry.data()[0..8].try_into().unwrap());
					gtid.server = entry.header().server_id();
					println!("gtid is: {}", gtid);
					tx.send(gtid.clone())?;
					waker.wake().unwrap();
				}
			},
			Err(err) => println!("got error {:?}", err),
		};
	}
	Ok(())
}

