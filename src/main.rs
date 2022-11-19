use mysql::prelude::*;
use mysql::*;
use std::collections::VecDeque;
use std::io::{ErrorKind, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::mpsc::{sync_channel, SyncSender, TryRecvError};
use std::time::{Duration, Instant};

type Result<R, E> = std::result::Result<R, E>;

fn parse_args() -> Result<(String, u16, u32, Duration), Box<dyn std::error::Error>> {
	let mut args = std::env::args().skip(1);
	let mysql_url = match args.next() {
		Some(env) if env == "env" => match std::env::var("PROXYSQL_BINLOG_READER_MYSQL_URL") {
			Err(_) => Err("missing environment variable PROXYSQL_BINLOG_READER_MYSQL_URL")?,
			Ok(e) => e,
		},
		Some(url) => url,
		None => Err("missing mandatory first parameter")?,
	};
	let listen_port = args.next().map(|s| s.parse()).unwrap_or(Ok(8888))?;
	let server_id = args.next().map(|s| s.parse()).unwrap_or(Ok(100))?;
	let simulated_delay = Duration::from_millis(args.next().map(|s| s.parse()).unwrap_or(Ok(0))?);

	Ok((mysql_url, listen_port, server_id, simulated_delay))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
	let (mysql_url, listen_port, server_id, simulated_delay) = match parse_args() {
		Err(e) => {
			println!("Error parsing arguments: {}", e);
			println!("\nSyntax: {} {{\"env\" | mysql connection url}} [port to listen on for proxysql connections] [our slave server_id] [simulated delay in ms]", std::env::args().next().unwrap());
			println!("\texample mysql connection url: mysql://replication_user:replication_pw@localhost:3306/?prefer_socket=false");
			return Ok(());
		}
		Ok(p) => p,
	};

	/// The identifier we'll use with `popol` to figure out the source of an event.
	/// The `K` in `Sources<K>`.
	#[derive(Eq, PartialEq, Clone, Debug)]
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

	let mysql_thread = std::thread::spawn(move || {
		if let Err(e) = receive_binlog(waker, tx, &mysql_url, server_id) {
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

	let mut delay_list = VecDeque::<(Instant, Gtid)>::new();
	let mut known_gtids = Gtids::new(rx.recv()?); // await first gtid before accepting any connections

	// bootstrap: ST= uuid:from-to,uuid:from-to (uuid including dashes)
	// update: I1= uuid:id (uuid without dashes)
	// update: I2= id

	'a: loop {
		// Wait for something to happen on our sources.
		if let Some((until, _)) = delay_list.front() {
			match sources.wait_timeout(&mut events, until.duration_since(Instant::now())) {
				Ok(()) => {}
				Err(err) if err.kind() == std::io::ErrorKind::TimedOut => {}
				Err(err) => Err(err)?,
			};
		} else {
			sources.wait(&mut events)?;
		}

		while let Some((until, _)) = delay_list.front() {
			if until > &Instant::now() {
				break;
			}
			handle_gtid(delay_list.pop_front().unwrap().1, &mut known_gtids, &clients)?;
		}

		for (key, event) in events.iter() {
			//println!("event: key {:?}, event {:?}", key, event);
			match key {
				Source::Listener => loop {
					// Accept as many connections as we can.
					let (mut conn, addr) = match listener.accept() {
						Ok((conn, addr)) => (conn, addr),
						Err(e) if e.kind() == ErrorKind::WouldBlock => break,
						Err(e) => return Err(e)?,
					};
					// Register the new peer using the `Peer` variant of `Source`.
					sources.register(Source::Peer(addr), &conn, popol::interest::READ);
					conn.write_all(format!("ST={}\n", known_gtids).as_bytes())?;
					clients.push(conn);

					println!("got {} clients", clients.len());
				},
				Source::Peer(addr) => {
					println!("{} has data to be read. event: {:?}", addr, event);
					clients.retain(|c| {
						println!("peer is {:?} {:?}", c.peer_addr(), addr);
						c.peer_addr().ok().map(|a| &a == addr) == Some(false)
					});
					sources.unregister(key);
				}
				Source::Waker => {
					if popol::Waker::reset(event.source).is_err() {
						break 'a; // waker dropped - mysql thread finished
					}
					loop { // waker may join multiple calls into one wakeup - loop to read them all
						match rx.try_recv() {
							Ok(gtid) => {
								if simulated_delay.is_zero() {
									handle_gtid(gtid, &mut known_gtids, &clients)?;
								} else {
									delay_list.push_back((Instant::now() + simulated_delay, gtid));
								}
							}
							Err(TryRecvError::Empty) => break,
							Err(TryRecvError::Disconnected) => break 'a,
						};
					}
				}
			}
		}
	}

	mysql_thread.join().unwrap();

	Ok(())
}

fn handle_gtid(
	gtid: Gtid,
	gtids: &mut Gtids,
	clients: &[TcpStream],
) -> Result<(), Box<dyn std::error::Error>> {
	assert_eq!(gtid.domain, gtids.domain);
	let msg = if gtids.add(&gtid) {
		format!("I2={}\n", gtid.id)
	} else {
		format!("I1={}:{}\n", gtid.to_uuid_without_dashes(), gtid.id)
	};
	for mut client in clients.iter() {
		client.write_all(msg.as_bytes())?;
	}

	Ok(())
}

#[derive(Debug, Clone)]
struct Gtid {
	domain: u32,
	server: u32,
	id: u64,
}

impl Gtid {
	fn to_uuid(&self) -> Uuid<'_> {
		Uuid { gtid: self }
	}
	fn to_uuid_without_dashes(&self) -> UuidWithoutDashes<'_> {
		UuidWithoutDashes { gtid: self }
	}
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
				id: id.parse().map_err(map_err)?,
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

struct Gtids {
	domain: u32,
	servers: Vec<GtidsPerServer>,
}

impl Gtids {
	fn new(gtid: Gtid) -> Gtids {
		Gtids {
			domain: gtid.domain,
			servers: vec![GtidsPerServer::new(gtid.server, gtid.id)],
		}
	}

	fn add(&mut self, gtid: &Gtid) -> bool {
		assert_eq!(self.domain, gtid.domain); // domain should never change
		if let Some((i, server)) = self
			.servers
			.iter_mut()
			.enumerate()
			.find(|(_, s)| s.server == gtid.server)
		{
			server.add(gtid.id);
			if i == 0 {
				return true;
			}
			self.servers[0..=i].rotate_right(1); // keep latest server at the beginning
			//self.servers.swap(i, self.servers.len() - 1); // keep latest server at the end
			return false;
		} else {
			self.servers
				.insert(0, GtidsPerServer::new(gtid.server, gtid.id));
			if self.servers.len() > 10 {
				self.servers.remove(self.servers.len() - 1);
			}
			return false;
		}
	}
}

impl Display for Gtids {
	fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
		for server in self.servers.iter().rev() { // .rev() because latest server is at beginning and we want to send the latest last to proxysql
			let gtid = Gtid {
				domain: self.domain,
				server: server.server,
				id: 0,
			};
			for ids in server.id_ranges.iter() {
				write!(f, "{}:{}-{},", gtid.to_uuid(), ids.first, ids.last)?;
			}
		}
		Ok(())
	}
}

struct GtidsPerServer {
	server: u32,
	id_ranges: Vec<IdRange>,
}

impl GtidsPerServer {
	fn new(server: u32, id: u64) -> GtidsPerServer {
		GtidsPerServer {
			server,
			id_ranges: vec![IdRange::new(id)],
		}
	}

	fn add(&mut self, id: u64) {
		assert!(!self.id_ranges.is_empty());
		let next = match self.id_ranges.iter().rposition(|range| range.last < id) {
			None => 0,
			Some(i) => {
				if self.id_ranges[i].last + 1 == id { // new id is one after existing range
					if i + 1 != self.id_ranges.len() && id + 1 == self.id_ranges[i + 1].first { // new id fills gap between two existing ranges
						self.id_ranges[i].last = self.id_ranges[i + 1].last;
						self.id_ranges.remove(i + 1);
					} else {
						self.id_ranges[i].last = id;
					}
					return;
				}
				i + 1
			}
		};

		if next != self.id_ranges.len() && id + 1 == self.id_ranges[next].first { // new id is one before existing range
			self.id_ranges[next].first = id;
		} else if next != self.id_ranges.len() && id >= self.id_ranges[next].first { // id is contained in existing range
			// no action required
			// the rposition condition above said (id_ranges[next].last < id) == false 
			// => id_ranges[next].last >= id
		} else { // insert new range
			self.id_ranges.insert(next, IdRange::new(id));
			if self.id_ranges.len() > 10 {
				self.id_ranges.remove(0);
			}
		}
	}
}

struct IdRange {
	first: u64,
	last: u64,
}

impl IdRange {
	fn new(id: u64) -> IdRange {
		IdRange {
			first: id,
			last: id,
		}
	}
}

struct Uuid<'a> {
	gtid: &'a Gtid,
}

impl<'a> Display for Uuid<'a> {
	fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
		write!(
			f,
			"{:0>8X}-0000-0000-0000-{:0>12X}",
			self.gtid.domain, self.gtid.server
		)
	}
}

struct UuidWithoutDashes<'a> {
	gtid: &'a Gtid,
}

impl<'a> Display for UuidWithoutDashes<'a> {
	fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
		write!(
			f,
			"{:0>8X}000000000000{:0>12X}",
			self.gtid.domain, self.gtid.server
		)
	}
}

fn receive_binlog(
	waker: popol::Waker,
	tx: SyncSender<Gtid>,
	url: &str,
	server_id: u32,
) -> Result<(), Box<dyn std::error::Error>> {
	let mut connection = Conn::new(url)?;

	let mut gtid : Gtid = match connection.query_first::<String, _>("select @@gtid_binlog_pos")?.unwrap() {
		gtid if gtid.is_empty() => {
			println!("WARNING: server reports empty gtid_binlog_pos - probably it hasn't replicated any transaction yet. Using gtid_slave_pos instead.");
			connection.query_first::<String, _>("select @@gtid_slave_pos")?.unwrap()
		},
		gtid => gtid,
	}.parse()?;
	let start_with_logfile = find_binlog_filename(&mut connection, 1024 * 1024 * 10);

	let mut request = BinlogRequest::new(server_id);
	connection.query_drop("set @mariadb_slave_capability=4")?;
	if let Some(name) = start_with_logfile.as_ref() {
		println!("starting with logfile {}", name);
		request = request.with_filename(name.as_bytes()).with_pos(4u64); // logs star at position 4 (after magic bytes)
	} else {	
		println!("starting from gtid {}", gtid);
		connection.query_drop(format!("set @slave_connect_state='{}'", gtid))?;

		tx.send(gtid.clone())?;
		waker.wake().unwrap();
	}
	connection.query_drop("set @slave_gtid_strict_mode=1")?;

	let binlog = connection.get_binlog_stream(request)?;
	// .with_flags(BinlogDumpFlags::BINLOG_DUMP_NON_BLOCK))?; <- this closes the connection if it would block :(

	for entry in binlog {
		match entry {
			Ok(entry) => {
				//println!("type: {:?}, server: {}", entry.header().event_type(), entry.header().server_id());
				if entry.header().event_type() == Err(mysql::binlog::UnknownEventType(0xa2)) {
					gtid.id = u64::from_le_bytes(entry.data()[0..8].try_into().unwrap());
					gtid.server = entry.header().server_id();
					//println!("gtid is: {}", gtid);
					tx.send(gtid.clone())?;
					waker.wake().unwrap();
				}
			}
			Err(err) => println!("got error {:?}", err),
		};
	}
	Ok(())
}

fn find_binlog_filename(connection: &mut Conn, approx_byte_distance: u64) -> Option<String> {
	#[derive(Debug)]
	struct BinaryLog {
		name: String,
		size: u64,
	}

	let mut logs = connection
		.query_map("show binary logs", |(name, size)| BinaryLog { name, size })
		.ok()?
		.into_iter();
	let mut size = 0;
	let first = logs.next();
	for log in logs.rev() {
		size += log.size;
		if size > approx_byte_distance {
			return Some(log.name);
		}
	}

	first.map(|l| l.name)
}

