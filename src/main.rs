use byteorder::{ByteOrder, BigEndian};
use mysql::prelude::*;
use mysql::*;
use std::collections::VecDeque;
use std::io::{ErrorKind, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::mpsc::{sync_channel, SyncSender, TryRecvError};
use std::time::{Duration, Instant};
use uuid::Uuid;

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

	let (tx_initial_gtids, rx_initial_gtids) = sync_channel(1);

	let mysql_thread = std::thread::spawn(move || {
		if let Err(e) = receive_binlog(waker, tx, tx_initial_gtids, &mysql_url, server_id) {
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
	let mut known_gtids = Gtids::new(&rx.recv()?); // await first gtid before accepting any connections

	if let Ok(gtids) = rx_initial_gtids.try_recv() {
		// in this case we can safely ignore the first sent gtid because it's already in the initial_gtids
		known_gtids = gtids;
	}

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
			handle_gtid(
				delay_list.pop_front().unwrap().1,
				&mut known_gtids,
				&clients
			)?;
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
	let msg = if gtids.add(&gtid) {
		format!("I2={}\n", gtid.id)
	} else {
		format!("I1={}:{}\n", gtid.uuid.as_simple(), gtid.id)
	};
	//println!("gtids are now {:?}", gtids.servers[0]);
	for mut client in clients.iter() {
		client.write_all(msg.as_bytes())?;
	}

	Ok(())
}

#[derive(Debug, Clone)]
struct Gtid {
	uuid: Uuid,
	id: u64,
}

impl Gtid {
	fn new(uuid: Uuid, id: u64) -> Gtid {
		Gtid { uuid, id }
	}

	fn set_server(&mut self, server_id: u32) {
		use uuid::Bytes;
		let bytes =
			unsafe { &mut *(self.uuid.as_bytes() as *const Bytes as *mut Bytes) }; // safe because uuid is &mut - unfortunately uuid has no update or as_bytes_mut() fn's
		BigEndian::write_u32(&mut bytes[12..16], server_id);
	}
}
impl std::str::FromStr for Gtid {
	type Err = &'static str;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let map_err = |_| "invalid GTID syntax";
		let mut split = s.split('-');
		if let (Some(domain), Some(server), Some(id)) = (split.next(), split.next(), split.next()) {
			let mut buf = [0; 8];
			BigEndian::write_u32(&mut buf[4..8], server.parse().map_err(map_err)?);
			Ok(Gtid {
				uuid: Uuid::from_fields(
					domain.parse().map_err(map_err)?,
					0,
					0,
					&buf
				),
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
		write!(f, "{}:{}", self.uuid, self.id)
	}
}

#[derive(Debug)]
struct Gtids {
	servers: Vec<GtidsPerServer>,
}

impl Gtids {
	fn new(gtid: &Gtid) -> Gtids {
		Gtids {
			servers: vec![GtidsPerServer::new(gtid)],
		}
	}

	fn add(&mut self, gtid: &Gtid) -> bool {
		if let Some((i, server)) = self
			.servers
			.iter_mut()
			.enumerate()
			.find(|(_, s)| s.uuid == gtid.uuid)
		{
			server.add(gtid.id);
			if i == 0 {
				return true;
			}
			self.servers[0..=i].rotate_right(1); // keep latest server at the beginning
			//self.servers.swap(i, self.servers.len() - 1); // keep latest server at the end
			false
		} else {
			self.servers
				.insert(0, GtidsPerServer::new(gtid));
			if self.servers.len() > 10 {
				self.servers.remove(self.servers.len() - 1);
			}
			false
		}
	}

	// attention: gtids returned by mysql (e.g. @@gtid_executed) are ordered lexically by uuid, not by age as we do it!
	fn any_gtid(&self) -> Gtid {
		Gtid::new(self.servers[0].uuid, self.servers[0].id_ranges.last().unwrap().last)
	}
}

impl std::str::FromStr for Gtids {
	type Err = &'static str;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let map_err = |_| "invalid GTIDs syntax";
		let server_strings = s.split(',').map(|s| s.trim()).filter(|s| !s.is_empty());
		let mut servers = Vec::with_capacity(s.chars().filter(|c| *c == ',').count());
		for server_string in server_strings {
			let mut split = server_string.split(':');
			if let Some(uuid) = split.next() {
				let uuid = uuid.parse().map_err(|_| "UUID in GTIDs invalid")?;
				let mut id_ranges = Vec::with_capacity(server_string.chars().filter(|c| *c == ':').count() - 1);
				for id_range in split {
					// TODO what about domains? (currently ignored because we don't use them)
					let mut split = id_range.split('-');
					if let (Some(first), Some(last)) = (split.next(), split.next()) {
						id_ranges.push(IdRange {
							first: first.parse().map_err(map_err)?,
							last: last.parse().map_err(map_err)?,
						});
					} else {
						return Err("invalid GTID syntax");
					}
				}
				servers.push(GtidsPerServer {
					uuid,
					id_ranges,
				});
			} else {
				return Err("invalid GTID syntax");
			}
		}
		//println!("parsed {} to {:?}", s, servers);
		Ok(Gtids { servers })
	}
}


impl Display for Gtids {
	fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
		for server in self.servers.iter().rev() { // .rev() because latest server is at beginning and we want to send the latest last to proxysql
			for ids in server.id_ranges.iter() {
				write!(f, "{}:{}-{},", server.uuid, ids.first, ids.last)?;
			}
		}
		Ok(())
	}
}

#[derive(Debug)]
struct GtidsPerServer {
	uuid: Uuid,
	id_ranges: Vec<IdRange>,
}

impl GtidsPerServer {
	fn new(gtid: &Gtid) -> GtidsPerServer {
		GtidsPerServer {
			uuid: gtid.uuid,
			id_ranges: vec![IdRange::new(gtid.id)],
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

#[derive(Debug)]
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

fn receive_binlog(
	waker: popol::Waker,
	tx: SyncSender<Gtid>,
	tx_initial_gtids: SyncSender<Gtids>,
	url: &str,
	server_id: u32,
) -> Result<(), Box<dyn std::error::Error>> {
	/*let mut connection = Conn::new(OptsBuilder::from_opts(Opts::try_from(url)?)
		.additional_capabilities(mysql::consts::CapabilityFlags::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA)
		)?;*/
	let mut connection = Conn::new(url)?;

	let is_mariadb = connection
		.query_first::<String, _>("select @@version")?
		.unwrap()
		.to_lowercase()
		.contains("mariadb");

	let (mut gtid, request) = if is_mariadb {
		prepare_binlog_request_mariadb(&mut connection, server_id, &tx)?
	} else {
		prepare_binlog_request_oracle(&mut connection, server_id, &tx, tx_initial_gtids)?
	};
	waker.wake().unwrap();

	//println!("prepared, now requesting binlog");

	let binlog = connection.get_binlog_stream(request)?;
	// .with_flags(BinlogDumpFlags::BINLOG_DUMP_NON_BLOCK))?; <- this closes the connection if it would block :(

	for entry in binlog {
		match entry {
			Ok(entry) => {
				//println!("type: {:?}, server: {}", entry.header().event_type(), entry.header().server_id());
				match entry.header().event_type() {
					Err(mysql::binlog::UnknownEventType(0xa2)) => {
						gtid.id = u64::from_le_bytes(entry.data()[0..8].try_into().unwrap());
						gtid.set_server(entry.header().server_id());
						//println!("gtid is: {}", gtid);
						tx.send(gtid.clone())?;
						waker.wake().unwrap();
					},
					Ok(mysql::binlog::EventType::GTID_EVENT) => {
						let gtid_event : mysql::binlog::events::GtidEvent = entry.read_event()?;
						if &gtid_event.sid() != gtid.uuid.as_bytes() {
							println!("uuid changed from {} to {}", gtid, Gtid::new(Uuid::from_slice(&gtid_event.sid())?, gtid_event.gno()));
							gtid = Gtid::new(Uuid::from_slice(&gtid_event.sid())?, gtid_event.gno());
						} else {
							gtid.id = gtid_event.gno();
						}
						//println!("gtid is: {}", gtid);
						tx.send(gtid.clone())?;
						waker.wake().unwrap();
					},
					_ => {},
				};
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

fn prepare_binlog_request_mariadb(connection: &mut Conn, server_id: u32, tx: &SyncSender<Gtid>) -> Result<(Gtid, BinlogRequest<'static>), Box<dyn std::error::Error>> {
	let gtid : Gtid = match connection.query_first::<String, _>("select @@gtid_binlog_pos")?.unwrap() {
		gtid if gtid.is_empty() => {
			println!("WARNING: server reports empty gtid_binlog_pos - probably it hasn't replicated any transaction yet. Using gtid_slave_pos instead.");
			connection.query_first::<String, _>("select @@gtid_slave_pos")?.unwrap()
		},
		gtid => gtid,
	}.parse()?;
	let start_with_logfile = find_binlog_filename(connection, 1024 * 1024 * 10);

	let mut request = BinlogRequest::new(server_id);
	connection.query_drop("set @mariadb_slave_capability=4")?;
	if let Some(name) = start_with_logfile.as_ref() {
		println!("starting with logfile {}", name);
		request = request.with_filename(name.clone().into_bytes()).with_pos(4u64); // logs star at position 4 (after magic bytes)
	} else {
		println!("starting from gtid {}", gtid);
		connection.query_drop(format!("set @slave_connect_state='{}'", gtid))?;

		tx.send(gtid.clone())?;
	}
	connection.query_drop("set @slave_gtid_strict_mode=1")?;

	Ok((gtid, request))
}

fn prepare_binlog_request_oracle(connection: &mut Conn, server_id: u32, tx: &SyncSender<Gtid>, tx_initial_gtids: SyncSender<Gtids>) -> Result<(Gtid, BinlogRequest<'static>), Box<dyn std::error::Error>> {
	let (file, position, _, _, gtids) = connection.query_first::<(String, u64, String, String, String), _>("show binary log status")?.unwrap();
	let gtids : Gtids = gtids.parse()?;

	println!("starting from file {} at position {}", file, position);
	let request = BinlogRequest::new(server_id)
		.with_use_gtid(true)
		.with_filename(file.into_bytes())
		.with_pos(position);
	let any_gtid = gtids.any_gtid();
	tx_initial_gtids.send(gtids)?;
	tx.send(any_gtid.clone())?;

	Ok((any_gtid, request))
}
