use async_io::Async;
use bytes::{BufMut, Bytes, BytesMut};
use futures_lite::{AsyncRead, AsyncReadExt};
use log::trace;
use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::time::Duration;
use std::{cmp, io, slice};

use crate::packet::{Opts, Packet, Request, PACKET_DATA_HEADER_LEN};
use crate::utils::io_timeout;
use crate::{Error, Result};

use super::server::{ServerConfig, DEFAULT_BLOCK_SIZE};

pub(crate) struct ReadRequest<'r, R>
where
    R: AsyncRead + Send,
{
    peer: SocketAddr,
    socket: Async<UdpSocket>,
    reader: &'r mut R,
    buffer: BytesMut,
    block_size: usize,
    timeout: Duration,
    max_send_retries: u32,
    oack_opts: Option<Opts>,
}

impl<'r, R> ReadRequest<'r, R>
where
    R: AsyncRead + Send + Unpin,
{
    pub(crate) async fn init(
        reader: &'r mut R,
        file_size: Option<u64>,
        peer: SocketAddr,
        req: &Request,
        config: ServerConfig,
        local_ip: IpAddr,
    ) -> Result<ReadRequest<'r, R>> {
        let oack_opts = build_oack_opts(&config, req, file_size);

        let block_size = oack_opts
            .as_ref()
            .and_then(|o| o.block_size)
            .map(usize::from)
            .unwrap_or(DEFAULT_BLOCK_SIZE);

        let timeout = oack_opts
            .as_ref()
            .and_then(|o| o.timeout)
            .map(|t| Duration::from_secs(u64::from(t)))
            .unwrap_or(config.timeout);

        let addr = SocketAddr::new(local_ip, 0);
        let socket = Async::<UdpSocket>::bind(addr).map_err(Error::Bind)?;

        Ok(ReadRequest {
            peer,
            socket,
            reader,
            buffer: BytesMut::with_capacity(block_size + PACKET_DATA_HEADER_LEN),
            block_size,
            timeout,
            max_send_retries: config.max_send_retries,
            oack_opts,
        })
    }

    pub(crate) async fn handle(&mut self) {
        if let Err(e) = self.try_handle().await {
            trace!("RRQ request failed (peer: {}, err: {})", &self.peer, &e);

            Packet::Error(e.into()).encode(&mut self.buffer);
            let buf = self.buffer.split().freeze();
            let _ = self.socket.send_to(&buf[..], self.peer).await;
        }
    }

    async fn try_handle(&mut self) -> Result<()> {
        let mut block: u16 = 0;

        // Send file to client
        loop {
            // Reclaim buffer
            self.buffer
                .reserve(PACKET_DATA_HEADER_LEN + self.block_size);

            // Encode head of Data packet
            block = block.wrapping_add(1);
            Packet::encode_data_head(block, &mut self.buffer);

            // Read block in self.buffer
            let last_block;
            let buf = unsafe {
                let uinit_buf = self.buffer.chunk_mut();
                let data_buf = slice::from_raw_parts_mut(uinit_buf.as_mut_ptr(), uinit_buf.len());
                let len = self.read_block(data_buf).await?;
                last_block = len < self.block_size;
                self.buffer.advance_mut(len);
                self.buffer.split().freeze()
            };

            // Send OACK after we manage to read the first block from reader.
            //
            // We do this because we want to give the developers the option to
            // produce an error after they construct a reader.
            if let Some(opts) = self.oack_opts.take() {
                trace!("RRQ OACK (peer: {}, opts: {:?}", &self.peer, &opts);

                let mut buf = BytesMut::new();
                Packet::OAck(opts.to_owned()).encode(&mut buf);
                self.send(buf.split().freeze(), 0).await?;
            }

            trace!(
                "RRQ (peer: {}, block: {}, len: {}) - Sent Data",
                &self.peer,
                block,
                buf.len(),
            );

            // Send Data packet
            self.send(buf, block).await?;
            if last_block {
                break;
            }
        }

        trace!("RRQ request served (peer: {})", &self.peer);
        Ok(())
    }

    async fn send(&mut self, packet: Bytes, block: u16) -> Result<()> {
        // Send packet until we receive an ack
        for _ in 0..=self.max_send_retries {
            self.socket.send_to(&packet[..], self.peer).await?;

            match self.recv_ack(block).await {
                Ok(_) => {
                    trace!("RRQ ACK (peer: {}, block: {})", &self.peer, block);
                    return Ok(());
                }
                Err(ref e) if e.kind() == io::ErrorKind::TimedOut => {
                    trace!("RRQ ACK timeout (peer: {}, block: {})", &self.peer, block);
                    continue;
                }
                Err(e) => return Err(e.into()),
            }
        }
        Err(Error::MaxSendRetriesReached(self.peer, block))
    }

    async fn recv_ack(&mut self, block: u16) -> io::Result<()> {
        let socket = &mut self.socket;
        let peer = self.peer;

        io_timeout(self.timeout, async {
            let mut buf = [0u8; 1024];

            loop {
                let (len, recv_peer) = socket.recv_from(&mut buf[..]).await?;
                if recv_peer != peer {
                    continue;
                }
                if let Ok(Packet::Ack(recv_block)) = Packet::decode(&buf[..len]) {
                    if recv_block == block {
                        return Ok(());
                    }
                }
            }
        })
        .await?;

        Ok(())
    }

    async fn read_block(&mut self, buf: &mut [u8]) -> Result<usize> {
        let mut len = 0;

        while len < buf.len() {
            match self.reader.read(&mut buf[len..]).await? {
                0 => break,
                n => len += n,
            }
        }
        Ok(len)
    }
}

fn build_oack_opts(config: &ServerConfig, req: &Request, file_size: Option<u64>) -> Option<Opts> {
    let mut opts = Opts::default();

    if !config.ignore_client_block_size {
        opts.block_size = match (req.opts.block_size, config.block_size_limit) {
            (Some(block_size), Some(limit)) => Some(cmp::min(block_size, limit)),
            (Some(block_size), None) => Some(block_size),
            _ => None,
        };
    }

    if !config.ignore_client_timeout {
        opts.timeout = req.opts.timeout;
    }

    if let (Some(0), Some(file_size)) = (opts.block_size, file_size) {
        opts.transfer_size = Some(file_size);
    }

    if opts == Opts::default() {
        None
    } else {
        Some(opts)
    }
}
