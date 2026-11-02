//! Пакетный ввод-вывод UDP: sendmmsg/recvmmsg и GSO на Linux, sendmsg_x/recvmsg_x на macOS.
#![allow(clippy::needless_range_loop)]
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use std::io;
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, RawFd};
use std::time::Duration;

pub struct UdpSock {
    sock: Socket,
}

impl UdpSock {
    pub fn bind(addr: SocketAddr, sndbuf: usize, rcvbuf: usize) -> io::Result<Self> {
        let domain = if addr.is_ipv4() { Domain::IPV4 } else { Domain::IPV6 };
        let sock = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?;
        let _ = sock.set_send_buffer_size(sndbuf);
        let _ = sock.set_recv_buffer_size(rcvbuf);
        sock.bind(&SockAddr::from(addr))?;
        Ok(Self { sock })
    }

    pub fn connect(&self, peer: SocketAddr) -> io::Result<()> {
        self.sock.connect(&SockAddr::from(peer))
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.sock.local_addr()?.as_socket().ok_or_else(|| io::Error::other("не IP-адрес"))
    }

    pub fn set_recv_timeout(&self, d: Option<Duration>) -> io::Result<()> {
        self.sock.set_read_timeout(d)
    }

    pub fn buffer_sizes(&self) -> (usize, usize) {
        (self.sock.send_buffer_size().unwrap_or(0), self.sock.recv_buffer_size().unwrap_or(0))
    }

    pub fn fd(&self) -> RawFd {
        self.sock.as_raw_fd()
    }

    /// Отправка одной датаграммы на подключённом сокете.
    pub fn send(&self, buf: &[u8]) -> io::Result<usize> {
        let n = unsafe { libc::send(self.fd(), buf.as_ptr().cast(), buf.len(), 0) };
        if n < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(n as usize)
        }
    }

    pub fn send_to(&self, buf: &[u8], peer: SocketAddr) -> io::Result<usize> {
        self.sock.send_to(buf, &SockAddr::from(peer))
    }

    pub fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        let mb = unsafe { std::slice::from_raw_parts_mut(buf.as_mut_ptr().cast(), buf.len()) };
        let (n, a) = self.sock.recv_from(mb)?;
        let a = a.as_socket().ok_or_else(|| io::Error::other("не IP-адрес"))?;
        Ok((n, a))
    }

    pub fn recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        let n = unsafe { libc::recv(self.fd(), buf.as_mut_ptr().cast(), buf.len(), 0) };
        if n < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(n as usize)
        }
    }

    /// Включает приём с объединением сегментов (UDP_GRO). Только Linux.
    #[cfg(target_os = "linux")]
    pub fn enable_gro(&self) -> io::Result<()> {
        let on: libc::c_int = 1;
        let r = unsafe { libc::setsockopt(self.fd(), libc::SOL_UDP, libc::UDP_GRO, (&on as *const libc::c_int).cast(), 4) };
        if r != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Пакетная отправка: каждая запись буфера станет отдельной датаграммой.
    /// Возвращает, сколько датаграмм принято ядром. Для сокета нужен connect().
    pub fn send_batch(&self, pkts: &[&[u8]]) -> io::Result<usize> {
        if pkts.is_empty() {
            return Ok(0);
        }
        sys::send_batch(self.fd(), pkts)
    }

    /// Отправка одним буфером, разрезанным ядром на сегменты по `seg` байт (UDP_SEGMENT).
    #[cfg(target_os = "linux")]
    pub fn send_gso(&self, buf: &[u8], seg: u16) -> io::Result<usize> {
        sys::send_gso(self.fd(), buf, seg)
    }

    /// Пакетный приём. Заполняет `lens`, возвращает число датаграмм (минимум 1 или ошибка таймаута).
    pub fn recv_batch(&self, rb: &mut RecvBatch) -> io::Result<usize> {
        sys::recv_batch(self.fd(), rb)
    }
}

/// Набор буферов для пакетного приёма.
pub struct RecvBatch {
    pub buf: Vec<u8>,
    pub pkt_size: usize,
    pub count: usize,
    pub lens: Vec<usize>,
}

impl RecvBatch {
    pub fn new(count: usize, pkt_size: usize) -> Self {
        Self { buf: vec![0; count * pkt_size], pkt_size, count, lens: vec![0; count] }
    }

    pub fn pkt(&self, i: usize) -> &[u8] {
        let s = i * self.pkt_size;
        &self.buf[s..s + self.lens[i]]
    }

    pub fn pkt_mut(&mut self, i: usize) -> &mut [u8] {
        let s = i * self.pkt_size;
        let l = self.lens[i];
        &mut self.buf[s..s + l]
    }
}

pub fn is_transient_send_error(e: &io::Error) -> bool {
    matches!(e.raw_os_error(), Some(libc::ENOBUFS) | Some(libc::EAGAIN) | Some(libc::EINTR) | Some(libc::ENOMEM))
}

pub fn is_timeout(e: &io::Error) -> bool {
    matches!(e.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut | io::ErrorKind::Interrupted)
}

#[cfg(target_os = "linux")]
mod sys {
    use super::RecvBatch;
    use std::io;
    use std::os::fd::RawFd;

    pub fn send_batch(fd: RawFd, pkts: &[&[u8]]) -> io::Result<usize> {
        let mut iovs: Vec<libc::iovec> = pkts.iter().map(|p| libc::iovec { iov_base: p.as_ptr() as *mut _, iov_len: p.len() }).collect();
        let mut hdrs: Vec<libc::mmsghdr> = iovs
            .iter_mut()
            .map(|iov| {
                let mut h: libc::mmsghdr = unsafe { std::mem::zeroed() };
                h.msg_hdr.msg_iov = iov;
                h.msg_hdr.msg_iovlen = 1;
                h
            })
            .collect();
        let n = unsafe { libc::sendmmsg(fd, hdrs.as_mut_ptr(), hdrs.len() as u32, 0) };
        if n < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(n as usize)
        }
    }

    pub fn send_gso(fd: RawFd, buf: &[u8], seg: u16) -> io::Result<usize> {
        let mut iov = libc::iovec { iov_base: buf.as_ptr() as *mut _, iov_len: buf.len() };
        let mut ctrl = [0u8; 32];
        let mut h: libc::msghdr = unsafe { std::mem::zeroed() };
        h.msg_iov = &mut iov;
        h.msg_iovlen = 1;
        h.msg_control = ctrl.as_mut_ptr().cast();
        h.msg_controllen = unsafe { libc::CMSG_SPACE(2) } as _;
        unsafe {
            let c = libc::CMSG_FIRSTHDR(&h);
            (*c).cmsg_level = libc::SOL_UDP;
            (*c).cmsg_type = libc::UDP_SEGMENT;
            (*c).cmsg_len = libc::CMSG_LEN(2) as _;
            std::ptr::write_unaligned(libc::CMSG_DATA(c) as *mut u16, seg);
        }
        let n = unsafe { libc::sendmsg(fd, &h, 0) };
        if n < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(n as usize)
        }
    }

    pub fn recv_batch(fd: RawFd, rb: &mut RecvBatch) -> io::Result<usize> {
        let base = rb.buf.as_mut_ptr();
        let mut iovs: Vec<libc::iovec> =
            (0..rb.count).map(|i| libc::iovec { iov_base: unsafe { base.add(i * rb.pkt_size) }.cast(), iov_len: rb.pkt_size }).collect();
        let mut hdrs: Vec<libc::mmsghdr> = iovs
            .iter_mut()
            .map(|iov| {
                let mut h: libc::mmsghdr = unsafe { std::mem::zeroed() };
                h.msg_hdr.msg_iov = iov;
                h.msg_hdr.msg_iovlen = 1;
                h
            })
            .collect();
        let n = unsafe { libc::recvmmsg(fd, hdrs.as_mut_ptr(), hdrs.len() as u32, libc::MSG_WAITFORONE, std::ptr::null_mut()) };
        if n < 0 {
            return Err(io::Error::last_os_error());
        }
        for i in 0..n as usize {
            rb.lens[i] = hdrs[i].msg_len as usize;
        }
        Ok(n as usize)
    }
}

#[cfg(target_os = "macos")]
mod sys {
    use super::RecvBatch;
    use std::io;
    use std::os::fd::RawFd;

    // Описание из xnu bsd/sys/socket.h. Функции экспортирует libsystem_kernel, в SDK их заголовка нет.
    #[repr(C)]
    struct MsghdrX {
        msg_name: *mut libc::c_void,
        msg_namelen: libc::socklen_t,
        msg_iov: *mut libc::iovec,
        msg_iovlen: libc::c_int,
        msg_control: *mut libc::c_void,
        msg_controllen: libc::socklen_t,
        msg_flags: libc::c_int,
        msg_datalen: libc::size_t,
    }

    extern "C" {
        fn sendmsg_x(s: libc::c_int, msgp: *const MsghdrX, cnt: libc::c_uint, flags: libc::c_int) -> isize;
        fn recvmsg_x(s: libc::c_int, msgp: *mut MsghdrX, cnt: libc::c_uint, flags: libc::c_int) -> isize;
    }

    fn hdr(iov: *mut libc::iovec) -> MsghdrX {
        MsghdrX {
            msg_name: std::ptr::null_mut(),
            msg_namelen: 0,
            msg_iov: iov,
            msg_iovlen: 1,
            msg_control: std::ptr::null_mut(),
            msg_controllen: 0,
            msg_flags: 0,
            msg_datalen: 0,
        }
    }

    pub fn send_batch(fd: RawFd, pkts: &[&[u8]]) -> io::Result<usize> {
        let mut iovs: Vec<libc::iovec> = pkts.iter().map(|p| libc::iovec { iov_base: p.as_ptr() as *mut _, iov_len: p.len() }).collect();
        let hdrs: Vec<MsghdrX> = iovs.iter_mut().map(|i| hdr(i)).collect();
        let n = unsafe { sendmsg_x(fd, hdrs.as_ptr(), hdrs.len() as u32, 0) };
        if n < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(n as usize)
        }
    }

    pub fn recv_batch(fd: RawFd, rb: &mut RecvBatch) -> io::Result<usize> {
        let base = rb.buf.as_mut_ptr();
        let mut iovs: Vec<libc::iovec> =
            (0..rb.count).map(|i| libc::iovec { iov_base: unsafe { base.add(i * rb.pkt_size) }.cast(), iov_len: rb.pkt_size }).collect();
        let mut hdrs: Vec<MsghdrX> = iovs.iter_mut().map(|i| hdr(i)).collect();
        let n = unsafe { recvmsg_x(fd, hdrs.as_mut_ptr(), hdrs.len() as u32, 0) };
        if n < 0 {
            return Err(io::Error::last_os_error());
        }
        for i in 0..n as usize {
            rb.lens[i] = hdrs[i].msg_datalen;
        }
        Ok(n as usize)
    }
}

impl UdpSock {
    pub fn set_nonblocking(&self, on: bool) -> io::Result<()> {
        self.sock.set_nonblocking(on)
    }
}
