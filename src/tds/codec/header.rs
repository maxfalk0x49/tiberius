use super::{Decode, Encode};
use crate::Error;
use bytes::{Buf, BufMut, BytesMut};

uint_enum! {
    /// the type of the packet [2.2.3.1.1]#[repr(u32)]
    #[repr(u8)]
    pub enum PacketType {
        SQLBatch = 1,
        /// unused
        PreTDSv7Login = 2,
        Rpc = 3,
        TabularResult = 4,
        AttentionSignal = 6,
        BulkLoad = 7,
        /// Federated Authentication Token
        Fat = 8,
        TransactionManagerReq = 14,
        TDSv7Login = 16,
        Sspi = 17,
        PreLogin = 18,
    }
}

/// The message state [2.2.3.1.2].
///
/// These are bitfield constants — the TDS status byte can combine multiple
/// flags (e.g. `EndOfMessage | ResetConnection` = `0x09`).
#[allow(missing_docs)]
pub struct PacketStatus;

impl PacketStatus {
    pub const NORMAL_MESSAGE: u8 = 0x00;
    pub const END_OF_MESSAGE: u8 = 0x01;
    /// [client to server ONLY] (EndOfMessage also required)
    pub const IGNORE_EVENT: u8 = 0x03;
    /// [client to server ONLY] [>= TDSv7.1]
    pub const RESET_CONNECTION: u8 = 0x08;
    /// [client to server ONLY] [>= TDSv7.3]
    pub const RESET_CONNECTION_SKIP_TRAN: u8 = 0x10;
}

/// packet header consisting of 8 bytes [2.2.3.1]
#[derive(Debug, Clone, Copy)]
pub(crate) struct PacketHeader {
    ty: PacketType,
    status: u8,
    /// [BE] the length of the packet (including the 8 header bytes)
    /// must match the negotiated size sending from client to server [since TDSv7.3] after login
    /// (only if not EndOfMessage)
    length: u16,
    /// [BE] the process ID on the server, for debugging purposes only
    spid: u16,
    /// packet id
    id: u8,
    /// currently unused
    window: u8,
}

impl PacketHeader {
    pub fn new(length: usize, id: u8) -> PacketHeader {
        assert!(length <= u16::max_value() as usize);
        PacketHeader {
            ty: PacketType::TDSv7Login,
            status: PacketStatus::NORMAL_MESSAGE,
            length: length as u16,
            spid: 0,
            id,
            window: 0,
        }
    }

    pub fn rpc(id: u8) -> Self {
        Self {
            ty: PacketType::Rpc,
            status: PacketStatus::NORMAL_MESSAGE,
            ..Self::new(0, id)
        }
    }

    pub fn pre_login(id: u8) -> Self {
        Self {
            ty: PacketType::PreLogin,
            status: PacketStatus::END_OF_MESSAGE,
            ..Self::new(0, id)
        }
    }

    pub fn login(id: u8) -> Self {
        Self {
            ty: PacketType::TDSv7Login,
            status: PacketStatus::END_OF_MESSAGE,
            ..Self::new(0, id)
        }
    }

    pub fn batch(id: u8) -> Self {
        Self {
            ty: PacketType::SQLBatch,
            status: PacketStatus::NORMAL_MESSAGE,
            ..Self::new(0, id)
        }
    }

    pub fn bulk_load(id: u8) -> Self {
        Self {
            ty: PacketType::BulkLoad,
            status: PacketStatus::NORMAL_MESSAGE,
            ..Self::new(0, id)
        }
    }

    pub fn set_status(&mut self, status: u8) {
        self.status = status;
    }

    pub fn set_type(&mut self, ty: PacketType) {
        self.ty = ty;
    }

    pub fn status(&self) -> u8 {
        self.status
    }

    /// Sets the reset-connection flag on this header.
    ///
    /// The flag is OR'd into the existing status so it can coexist with
    /// `EndOfMessage` and other bits.
    pub fn set_reset_connection(&mut self) {
        self.status |= PacketStatus::RESET_CONNECTION;
    }

    pub fn r#type(&self) -> PacketType {
        self.ty
    }

    pub fn length(&self) -> u16 {
        self.length
    }
}

impl<B> Encode<B> for PacketHeader
where
    B: BufMut,
{
    fn encode(self, dst: &mut B) -> crate::Result<()> {
        dst.put_u8(self.ty as u8);
        dst.put_u8(self.status);
        dst.put_u16(self.length);
        dst.put_u16(self.spid);
        dst.put_u8(self.id);
        dst.put_u8(self.window);

        Ok(())
    }
}

impl Decode<BytesMut> for PacketHeader {
    fn decode(src: &mut BytesMut) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let raw_ty = src.get_u8();

        let ty = PacketType::try_from(raw_ty).map_err(|_| {
            Error::Protocol(format!("header: invalid packet type: {}", raw_ty).into())
        })?;

        let status = src.get_u8();

        let header = PacketHeader {
            ty,
            status,
            length: src.get_u16(),
            spid: src.get_u16(),
            id: src.get_u8(),
            window: src.get_u8(),
        };

        Ok(header)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn set_reset_connection_ors_flag() {
        let mut header = PacketHeader::batch(0);
        assert_eq!(header.status(), PacketStatus::NORMAL_MESSAGE);

        header.set_status(PacketStatus::END_OF_MESSAGE);
        assert_eq!(header.status(), PacketStatus::END_OF_MESSAGE);

        header.set_reset_connection();
        assert_eq!(
            header.status(),
            PacketStatus::END_OF_MESSAGE | PacketStatus::RESET_CONNECTION
        );
        assert_eq!(header.status(), 0x09);
    }

    #[test]
    fn encode_round_trips_with_reset_flag() {
        let mut header = PacketHeader::rpc(42);
        header.set_status(PacketStatus::END_OF_MESSAGE);
        header.set_reset_connection();

        let mut buf = BytesMut::new();
        header.encode(&mut buf).unwrap();

        assert_eq!(buf[0], PacketType::Rpc as u8);
        assert_eq!(buf[1], 0x09); // END_OF_MESSAGE | RESET_CONNECTION
        assert_eq!(buf[6], 42); // packet id
    }
}
