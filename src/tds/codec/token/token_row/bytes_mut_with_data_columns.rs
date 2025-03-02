use crate::MetaDataColumn;
use bytes::buf::UninitSlice;
use bytes::{BufMut, BytesMut};
use std::borrow::{Borrow, BorrowMut};
use std::ops::{Deref, DerefMut};

pub(crate) struct BytesMutWithDataColumns<'a> {
    bytes: &'a mut BytesMut,
    data_columns: &'a Vec<MetaDataColumn<'a>>,
}

impl<'a> BytesMutWithDataColumns<'a> {
    pub fn new(bytes: &'a mut BytesMut, data_columns: &'a Vec<MetaDataColumn<'a>>) -> Self {
        BytesMutWithDataColumns {
            bytes,
            data_columns,
        }
    }

    pub fn data_columns(&self) -> &'a Vec<MetaDataColumn<'a>> {
        self.data_columns
    }
}

unsafe impl BufMut for BytesMutWithDataColumns<'_> {
    fn remaining_mut(&self) -> usize {
        self.bytes.remaining_mut()
    }

    unsafe fn advance_mut(&mut self, cnt: usize) {
        unsafe { self.bytes.advance_mut(cnt) }
    }

    fn chunk_mut(&mut self) -> &mut UninitSlice {
        self.bytes.chunk_mut()
    }
}

impl Borrow<[u8]> for BytesMutWithDataColumns<'_> {
    fn borrow(&self) -> &[u8] {
        self.bytes.deref()
    }
}

impl BorrowMut<[u8]> for BytesMutWithDataColumns<'_> {
    fn borrow_mut(&mut self) -> &mut [u8] {
        self.bytes.borrow_mut()
    }
}

impl Deref for BytesMutWithDataColumns<'_> {
    type Target = BytesMut;

    fn deref(&self) -> &Self::Target {
        self.bytes
    }
}

impl DerefMut for BytesMutWithDataColumns<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.bytes
    }
}
