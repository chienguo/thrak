use std::mem;

use crate::transaction::TxId;

pub type PageId = u64;

#[repr(C, packed)]
pub struct Page {
    page_id: PageId,
    flag: u16,
    count: u16,
    overflow: u16,
    body_ptr: u128,
}

#[repr(C, packed)]
pub struct BranchPageElement {
    pos: usize,
    key_size: usize,
    page_id: PageId,
}

#[repr(C, packed)]
pub struct LeafPageElement {
    flag: u32,
    pos: usize,
    key_size: usize,
    value_size: usize,
    page_id: PageId,
}

#[repr(C, packed)]
pub struct Meta {
    magic: u32,
    version: u32,
    page_size: u32,
    flags: u32,
    //root: &'a Bucket,
    freelist: PageId,
    page_id: PageId,
    tx_id: TxId,
    checksum: u64,
}

const PAGE_HEADER_SIZE: usize = memoffset::offset_of!(Page, body_ptr);

const MIN_KEYS_PER_PAGE: u8 = 2;

const BRANCH_PAGE_ELEMENT_SIZE: usize = mem::size_of::<BranchPageElement>();

const LEAF_PAGE_ELEMENT_SIZE: usize = mem::size_of::<LeafPageElement>();

const BRANCH_PAGE_FLAG: u8 = 0x01; // 0000_0001
const LEAF_PAGE_FLAG: u8 = 0x02; // 0000_0010
const META_PAGE_FLAG: u8 = 0x04; // 0000_0100
const FREELIST_PAGE_FLAG: u8 = 0x10; // 0001_0000

const BUCKET_LEAF_FLAG: u8 = 0x01;

impl Page {
    unsafe fn meta(&self) -> &Meta {
        mem::transmute::<u64, &Meta>(self.body_ptr as u64)
    }

    unsafe fn leaf_page_element(&self, idx: usize) -> &LeafPageElement {
        &mem::transmute::<u128, &[LeafPageElement]>(self.body_ptr)[idx]
    }

    unsafe fn leaf_page_elements(&self) -> Option<&[LeafPageElement]> {
        if self.count == 0 {
            return None;
        }
        Some(mem::transmute::<u128, &[LeafPageElement]>(self.body_ptr))
    }

    unsafe fn branch_page_element(&self, idx: usize) -> &BranchPageElement {
        &mem::transmute::<u128, &[BranchPageElement]>(self.body_ptr)[idx]
    }

    unsafe fn branch_page_elements(&self) -> Option<&[BranchPageElement]> {
        if self.count == 0 {
            return None;
        }
        Some(mem::transmute::<u128, &[BranchPageElement]>(self.body_ptr))
    }
}

impl LeafPageElement {
    unsafe fn key(&self) -> &[u8] {
        let ptr = self as *const LeafPageElement as u128;
        let buf = mem::transmute::<u128, &[u8]>(ptr);
        &buf[self.pos..(self.pos + self.key_size)]
    }

    unsafe fn value(&self) -> &[u8] {
        let ptr = self as *const LeafPageElement as u128;
        let buf = mem::transmute::<u128, &[u8]>(ptr);
        &buf[self.pos..(self.pos + self.value_size)]
    }
}

fn merge(a: &Vec<PageId>, b: &Vec<PageId>) -> Vec<PageId> {
    if a.is_empty() {
        return b.to_owned();
    }
    if b.is_empty() {
        return a.to_owned();
    }
    let mut merged = Vec::with_capacity(a.len() + b.len());
    for i in 0..(a.len() + b.len()) {
        merged.insert(i, 0);
    }
    merge_page_ids(&mut merged, a, b);
    merged
}

fn merge_page_ids(dst: &mut [PageId], a: &Vec<PageId>, b: &Vec<PageId>) {
    if a.is_empty() {
        dst[..b.len()].copy_from_slice(&b[..]);
        return;
    }
    if b.is_empty() {
        dst[..a.len()].copy_from_slice(&a[..]);
        return;
    }

    let mut i = 0;
    let mut j = 0;
    let mut counter = 0;
    while i < a.len() && j < b.len() {
        if a[i] < b[j] {
            dst[counter] = a[i];
            i += 1;
        } else {
            dst[counter] = b[j];
            j += 1;
        }
        counter += 1;
    }
    if i == a.len() {
        dst[counter..].copy_from_slice(&b[j..]);
    } else {
        dst[counter..].copy_from_slice(&a[i..])
    }
}

#[cfg(test)]
mod tests {
    use crate::page::{merge, PageId};

    #[test]
    fn test_merge_page_ids() {
        let a: Vec<PageId> = vec![4, 5, 6, 10, 11, 12, 13, 27];
        let b: Vec<PageId> = vec![1, 3, 8, 9, 25, 30];
        let c = merge(&a, &b);
        assert_eq!(c, vec![1, 3, 4, 5, 6, 8, 9, 10, 11, 12, 13, 25, 27, 30]);

        let a = vec![4, 5, 6, 10, 11, 12, 13, 27, 35, 36];
        let b = vec![8, 9, 25, 30];
        let c = merge(&a, &b);
        assert_eq!(c, vec![4, 5, 6, 8, 9, 10, 11, 12, 13, 25, 27, 30, 35, 36]);
    }
}
