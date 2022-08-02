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
    if a.len() == 0 {
        return b.to_owned();
    }
    if b.len() == 0 {
        return a.to_owned();
    }
    let mut merged = Vec::with_capacity(a.len() + b.len());
    merge_page_ids(&mut merged, a, b);
    merged
}

fn merge_page_ids(dst: &mut Vec<PageId>, a: &Vec<PageId>, b: &Vec<PageId>) {
    if a.len() == 0 {
        for i in 0..b.len() {
            dst[i] = b[i];
        }
        return;
    }
    if b.len() == 0 {
        for i in 0..a.len() {
            dst[i] = a[i];
        }
        return;
    }

    let mut i = 0;
    let mut j = 0;
    let mut counter = 0;
    while i <= a.len() && j <= b.len() {
        if a[i] < b[j] {
            dst[counter] = a[i];
            i += 1;
        } else {
            dst[counter] = b[j];
            j += 1;
        }
        counter += 1;
    }
    if i <= a.len() {
        for k in i..a.len() {
            dst[counter] = a[k];
            counter += 1;
        }
    } else {
        for k in j..b.len() {
            dst[counter] = b[k];
            counter += 1;
        }
    }
}
