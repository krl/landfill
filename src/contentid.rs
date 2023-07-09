#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct ContentId([u8; 32]);

impl AsRef<[u8]> for ContentId {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Debug for ContentId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        for byte in self.0 {
            write!(f, "{:02x}", byte)?
        }
        Ok(())
    }
}

impl ContentId {
    pub fn from_slice(slice: &[u8]) -> Self {
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(slice);
        ContentId(bytes)
    }

    pub fn from_hex(hex: &str) -> Self {
        assert_eq!(hex.len(), 64);
        let mut bytes = [0u8; 32];
        for (i, byte) in bytes.iter_mut().enumerate() {
            let slice = &hex[i * 2..i * 2 + 2];
            *byte = u8::from_str_radix(slice, 16).expect("invalid hex string");
        }
        ContentId(bytes)
    }

    pub(crate) fn discriminant(&self) -> u32 {
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(&self.0[..4]);
        u32::from_le_bytes(bytes)
    }
}
