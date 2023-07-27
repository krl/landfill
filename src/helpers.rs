use bytemuck::{Pod, Zeroable};

// Helper function to test if a value is all zeroes,
// used to avoid having to put a PartialEq bind on the
// values used in `Array`
#[inline(always)]
pub(crate) fn is_all_zeroes<T: Zeroable + Pod>(t: &[T]) -> bool {
    let bytes_input: &[u8] = bytemuck::cast_slice(t);
    let zero = &[T::zeroed()];
    let zero_bytes: &[u8] = bytemuck::cast_slice(zero);
    bytes_input == zero_bytes
}
