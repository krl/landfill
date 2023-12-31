use std::io;

use landfill::{Landfill, OnceMap};

const A_LOT: usize = 1024;

#[test]
fn lots() -> io::Result<()> {
    let lf = Landfill::ephemeral()?;
    let map: OnceMap<_, _> = lf.substructure("map")?;

    for i in 0..A_LOT {
        map.insert(i, i + 1)?;
    }

    for i in 0..A_LOT {
        assert_eq!(map.get(&i).unwrap(), &(i + 1))
    }

    Ok(())
}
