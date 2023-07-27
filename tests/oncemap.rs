use std::io;

use landfill::{Landfill, OnceMap};

const A_LOT: usize = 1024;

#[test]
fn lots() -> Result<(), io::Error> {
    let lf = Landfill::ephemeral()?;
    let map = OnceMap::try_from(&lf)?;

    for i in 0..A_LOT {
        map.insert(i, i + 1)?;
    }

    for i in 0..A_LOT {
        assert_eq!(map.get(&i).unwrap(), &(i + 1))
    }

    Ok(())
}
