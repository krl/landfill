use std::io;

use landfill::{Landfill, Search, SmashMap};

#[test]
fn trivial() -> Result<(), io::Error> {
    let lf = Landfill::ephemeral()?;

    let h = SmashMap::try_from(&lf)?;

    let msg: u32 = 1234;

    h.insert(b"omg", |_| Search::Continue, || msg)?;

    h.get(b"omg", |candidate| {
        if *candidate != msg {
            panic!("oh no");
        }
        Search::Continue
    });

    Ok(())
}

const A_LOT: usize = 1024 * 128;

#[test]
fn a_lot() -> Result<(), io::Error> {
    let lf = Landfill::ephemeral()?;
    let h = SmashMap::try_from(&lf)?;

    for i in 0..A_LOT {
        let value = (i + 1) as u32;
        h.insert(&value, |_| Search::Continue, || value)?;
    }

    for i in 0..A_LOT {
        let value = (i + 1) as u32;
        let mut found = false;
        h.get(&value, |candidate| {
            if *candidate == (i + 1) as u32 {
                found = true;
                Search::Halt
            } else {
                Search::Continue
            }
        });
        assert_eq!(found, true);
    }

    let mut found = false;
    let nonexist = A_LOT as u32 + 1;
    h.get(&nonexist, |candidate| {
        if *candidate == nonexist {
            found = true;
            Search::Halt
        } else {
            Search::Continue
        }
    });

    assert_eq!(found, false);

    Ok(())
}
