use std::io;

use landfill::{Landfill, SmashMap};

#[test]
fn trivial() -> Result<(), io::Error> {
    let lf = Landfill::ephemeral()?;

    let h = SmashMap::try_from(&lf)?;

    let msg: u32 = 1234;

    h.insert(b"omg", |s, _| s.proceed(), |_| Ok(msg))?;

    h.get(b"omg", |s, candidate| {
        if *candidate != msg {
            panic!("oh no");
        }
        s.proceed()
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
        h.insert(&value, |s, _| s.proceed(), |_| Ok(value))?;
    }

    for i in 0..A_LOT {
        let value = (i + 1) as u32;
        let mut found = false;
        h.get(&value, |s, candidate| {
            if *candidate == (i + 1) as u32 {
                found = true;
                s.halt()
            } else {
                s.proceed()
            }
        });
        assert_eq!(found, true);
    }

    let mut found = false;
    let nonexist = A_LOT as u32 + 1;
    h.get(&nonexist, |s, candidate| {
        if *candidate == nonexist {
            found = true;
            s.halt()
        } else {
            s.proceed()
        }
    });

    assert_eq!(found, false);

    Ok(())
}
