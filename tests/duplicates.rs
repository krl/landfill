use landfill::{AppendOnly, Entropy, Landfill, RandomAccess};
use std::io;

#[test]
fn duplicates_static() -> io::Result<()> {
    let lf = Landfill::ephemeral()?;

    let _a: Entropy = lf.substructure("a")?;
    let a2: io::Result<Entropy> = lf.substructure("a");

    assert!(a2.is_err());

    Ok(())
}

#[test]
fn duplicates_ao() -> io::Result<()> {
    let lf = Landfill::ephemeral()?;

    let _a: AppendOnly = lf.substructure("a")?;
    let a2: io::Result<AppendOnly> = lf.substructure("a");

    assert!(a2.is_err());

    Ok(())
}

#[test]
fn duplicates_rand_access() -> io::Result<()> {
    let lf = Landfill::ephemeral()?;

    let _a: RandomAccess<u8> = lf.substructure("a")?;
    let a2: io::Result<RandomAccess<u8>> = lf.substructure("a");

    assert!(a2.is_err());

    Ok(())
}
