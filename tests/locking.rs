mod with_temp_path;
use with_temp_path::with_temp_path;

use landfill::Landfill;

#[test]
fn lock_contention() -> Result<(), std::io::Error> {
    with_temp_path(|path| {
        let _lf = Landfill::open(path)?;
        assert!(Landfill::open(path).is_err());
        Ok(())
    })
}
