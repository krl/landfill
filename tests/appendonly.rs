use landfill::{AppendOnly, Landfill};

mod with_temp_path;
use with_temp_path::with_temp_path;

#[test]
fn appendonly_trivial() -> Result<(), std::io::Error> {
    let lf = Landfill::ephemeral()?;
    let ao = AppendOnly::try_from(&lf)?;

    let msg_a = b"hello word";
    let msg_b = b"hello world!";

    let rec_a = ao.write(msg_a)?;

    let slice_a = ao.get(rec_a);

    let rec_b = ao.write(msg_b)?;

    let slice_b = ao.get(rec_b);

    assert_eq!(slice_a, msg_a);
    assert_eq!(slice_b, msg_b);

    Ok(())
}

#[test]
fn appendonly_save_restore() -> Result<(), std::io::Error> {
    with_temp_path(|path| {
        let rec_a;
        let rec_b;

        let msg_a = b"hello word";
        let msg_b = b"hello world!";

        {
            let lf = Landfill::open(path)?;
            let ao = AppendOnly::try_from(&lf)?;

            rec_a = ao.write(msg_a)?;

            let slice_a = ao.get(rec_a);

            rec_b = ao.write(msg_b)?;

            let slice_b = ao.get(rec_b);

            assert_eq!(slice_a, msg_a);
            assert_eq!(slice_b, msg_b);
        }

        let lf = Landfill::open(path)?;

        // re-open

        let ao = AppendOnly::try_from(&lf)?;
        assert_eq!(ao.get(rec_a), msg_a);
        assert_eq!(ao.get(rec_b), msg_b);

        Ok(())
    })
}

#[test]
fn appendonly_save_restore_skip_files() -> Result<(), std::io::Error> {
    with_temp_path(|path| {
        let rec_a;
        let rec_b;

        let msg_a = b"hello word";
        let msg_b = b"hello world!";

        {
            let lf = Landfill::open(path)?;
            let ao = AppendOnly::try_from(&lf)?;

            rec_a = ao.write(msg_a)?;

            let slice_a = ao.get(rec_a);

            rec_b = ao.write(msg_b)?;

            let slice_b = ao.get(rec_b);

            assert_eq!(slice_a, msg_a);
            assert_eq!(slice_b, msg_b);
        }

        // re-open

        let lf = Landfill::open(path)?;
        let ao = AppendOnly::try_from(&lf)?;

        assert_eq!(ao.get(rec_a), msg_a);
        assert_eq!(ao.get(rec_b), msg_b);

        Ok(())
    })
}
