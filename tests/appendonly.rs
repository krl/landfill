use landfill::{AppendOnly, Landfill};

mod with_temp_path;
use with_temp_path::with_temp_path;

#[test]
fn appendonly_trivial() -> Result<(), std::io::Error> {
    let lf = Landfill::ephemeral()?;
    let ao: AppendOnly = lf.substructure("ao")?;

    let msg_a = b"hello word";
    let msg_b = b"hello world!";

    let ofs_a = ao.write(msg_a)?;

    let slice_a = ao.get(ofs_a, msg_a.len() as u32);

    let ofs_b = ao.write(msg_b)?;

    let slice_b = ao.get(ofs_b, msg_b.len() as u32);

    assert_eq!(slice_a, msg_a);
    assert_eq!(slice_b, msg_b);

    Ok(())
}

#[test]
fn appendonly_save_restore() -> Result<(), std::io::Error> {
    with_temp_path(|path| {
        let ofs_a;
        let ofs_b;

        let msg_a = b"hello word";
        let msg_b = b"hello world!";

        {
            let lf = Landfill::open(path)?;
            let ao: AppendOnly = lf.substructure("ao")?;

            ofs_a = ao.write(msg_a)?;

            let slice_a = ao.get(ofs_a, msg_a.len() as u32);

            ofs_b = ao.write(msg_b)?;

            let slice_b = ao.get(ofs_b, msg_b.len() as u32);

            assert_eq!(slice_a, msg_a);
            assert_eq!(slice_b, msg_b);
        }

        let lf = Landfill::open(path)?;

        // re-open

        let ao: AppendOnly = lf.substructure("ao")?;
        assert_eq!(ao.get(ofs_a, msg_a.len() as u32), msg_a);
        assert_eq!(ao.get(ofs_b, msg_b.len() as u32), msg_b);

        Ok(())
    })
}

#[test]
fn appendonly_save_restore_skip_files() -> Result<(), std::io::Error> {
    with_temp_path(|path| {
        let ofs_a;
        let ofs_b;

        let msg_a = b"hello word";
        let msg_b = b"hello world!";

        {
            let lf = Landfill::open(path)?;
            let ao: AppendOnly = lf.substructure("ao")?;

            ofs_a = ao.write(msg_a)?;

            let slice_a = ao.get(ofs_a, msg_a.len() as u32);

            ofs_b = ao.write(msg_b)?;

            let slice_b = ao.get(ofs_b, msg_b.len() as u32);

            assert_eq!(slice_a, msg_a);
            assert_eq!(slice_b, msg_b);
        }

        // re-open

        let lf = Landfill::open(path)?;
        let ao: AppendOnly = lf.substructure("ao")?;

        assert_eq!(ao.get(ofs_a, msg_a.len() as u32), msg_a);
        assert_eq!(ao.get(ofs_b, msg_b.len() as u32), msg_b);

        Ok(())
    })
}
