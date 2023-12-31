use std::sync::Arc;

use bytemuck::Zeroable;
use bytemuck_derive::*;
use landfill::{Landfill, RandomAccess};
use rand::{seq::SliceRandom, Rng};

mod with_temp_path;
use with_temp_path::with_temp_path;

#[test]
fn random_access_trivial() -> Result<(), std::io::Error> {
    let lf = Landfill::ephemeral()?;
    let da: RandomAccess<_> = lf.substructure("da")?;

    da.with_mut(39, |m| *m = 32)?;

    assert_eq!(*da.get(39).unwrap(), 32);

    Ok(())
}

#[test]
fn random_access_stress() -> Result<(), std::io::Error> {
    const N_THREADS: usize = 16;
    const WRITES_PER_THREAD: usize = 512;

    // We let half of the slots stay empty to easily find empty slots when
    // searching randomly
    const N_SLOTS: usize = N_THREADS * WRITES_PER_THREAD * 2;

    // setup

    #[derive(Copy, Clone, Zeroable, Pod, Debug, PartialEq)]
    #[repr(C)]
    struct Record {
        origin: u32,
        // For padding and destinguising the zero offset,
        // zero length record (0u64, 0u32) from `Zeroable::zeroed()`
        marker: u32,
        value: u64,
    }

    let mut rng = rand::thread_rng();

    let mut writer_datasets = vec![];
    let mut reader_datasets = vec![];

    for t in 0..N_THREADS {
        let mut data = vec![];

        for _ in 0..WRITES_PER_THREAD {
            data.push(Record {
                origin: t as u32,
                value: rng.gen(),
                marker: 0xff,
            });
        }
        let mut reader_data = data.clone();
        let writer_data = data;

        writer_datasets.push(writer_data);

        reader_data.shuffle(&mut rng);
        reader_datasets.push(reader_data)
    }

    // data setup complete

    let mut writer_threads = vec![];
    let mut reader_threads = vec![];

    let lf = Landfill::ephemeral()?;
    let da: Arc<RandomAccess<_>> = Arc::new(lf.substructure("da")?);

    for mut writer_data in writer_datasets.drain(..) {
        let da_write = da.clone();
        writer_threads.push(std::thread::spawn(move || {
            let mut rng = rand::thread_rng();
            for record in writer_data.drain(..) {
                // find a random empty spot to write
                loop {
                    let idx = rng.gen::<usize>() % N_SLOTS;
                    if da_write
                        .with_mut(idx, |slot| {
                            if *slot == Record::zeroed() {
                                *slot = record;
                                true
                            } else {
                                false
                            }
                        })
                        .expect("no errors plz")
                    {
                        break;
                    }
                }
            }
        }))
    }

    for mut reader_data in reader_datasets.drain(..) {
        let da_read = da.clone();
        reader_threads.push(std::thread::spawn(move || {
            let mut rng = rand::thread_rng();
            let mut idx = rng.gen::<usize>() % N_SLOTS;

            // loop until we find our entry!
            while let Some(record) = reader_data.pop() {
                loop {
                    if let Some(written) = da_read.get(idx % N_SLOTS) {
                        if *written == record {
                            break;
                        }
                    }
                    idx += 1;
                }
            }
        }))
    }

    // make sure all threads finish successfully
    for thread in writer_threads {
        thread.join().unwrap()
    }

    for thread in reader_threads {
        thread.join().unwrap()
    }

    Ok(())
}

#[test]
fn random_access_persist_restore() -> Result<(), std::io::Error> {
    with_temp_path(|path| {
        {
            let lf = Landfill::open(path)?;
            let ra: RandomAccess<u32> = lf.substructure("ra")?;

            for i in 1..=1024 {
                ra.with_mut(i, |slot| *slot = i as u32)?;
            }
        }

        // re-open

        let lf = Landfill::open(path)?;
        let ra: RandomAccess<u32> = lf.substructure("ra")?;

        for i in 1..=1024 {
            assert_eq!(*ra.get(i).unwrap(), i as u32)
        }

        Ok(())
    })
}
