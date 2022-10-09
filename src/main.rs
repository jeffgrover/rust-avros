use std::fs::File;
use avro_rs::Reader;
use avro_rs::types::Value;
use similar::TextDiff;
use std::io;
use std::time::{Duration, Instant};


const FILE1:&str = "staging_101-metrics-FilterType.SITE_COHORT-3168443062086018990.avro";
const FILE2:&str = "staging_101-metrics-FilterType.SITE_COHORT--7221345451198866622.avro";


fn avro_str(filename:&str) -> Result<Vec<Value>, String> {
        let input:File = File::open(filename).unwrap();
    let reader:Reader<File> = Reader::new(input).unwrap();
    let mut _count = 0;
    let mut rows:Vec<Value> = Vec::new();

    for value in reader {
        _count += 1;
        let rec = value.unwrap();
        // println!("{:?}", rec);
        rows.push(rec);
    }
    // println!("{:#?}", rows);  //  Debugging help
    // println!("{} total rows", _count);
    Ok(rows)
}



fn main() {
    let start = Instant::now();

    let old:Vec<Value> = avro_str(FILE1).unwrap();
    let new:Vec<Value> = avro_str(FILE2).unwrap();

    TextDiff::from_lines(&format!("{:#?}", old), &format!("{:#?}", new))
    .unified_diff()
    .header(
        &FILE1,
        &FILE2,
    ).to_writer(io::stdout()).unwrap();

    let duration:Duration = start.elapsed();

    println!("Time elapsed comparing avro files: {:?}", duration);

}
