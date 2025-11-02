// benches/write_benchmark.rs
use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId, Throughput};
use tdms_rs::*;

fn benchmark_write_f64(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_f64");
    
    for size in [1000, 10000, 100000].iter() {
        group.throughput(Throughput::Bytes((*size * 8) as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let mut writer = TdmsWriter::create("bench_output.tdms").unwrap();
                writer.create_channel("Bench", "Data", DataType::DoubleFloat).unwrap();
                
                let data: Vec<f64> = (0..size).map(|i| i as f64).collect();
                writer.write_channel_data("Bench", "Data", &data).unwrap();
                writer.flush().unwrap();
                
                std::fs::remove_file("bench_output.tdms").ok();
                std::fs::remove_file("bench_output.tdms_index").ok();
            });
        });
    }
    
    group.finish();
}

criterion_group!(benches, benchmark_write_f64);
criterion_main!(benches);