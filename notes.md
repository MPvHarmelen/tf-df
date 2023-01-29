make a flamegraph:

```bash
echo -1 | sudo tee /proc/sys/kernel/perf_event_paranoid # once per boot
sudo /home/personal/.cargo/bin/flamegraph -- ./target/release/tf-df -p data/dir
```
