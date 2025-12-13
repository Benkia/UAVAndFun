from(bucket: "sample-bucket")
  |> range(start: 2025-12-01T00:00:00Z, stop: 2025-12-12T00:00:00Z)
  |> filter(fn: (r) => r["_measurement"] == "vibe")
  |> filter(fn: (r) => r["_field"] == "VibeX")
  |> filter(fn: (r) => r["imu"] == "0")
  |> map(fn: (r) => ({
      r with
      above: if r._value > 25 then 1 else 0
  }))
  |> stateDuration(fn: (r) => r.above == 1, column: "dur", unit: 1s)
  |> filter(fn: (r) => r.dur >= 2.0)
  |> keep(columns: ["_time", "_value", "dur", "IMU"])
