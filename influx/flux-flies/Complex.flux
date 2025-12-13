from(bucket: "ardupilot")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "MAG")
  |> filter(fn: (r) => r.instance == "0")
  |> filter(fn: (r) => r._field == "MagX" or r._field == "MagY" or r._field == "MagZ")
  |> pivot(rowKey: ["_time","instance"], columnKey: ["_field"], valueColumn: "_value")
  |> map(fn: (r) => ({
      r with
      mag: sqrt(float(v: r.MagX * r.MagX + r.MagY * r.MagY + r.MagZ * r.MagZ))
  }))
  |> map(fn: (r) => ({
      r with
      inRange: if r.mag >= 400.0 and r.mag <= 500.0 then 1 else 0
  }))
  |> stateDuration(fn: (r) => r.inRange == 1, column: "dur", unit: 1s)
  |> filter(fn: (r) => r.dur >= 0.5)
  |> keep(columns: ["_time", "mag", "dur", "instance"])