option v = {
  bucket: "ardupilot",
  flightFilterEnabled: false,
}

// Generic simple check
simpleCheck = (message_type, field, min_value, max_value, duration_seconds, alert_name_low, alert_name_high, instance) =>
  from(bucket: v.bucket)
    |> range(start: -1h)
    |> filter(fn: (r) => r._measurement == message_type)
    |> filter(fn: (r) => r._field == field)
    |> (if exists instance then filter(fn: (r) => r.instance == string(v: instance)) else (r) => r)
    // TODO: add additional filter for "in flight" if required, e.g.:
    // |> filter(fn: (r) => r.in_flight == "true")
    |> map(fn: (r) => ({
        r with
        low_violation: if exists min_value and float(v: r._value) < float(v: min_value) then 1 else 0,
        high_violation: if exists max_value and float(v: r._value) > float(v: max_value) then 1 else 0,
    }))
    |> map(fn: (r) => ({
        r with
        state: if r.low_violation == 1 or r.high_violation == 1 then 1 else 0
    }))
    |> stateDuration(fn: (r) => r.state == 1, column: "dur", unit: 1s)
    |> filter(fn: (r) => r.dur >= duration_seconds)
    |> map(fn: (r) => ({
        r with
        alert_name: if r.low_violation == 1 then alert_name_low
                    else if r.high_violation == 1 then alert_name_high
                    else ""
    }))
    |> keep(columns: ["_time", "_measurement", "_field", "_value", "dur", "alert_name", "instance"])



bat0_voltage = simpleCheck(
  message_type: "BAT",
  field: "Volt",
  min_value: 39.5,
  max_value: 50.4,
  duration_seconds: 2.0,
  alert_name_low: "Battery 0 Voltage Low",
  alert_name_high: "Battery 0 Voltage High",
  instance: 0
)
rcou_c1 = simpleCheck(
  message_type: "RCOU",
  field: "C1",
  min_value: 1150.0,
  max_value: 1870.0,
  duration_seconds: 5.0,
  alert_name_low: "Motor 1 Command Low",
  alert_name_high: "Motor 1 Command High",
  instance: null
)

rcou_c2 = simpleCheck(
  message_type: "RCOU",
  field: "C2",
  min_value: 1150.0,
  max_value: 1870.0,
  duration_seconds: 5.0,
  alert_name_low: "Motor 2 Command Low",
  alert_name_high: "Motor 2 Command High",
  instance: null
)

// ... repeat for C3–C14, changing field and alert names
motors = union(tables: [rcou_c1, rcou_c2, rcou_c3, rcou_c4, rcou_c5, rcou_c6, rcou_c7, rcou_c8, rcou_c9, rcou_c10, rcou_c11, rcou_c12, rcou_c13, rcou_c14])

vibe_x = simpleCheck(
  message_type: "VIBE",
  field: "VibeX",
  min_value: null,
  max_value: 25.0,
  duration_seconds: 2.0,
  alert_name_low: "",
  alert_name_high: "IMU1 High X-Vibration",
  instance: 0
)

vibe_y = simpleCheck(
  message_type: "VIBE",
  field: "VibeY",
  min_value: null,
  max_value: 25.0,
  duration_seconds: 2.0,
  alert_name_low: "",
  alert_name_high: "IMU1 High Y-Vibration",
  instance: 0
)

vibe_z = simpleCheck(
  message_type: "VIBE",
  field: "VibeZ",
  min_value: null,
  max_value: 30.0,
  duration_seconds: 2.0,
  alert_name_low: "",
  alert_name_high: "IMU1 High Z-Vibration",
  instance: 0
)


all_alerts =
  union(tables: [
    bat0_voltage,
    motors,
    vibe_x,
    vibe_y,
    vibe_z,
  ])
  |> sort(columns: ["_time"])
