# temp_humidity_sensor — ESP32-C3 battery-powered SHTC3 temp/humidity sensor

Raw Arduino C++ replacement for the ESPHome sketch. Wakes from deep sleep on
a timer, reads temperature/humidity + battery voltage, publishes over MQTT
with QoS 1 + PUBACK confirmation and retry, then goes straight back to
sleep. `espMqttClient`, retained HA MQTT discovery, LWT-style diagnostics
(reset reason, connect-fail counters), NVS/RTC-persisted state across sleep,
NTP-synced last-update timestamp, remote and physical-button OTA, manufacturer
`P@cho`.

## Files

- `temp_humidity_sensor.ino` — the sketch.
- `config.h.example` — copy to `config.h` and fill in: WiFi/MQTT/OTA
  credentials, static IP, device identity (`DEVICE_ID`, `DEVICE_NAME`,
  `DEVICE_HW_VERSION`, `FIRMWARE_VERSION`), hardware pins, and this board's
  own battery-voltage calibration table. Keep `config.h` out of git (already
  covered by `.gitignore`).

## Hardware

- ESP32-C3, battery-powered
- **SHTC3** (I2C): SDA → GPIO4, SCL → GPIO5
- Sensor power switch (MOSFET/load switch gate): GPIO3 — SHTC3 is powered
  only while actively being read, to save battery between wakes
- Status LED: GPIO7
- Battery voltage divider midpoint → GPIO1 (ADC)
- OTA trigger button: one leg → GPIO0, other leg → GND (`INPUT_PULLUP`,
  active-low). An external ~10k pull-up from GPIO0 to 3.3V is recommended
  alongside the internal one — deep-sleep GPIO wakeup on the ESP32-C3 needs
  a reliably-held HIGH level while idle, and internal pulls aren't always
  dependable across sleep the way an external resistor is.

## Behavior

- Wakes on a timer (`SLEEP_INTERVAL_US`, default 10 min) **or** by pressing
  the OTA button, which wakes the device early straight into OTA mode — no
  separate reset needed.
- Powers the SHTC3 on, reads temp/humidity, powers it back off.
- Reads battery voltage (averaged over `BATT_ADC_SAMPLES` reads using the
  ESP32's factory ADC calibration), converts to a percentage via a
  piecewise-linear calibration curve (`BATT_CAL` in `config.h`).
- Publishes everything over MQTT (QoS 1, with PUBACK wait + up to
  `MQTT_PUBLISH_RETRIES` retries per topic), then goes back to sleep.
- Holding the OTA button — or setting the retained `ota_request` MQTT
  switch from Home Assistant — keeps the device awake and starts
  `ArduinoOTA` for up to `OTA_WINDOW_MS`.
- A hardware timer (`AWAKE_WATCHDOG_TIMEOUT_MS`) force-restarts the device
  if it's ever awake too long, independent of the rest of the sketch, so a
  hang can't drain the battery overnight.
- NTP resync happens only every `NTP_RESYNC_EVERY_N_BOOTS` wakes (or if
  never synced yet) and always *after* sensor data has already published
  successfully, so a slow/failed NTP round trip only costs the
  `last_update` field, never the actual reading.

## Before building

1. **Arduino IDE board package**: "esp32 by Espressif Systems", core 3.x+.
2. **Libraries** (Library Manager):
   - `espMqttClient` by bertmelis
   - `Adafruit SHTC3` (+ Adafruit BusIO, Adafruit Unified Sensor)
   - `ArduinoOTA` (bundled with the ESP32 core)
3. Select board **"ESP32C3 Dev Module"**.
4. Copy `config.h.example` to `config.h` and fill in your WiFi/MQTT/OTA
   values, static IP, `DEVICE_ID`/`DEVICE_NAME`, and — importantly —
   re-measure and replace the `BATT_CAL` calibration points against your
   own board's actual battery (multimeter reading vs. the raw ADC value the
   firmware logs over serial).

## First flash vs. later updates

First flash needs a USB cable. After that, hold the OTA button (or trigger
the `ota_request` switch from Home Assistant) to bring the device up in OTA
mode for `OTA_WINDOW_MS` (default 10 min) — it shows up in `Tools > Port` as
a network port, protected by `OTA_PASSWORD` from `config.h`.

## MQTT / Home Assistant

Base topic: `home/<DEVICE_ID>/...`

| Purpose | Topic |
|---|---|
| Temperature (°C) | `home/<id>/temperature` |
| Humidity (%) | `home/<id>/humidity` |
| Battery voltage | `home/<id>/battery_voltage` |
| Battery percent | `home/<id>/battery_percent` |
| Low-battery flag | `home/<id>/battery_low` |
| Last full charge date | `home/<id>/last_full_charge` |
| WiFi signal (dBm) | `home/<id>/wifi_signal` |
| Reset reason | `home/<id>/reset_reason` |
| Connect fail count (resets on success) | `home/<id>/connect_fail_count` |
| Total fail count (lifetime) | `home/<id>/total_fail_count` |
| Boot count | `home/<id>/boot_count` |
| Firmware version | `home/<id>/firmware_version` |
| Last update (UTC timestamp) | `home/<id>/last_update` |
| OTA request (retained, HA switch) | `home/<id>/ota_request` |

All state topics get retained, QoS-1, HA MQTT discovery configs on first
successful boot (`homeassistant/sensor/<id>/.../config`, etc.), each with an
`expire_after` of 3× the sleep interval so Home Assistant marks the entity
unavailable if a device stops reporting rather than showing stale data
forever.

## Status LED

Driven via LEDC PWM (not a plain digital on/off) so brightness is
adjustable — see `LED_BRIGHTNESS_PCT` in `config.h` (0–100, default 100).

| Pattern | Meaning |
|---|---|
| 1 short blink | Cycle completed and published successfully |
| 3 quick blinks | Cycle failed (WiFi, MQTT, or an unacked publish) |
| Solid | OTA mode active |

## Config file

Credentials, device identity, hardware pins, LED brightness, and this
board's battery calibration all live in one `config.h` — copy
`config.h.example` to `config.h` and fill in real values. Battery
calibration (`BATT_CAL`, `BATT_DIVIDER_RATIO`) is genuinely per-board —
the example values are just a starting point, not something to trust
as-is.

## DEBUG_MODE

Set `DEBUG_MODE` to `true` in `config.h` to disable deep sleep after a
cycle completes (stays connected, drops into `loop()`) — useful for serial
debugging without waiting through the sleep interval on every iteration.

## Version History

`FIRMWARE_VERSION` lives in the gitignored `config.h`. Versions below
v3.6.1 predate this changelog (pre-existing baseline); from v3.6.1 onward
each entry is confirmed against the actual commit/request history.

| Version | Date | Changes |
|---|---|---|
| v3.6.0 | — | Baseline before this changelog was introduced. |
| v3.6.1 | 2026-09-06 | LED feedback changed to 1 blink success / 3 blinks failure (was 2/10); status LED switched to LEDC PWM with `LED_BRIGHTNESS_PCT` in `config.h` for adjustable brightness. |
| v3.6.2 | 2026-09-07 | Battery percentage curve's 100% point lowered from 4.20V to 4.15V — a resting, unplugged cell settles below 4.20V well before it's actually due for a recharge. |
| v3.6.3 | 2026-09-07 | Battery curve properly rescaled (every breakpoint proportionally compressed) instead of just flattening the top into an instant jump at 4.15V. |
| v3.6.4 | 2026-09-12 | Last-full-charge date diagnostic (flash/NVS-backed, survives an actual battery depletion). |
| v3.6.5 | 2026-09-20 | Fixed the last-full-charge diagnostic re-triggering spuriously: a battery reading hovering right at the top of its curve (ADC noise) could bounce 99%→100%→99%→100% and record a "new" full charge on every single upward bounce. Replaced the plain `wasAt100` rising-edge flag in `updateAndGetLastFullChargeDate()` with an "armed" flag that only re-arms once the battery actually reads at or below the new `FULL_CHARGE_REARM_THRESHOLD_PCT` (default 97%, `config.h`). Applied fleet-wide to every project sharing this diagnostic (`door_sensor`, `DS_1_v3`, `TH_2_v4`, `TH_2_v4_L`, `temp_humidity_sensor_zdravkovec`). |
