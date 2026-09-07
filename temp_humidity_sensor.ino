/*
 * ESP32-C3 Battery-Powered Temp/Humidity Sensor (SHTC3)
 * SHTC3 (I2C) + battery voltage -> MQTT -> Home Assistant (via MQTT discovery)
 *
 * Wiring:
 *   SHTC3: SDA -> GPIO4, SCL -> GPIO5
 *   Sensor power switch (MOSFET/load switch gate): GPIO3
 *   Status LED: GPIO7
 *   Battery voltage divider: midpoint -> GPIO1 (ADC)
 *   OTA trigger button: one leg -> GPIO0, other leg -> GND (INPUT_PULLUP, active LOW)
 *     An external ~10k pull-up from GPIO0 to 3.3V is recommended alongside
 *     the internal one -- deep-sleep GPIO wakeup on the ESP32-C3 needs a
 *     reliably-held HIGH level while idle, and internal pulls aren't always
 *     dependable across sleep the way an external resistor is.
 *
 * Behavior:
 *   - Wakes on a timer (periodic report) OR by pressing the OTA button,
 *     which wakes the device early straight into OTA mode -- no separate
 *     reset needed
 *   - Powers the SHTC3 on, reads temp/humidity, powers it back off
 *   - Reads battery voltage, converts to % via the same lookup curve as before
 *   - Publishes everything over MQTT, then goes back to sleep
 *   - Holding the OTA button keeps the device awake (deep sleep prevented)
 *     and starts ArduinoOTA for up to OTA_WINDOW_MS, mirroring the
 *     check_ota / ota_timeout_monitor scripts from the ESPHome config
 *
 * Libraries required (Library Manager):
 *   - espMqttClient (bertmelis) — QoS 1 publish with broker PUBACK confirmation
 *   - Adafruit SHTC3 (+ Adafruit BusIO, Adafruit Unified Sensor)
 *   - ArduinoOTA (bundled with ESP32 core)
 */

#include <WiFi.h>
#include <espMqttClient.h>
#include <Wire.h>
#include <Adafruit_SHTC3.h>
#include <ArduinoOTA.h>
#include "esp_sleep.h"
#include "esp_system.h"
#include "esp_timer.h"
#include "esp32-hal-bt.h"
#include <time.h>
#include <sys/time.h>
#include "config.h"

// MQTT topics
String TOPIC_TEMP         = String("home/") + DEVICE_ID + "/temperature";
String TOPIC_HUMIDITY     = String("home/") + DEVICE_ID + "/humidity";
String TOPIC_BATTERY_V    = String("home/") + DEVICE_ID + "/battery_voltage";
String TOPIC_BATTERY_PCT  = String("home/") + DEVICE_ID + "/battery_percent";
String TOPIC_RSSI         = String("home/") + DEVICE_ID + "/wifi_signal";
String TOPIC_LAST_UPDATE  = String("home/") + DEVICE_ID + "/last_update";
String TOPIC_RESET_REASON = String("home/") + DEVICE_ID + "/reset_reason";
String TOPIC_FAIL_COUNT   = String("home/") + DEVICE_ID + "/connect_fail_count";
String TOPIC_TOTAL_FAIL_COUNT = String("home/") + DEVICE_ID + "/total_fail_count";
String TOPIC_OTA_REQUEST = String("home/") + DEVICE_ID + "/ota_request"; // retained flag, set from HA to request OTA remotely
String TOPIC_BATTERY_LOW  = String("home/") + DEVICE_ID + "/battery_low";
String TOPIC_BOOT_COUNT   = String("home/") + DEVICE_ID + "/boot_count";
String TOPIC_FW_VERSION = String("home/") + DEVICE_ID + "/firmware_version";

// Home Assistant MQTT discovery topics
String DISCOVERY_TEMP        = String("homeassistant/sensor/") + DEVICE_ID + "/temperature/config";
String DISCOVERY_HUMIDITY    = String("homeassistant/sensor/") + DEVICE_ID + "/humidity/config";
String DISCOVERY_BATTERY_V   = String("homeassistant/sensor/") + DEVICE_ID + "/battery_voltage/config";
String DISCOVERY_BATTERY_PCT = String("homeassistant/sensor/") + DEVICE_ID + "/battery_percent/config";
String DISCOVERY_RSSI        = String("homeassistant/sensor/") + DEVICE_ID + "/wifi_signal/config";
String DISCOVERY_LAST_UPDATE = String("homeassistant/sensor/") + DEVICE_ID + "/last_update/config";
String DISCOVERY_RESET_REASON = String("homeassistant/sensor/") + DEVICE_ID + "/reset_reason/config";
String DISCOVERY_FAIL_COUNT   = String("homeassistant/sensor/") + DEVICE_ID + "/connect_fail_count/config";
String DISCOVERY_TOTAL_FAIL_COUNT = String("homeassistant/sensor/") + DEVICE_ID + "/total_fail_count/config";
String DISCOVERY_OTA_REQUEST = String("homeassistant/switch/") + DEVICE_ID + "/ota_request/config";
String DISCOVERY_BATTERY_LOW  = String("homeassistant/binary_sensor/") + DEVICE_ID + "/battery_low/config";
String DISCOVERY_BOOT_COUNT   = String("homeassistant/sensor/") + DEVICE_ID + "/boot_count/config";
String DISCOVERY_FW_VERSION = String("homeassistant/sensor/") + DEVICE_ID + "/firmware_version/config";

// ---------------- Persisted state (survives deep sleep) ----------------

RTC_DATA_ATTR bool discoverySent = false;
RTC_DATA_ATTR uint32_t bootCount = 0;
RTC_DATA_ATTR uint32_t connectFailCount = 0; // increments on any wake that fails to publish, resets on success
RTC_DATA_ATTR uint32_t totalFailCount = 0; // lifetime total failed wakes -- never resets, mirrors bootCount
RTC_DATA_ATTR uint8_t cachedWifiChannel = 0; // 0 = unknown yet, let WiFi.begin() auto-select
RTC_DATA_ATTR bool g_timeSynced = false; // true once any cycle has completed a real NTP sync

// ---------------- Globals ----------------

espMqttClient mqttClient;
Adafruit_SHTC3 shtc3 = Adafruit_SHTC3();

// Tracks the PUBACK for whichever single publish is currently in flight.
// publishWithAck() only ever has one outstanding packet at a time, so a
// single tracked ID (rather than a list) is enough here.
volatile uint16_t g_trackedPacketId = 0;
volatile bool g_packetAcked = false;

void onMqttPublish(uint16_t packetId) {
  if (packetId == g_trackedPacketId) {
    g_packetAcked = true;
  }
}

// Remote OTA trigger: set from Home Assistant (a switch entity) rather than
// only via the physical button. Since the device is subscribed for only a
// moment each cycle, this relies on the message being retained -- the
// broker delivers it immediately on subscribe, no polling needed.
volatile bool g_otaRequestReceived = false;
char g_otaRequestPayload[8] = {0};

void onMqttMessage(const espMqttClientTypes::MessageProperties& properties, const char* topic,
                    const uint8_t* payload, size_t len, size_t index, size_t total) {
  if (TOPIC_OTA_REQUEST.equals(topic)) {
    size_t copyLen = len < sizeof(g_otaRequestPayload) - 1 ? len : sizeof(g_otaRequestPayload) - 1;
    memcpy(g_otaRequestPayload, payload, copyLen);
    g_otaRequestPayload[copyLen] = '\0';
    g_otaRequestReceived = true;
  }
}

// ---------------- Awake watchdog ----------------
// A hardware timer that force-restarts the device if it's ever awake too
// long -- a hang, an unexpected infinite loop, a stuck library call. This
// is independent of everything else in the sketch: even if the main flow
// gets stuck somewhere no one anticipated, this guarantees the device
// eventually resets rather than draining the battery all night awake.

esp_timer_handle_t g_watchdogTimer = nullptr;

void watchdogTimeoutHandler(void* arg) {
  esp_restart();
}

void startAwakeWatchdog(uint32_t timeoutMs) {
  if (g_watchdogTimer != nullptr) {
    esp_timer_stop(g_watchdogTimer);
    esp_timer_delete(g_watchdogTimer);
    g_watchdogTimer = nullptr;
  }
  esp_timer_create_args_t args = {};
  args.callback = &watchdogTimeoutHandler;
  args.arg = nullptr;
  args.dispatch_method = ESP_TIMER_TASK;
  args.name = "awake_wdt";
  esp_timer_create(&args, &g_watchdogTimer);
  esp_timer_start_once(g_watchdogTimer, (uint64_t)timeoutMs * 1000ULL);
}

void stopAwakeWatchdog() {
  if (g_watchdogTimer != nullptr) {
    esp_timer_stop(g_watchdogTimer);
    esp_timer_delete(g_watchdogTimer);
    g_watchdogTimer = nullptr;
  }
}

// ---------------- Function declarations ----------------

void connectWiFi();
bool attemptWifiConnect(uint8_t channel, bool useBSSID);
bool connectMQTT();
bool publishWithAck(const char* topic, const char* payload, bool retained);
void sendDiscoveryConfig();
int publishState(float tempC, float humidity, float battV, float battPct, int rssi, const String& resetReasonStr, uint32_t failCount);
void goToSleep();
float readBatteryVoltage();
float batteryPercentage(float v);
void blink(int times, uint32_t onMs, uint32_t gapMs);
void runOtaWindow();
void enterOtaMode();
bool syncTimeUtc();
String getCurrentTimestampUtc();
String resetReasonToString(esp_reset_reason_t reason);

// ---------------- Setup / main flow ----------------

void setup() {
  Serial.begin(115200);
  startAwakeWatchdog(AWAKE_WATCHDOG_TIMEOUT_MS); // armed immediately -- extended later if OTA mode is entered

  btStop(); // BT controller isn't used here; costs nothing to make sure it's off

  if (DEBUG_MODE) {
    delay(DEBUG_BOOT_DELAY_MS);
    Serial.println("=== DEBUG_MODE is ON: deep sleep disabled, staying awake ===");
  } else {
    delay(100);
  }

  bootCount++;

  esp_reset_reason_t resetReason = esp_reset_reason();
  String resetReasonStr = resetReasonToString(resetReason);
  if (resetReason == ESP_RST_BROWNOUT) {
    connectFailCount++; // a brownout mid-cycle means this wake never got to publish either
    totalFailCount++;
  }

  pinMode(SENSOR_POWER_PIN, OUTPUT);
  digitalWrite(SENSOR_POWER_PIN, LOW); // ALWAYS_OFF until we explicitly power it
  ledcAttach(LED_PIN, LED_PWM_FREQ_HZ, LED_PWM_RESOLUTION);
  ledcWrite(LED_PIN, 0);
  pinMode(OTA_PIN, INPUT_PULLUP);
  analogSetPinAttenuation((uint8_t)BATT_PIN, ADC_11db);

  // Check this as early as possible, before any slow work (sensor reads,
  // battery averaging). A wake caused by the button itself, or the button
  // still being physically held, both count -- either way we want to skip
  // straight to WiFi + OTA rather than sitting through a full normal report
  // cycle first.
  esp_sleep_wakeup_cause_t wakeupCause = esp_sleep_get_wakeup_cause();
  bool otaRequested = (digitalRead(OTA_PIN) == LOW) || (wakeupCause == ESP_SLEEP_WAKEUP_GPIO);
  Serial.printf("Boot #%lu, wakeup cause: %d, OTA button: %s\n",
                bootCount, (int)wakeupCause, otaRequested ? "PRESSED" : "released");

  if (otaRequested) {
    // Fast path: no sensor reads, no MQTT, no publish retries -- just get
    // to WiFi and start listening for an OTA upload as quickly as possible.
    connectWiFi();

    if (WiFi.status() == WL_CONNECTED) {
      enterOtaMode();
    } else {
      Serial.println("OTA requested but WiFi failed to connect -- going back to sleep.");
      blink(3, 20, 200); // same failure indicator as a normal failed cycle
    }

    if (DEBUG_MODE) {
      stopAwakeWatchdog();
      Serial.println("=== DEBUG_MODE: staying connected, entering loop() ===");
      return;
    }

    WiFi.disconnect(true);
    stopAwakeWatchdog();
    goToSleep();
    return;
  }

  // Power on SHTC3 and let it settle
  digitalWrite(SENSOR_POWER_PIN, HIGH);
  delay(SENSOR_POWER_SETTLE_MS);

  Wire.begin(SDA_PIN, SCL_PIN);
  bool shtOk = shtc3.begin(&Wire);
  float tempC = NAN, humidity = NAN;
  if (shtOk) {
    sensors_event_t humEvent, tempEvent;
    shtc3.getEvent(&humEvent, &tempEvent);
    tempC = tempEvent.temperature + TEMP_OFFSET_C;
    humidity = humEvent.relative_humidity;
  } else {
    Serial.println("SHTC3 not found on I2C bus!");
  }

  float battV = readBatteryVoltage();
  float battPct = batteryPercentage(battV);

  connectWiFi();

  bool published = false;
  bool otaModeEntered = false;
  int rssi = 0;

  if (WiFi.status() == WL_CONNECTED) {
    rssi = WiFi.RSSI();

    if (connectMQTT()) {
      // Subscribe now -- retained messages get delivered right away, and by
      // the time publishState() below finishes, there's been enough time
      // for the library's background task to have received and processed
      // it, with no extra blocking wait added on our part.
      g_otaRequestReceived = false;
      mqttClient.subscribe(TOPIC_OTA_REQUEST.c_str(), 1);

      if (!discoverySent) {
        sendDiscoveryConfig();
        discoverySent = true;
      }
      int unackedTopics = publishState(tempC, humidity, battV, battPct, rssi, resetReasonStr, connectFailCount);

      if (unackedTopics == 0) {
        published = true;
        connectFailCount = 0; // every topic confirmed by the broker, counter clears
      } else {
        Serial.printf("%d topic(s) never got a PUBACK this cycle.\n", unackedTopics);
        connectFailCount++;
        totalFailCount++;
      }

      bool remoteOtaRequested = g_otaRequestReceived && strcmp(g_otaRequestPayload, "ON") == 0;
      if (remoteOtaRequested) {
        Serial.println("Remote OTA request received via MQTT.");
        publishWithAck(TOPIC_OTA_REQUEST.c_str(), "OFF", true); // clear the flag so it doesn't retrigger next wake
        enterOtaMode();
        otaModeEntered = true;
      }
    } else {
      Serial.println("MQTT connect failed, skipping publish this cycle.");
      connectFailCount++;
      totalFailCount++;
    }
  } else {
    Serial.println("WiFi connect failed, skipping publish this cycle.");
    connectFailCount++;
    totalFailCount++;
  }

  // LED feedback: one short blink on success, three quick blinks on failure.
  // Skipped if we just ran the OTA flourish blink instead.
  if (!otaModeEntered) {
    if (published) {
      blink(1, 20, 200);
    } else {
      blink(3, 20, 200);
    }
  }

  digitalWrite(SENSOR_POWER_PIN, LOW);

  if (DEBUG_MODE) {
    stopAwakeWatchdog(); // staying awake indefinitely is intentional in debug mode
    Serial.println("=== DEBUG_MODE: staying connected, entering loop() ===");
    return;
  }

  mqttClient.disconnect();
  delay(100);
  WiFi.disconnect(true);

  stopAwakeWatchdog(); // about to sleep on our own terms, no need for the failsafe to fire mid-sleep
  goToSleep();
}

void loop() {
  if (DEBUG_MODE) {
    delay(1000); // espMqttClient runs its own background task, nothing to pump here
  }
  // In normal (non-debug) operation this is never reached — device sleeps at the end of setup()
}

// ---------------- WiFi ----------------

// Single connection attempt on the given channel (0 = let the radio auto-scan/pick).
// Returns true if connected within WIFI_CONNECT_TIMEOUT_MS.
bool attemptWifiConnect(uint8_t channel, bool useBSSID) {
  if (useBSSID) {
    WiFi.begin(WIFI_SSID, WIFI_PASSWORD, channel, WIFI_BSSID);
  } else {
    WiFi.begin(WIFI_SSID, WIFI_PASSWORD, channel);
  }

  unsigned long start = millis();
  wl_status_t lastStatus = WiFi.status();
  while (WiFi.status() != WL_CONNECTED && millis() - start < WIFI_CONNECT_TIMEOUT_MS) {
    delay(50);
    wl_status_t s = WiFi.status();
    if (s != lastStatus) {
      Serial.printf("[debug] WiFi status changed: %d\n", s);
      lastStatus = s;
    }
  }

  if (WiFi.status() == WL_CONNECTED) {
    Serial.printf("WiFi connected in %lums, IP: %s, channel: %u\n",
                  millis() - start, WiFi.localIP().toString().c_str(), WiFi.channel());
    return true;
  }

  Serial.printf("[debug] Final WiFi status: %d (0=IDLE,1=NO_SSID,3=CONNECTED,4=CONNECT_FAILED,6=DISCONNECTED)\n",
                WiFi.status());
  return false;
}

void connectWiFi() {
  WiFi.mode(WIFI_STA);
  WiFi.setSleep(false); // disable modem power-save during connect -- noticeably faster/more reliable, especially right out of deep sleep

  if (USE_STATIC_IP) {
    WiFi.config(STATIC_IP, GATEWAY, SUBNET, DNS_SERVER);
  }

  bool useBSSID = false;
  for (int i = 0; i < 6; i++) {
    if (WIFI_BSSID[i] != 0x00) { useBSSID = true; break; }
  }

  // Reusing the channel from the last successful connect skips a small
  // amount of scan/negotiation time. cachedWifiChannel is 0 (auto) until
  // the first successful connect populates it below.
  bool connected = attemptWifiConnect(cachedWifiChannel, useBSSID);

  // If the cached channel attempt failed (e.g. the router switched channels
  // since we last connected), fall back to one auto-scan retry before
  // giving up on this cycle entirely.
  if (!connected && cachedWifiChannel != 0) {
    Serial.println("[debug] Cached-channel connect failed, retrying with auto channel scan...");
    WiFi.disconnect();
    delay(100);
    connected = attemptWifiConnect(0, useBSSID);
  }

  cachedWifiChannel = connected ? WiFi.channel() : 0;
}

// ---------------- MQTT ----------------

bool connectMQTT() {
  mqttClient.setServer(MQTT_BROKER, MQTT_PORT);
  mqttClient.setCredentials(MQTT_USER, MQTT_PASSWORD);
  mqttClient.setClientId(DEVICE_ID);
  mqttClient.onPublish(onMqttPublish);
  mqttClient.onMessage(onMqttMessage);
  mqttClient.connect(); // non-blocking on ESP32 -- offloaded to the library's own task

  unsigned long start = millis();
  while (!mqttClient.connected() && millis() - start < MQTT_CONNECT_TIMEOUT_MS) {
    delay(50);
  }

  if (mqttClient.connected()) {
    Serial.printf("MQTT connected in %lums\n", millis() - start);
    return true;
  }

  Serial.println("MQTT connect failed/timed out");
  return false;
}

// Publishes one topic at QoS 1 and waits for the broker's PUBACK before
// returning. Retries up to MQTT_PUBLISH_RETRIES times if the ack doesn't
// arrive in time (dropped packet, broker hiccup, etc.) or if the library
// fails to even queue the publish (packetId 0, e.g. internal queue full).
bool publishWithAck(const char* topic, const char* payload, bool retained) {
  for (uint8_t attempt = 1; attempt <= MQTT_PUBLISH_RETRIES; attempt++) {
    g_packetAcked = false;
    g_trackedPacketId = 0;

    uint16_t packetId = mqttClient.publish(topic, 1 /* QoS 1 */, retained, payload);
    if (packetId == 0) {
      Serial.printf("[mqtt] queue failed for %s (attempt %u/%u)\n", topic, attempt, MQTT_PUBLISH_RETRIES);
      delay(150);
      continue;
    }
    g_trackedPacketId = packetId;

    unsigned long waitStart = millis();
    while (!g_packetAcked && millis() - waitStart < MQTT_ACK_TIMEOUT_MS) {
      delay(10);
    }

    if (g_packetAcked) {
      return true;
    }
    Serial.printf("[mqtt] no PUBACK for %s (packetId %u, attempt %u/%u)\n",
                  topic, packetId, attempt, MQTT_PUBLISH_RETRIES);
  }

  Serial.printf("[mqtt] giving up on %s after %u attempts\n", topic, MQTT_PUBLISH_RETRIES);
  return false;
}

void sendDiscoveryConfig() {
  String devBlock = String("\"device\":{\"identifiers\":[\"") + DEVICE_ID
      + "\"],\"name\":\"" + DEVICE_NAME
      + "\",\"manufacturer\":\"" + DEVICE_MANUFACTURER
      + "\",\"model\":\"" + DEVICE_MODEL
      + "\",\"sw_version\":\"" + FIRMWARE_VERSION
      + "\",\"hw_version\":\"" + DEVICE_HW_VERSION + "\"}";

  String tempPayload = String("{")
    + "\"name\":\"" + DEVICE_NAME + " Temperature\","
    + "\"unique_id\":\"" + DEVICE_ID + "_temperature\","
    + "\"device_class\":\"temperature\","
    + "\"unit_of_measurement\":\"°C\","
    + "\"state_class\":\"measurement\","
    + "\"suggested_display_precision\":2,"
    + "\"expire_after\":" + String(EXPIRE_AFTER_SEC) + ","
    + "\"state_topic\":\"" + TOPIC_TEMP + "\","
    + devBlock + "}";
  publishWithAck(DISCOVERY_TEMP.c_str(), tempPayload.c_str(), true);

  String humPayload = String("{")
    + "\"name\":\"" + DEVICE_NAME + " Humidity\","
    + "\"unique_id\":\"" + DEVICE_ID + "_humidity\","
    + "\"device_class\":\"humidity\","
    + "\"unit_of_measurement\":\"%\","
    + "\"state_class\":\"measurement\","
    + "\"suggested_display_precision\":1,"
    + "\"expire_after\":" + String(EXPIRE_AFTER_SEC) + ","
    + "\"state_topic\":\"" + TOPIC_HUMIDITY + "\","
    + devBlock + "}";
  publishWithAck(DISCOVERY_HUMIDITY.c_str(), humPayload.c_str(), true);

  String battVPayload = String("{")
    + "\"name\":\"" + DEVICE_NAME + " Battery Voltage\","
    + "\"unique_id\":\"" + DEVICE_ID + "_battery_voltage\","
    + "\"device_class\":\"voltage\","
    + "\"unit_of_measurement\":\"V\","
    + "\"state_class\":\"measurement\","
    + "\"suggested_display_precision\":2,"
    + "\"expire_after\":" + String(EXPIRE_AFTER_SEC) + ","
    + "\"state_topic\":\"" + TOPIC_BATTERY_V + "\","
    + devBlock + "}";
  publishWithAck(DISCOVERY_BATTERY_V.c_str(), battVPayload.c_str(), true);

  String battPctPayload = String("{")
    + "\"name\":\"" + DEVICE_NAME + " Battery\","
    + "\"unique_id\":\"" + DEVICE_ID + "_battery_percent\","
    + "\"device_class\":\"battery\","
    + "\"unit_of_measurement\":\"%\","
    + "\"state_class\":\"measurement\","
    + "\"suggested_display_precision\":0,"
    + "\"expire_after\":" + String(EXPIRE_AFTER_SEC) + ","
    + "\"state_topic\":\"" + TOPIC_BATTERY_PCT + "\","
    + devBlock + "}";
  publishWithAck(DISCOVERY_BATTERY_PCT.c_str(), battPctPayload.c_str(), true);

  String rssiPayload = String("{")
    + "\"name\":\"" + DEVICE_NAME + " WiFi Signal\","
    + "\"unique_id\":\"" + DEVICE_ID + "_wifi_signal\","
    + "\"device_class\":\"signal_strength\","
    + "\"unit_of_measurement\":\"dBm\","
    + "\"state_class\":\"measurement\","
    + "\"entity_category\":\"diagnostic\","
    + "\"expire_after\":" + String(EXPIRE_AFTER_SEC) + ","
    + "\"state_topic\":\"" + TOPIC_RSSI + "\","
    + devBlock + "}";
  publishWithAck(DISCOVERY_RSSI.c_str(), rssiPayload.c_str(), true);

  String lastUpdatePayload = String("{")
    + "\"name\":\"" + DEVICE_NAME + " Last Update\","
    + "\"unique_id\":\"" + DEVICE_ID + "_last_update\","
    + "\"device_class\":\"timestamp\","
    + "\"entity_category\":\"diagnostic\","
    + "\"expire_after\":" + String(EXPIRE_AFTER_SEC) + ","
    + "\"state_topic\":\"" + TOPIC_LAST_UPDATE + "\","
    + devBlock + "}";
  publishWithAck(DISCOVERY_LAST_UPDATE.c_str(), lastUpdatePayload.c_str(), true);

  String resetReasonPayload = String("{")
    + "\"name\":\"" + DEVICE_NAME + " Reset Reason\","
    + "\"unique_id\":\"" + DEVICE_ID + "_reset_reason\","
    + "\"entity_category\":\"diagnostic\","
    + "\"icon\":\"mdi:restart-alert\","
    + "\"expire_after\":" + String(EXPIRE_AFTER_SEC) + ","
    + "\"state_topic\":\"" + TOPIC_RESET_REASON + "\","
    + devBlock + "}";
  publishWithAck(DISCOVERY_RESET_REASON.c_str(), resetReasonPayload.c_str(), true);

  String failCountPayload = String("{")
    + "\"name\":\"" + DEVICE_NAME + " Connect Fail Count\","
    + "\"unique_id\":\"" + DEVICE_ID + "_connect_fail_count\","
    + "\"entity_category\":\"diagnostic\","
    + "\"state_class\":\"measurement\","
    + "\"icon\":\"mdi:wifi-alert\","
    + "\"expire_after\":" + String(EXPIRE_AFTER_SEC) + ","
    + "\"state_topic\":\"" + TOPIC_FAIL_COUNT + "\","
    + devBlock + "}";
  publishWithAck(DISCOVERY_FAIL_COUNT.c_str(), failCountPayload.c_str(), true);

  String totalFailCountPayload = String("{")
    + "\"name\":\"" + DEVICE_NAME + " Total Fail Count\","
    + "\"unique_id\":\"" + DEVICE_ID + "_total_fail_count\","
    + "\"entity_category\":\"diagnostic\","
    + "\"state_class\":\"total_increasing\","
    + "\"icon\":\"mdi:counter\","
    + "\"expire_after\":" + String(EXPIRE_AFTER_SEC) + ","
    + "\"state_topic\":\"" + TOPIC_TOTAL_FAIL_COUNT + "\","
    + devBlock + "}";
  publishWithAck(DISCOVERY_TOTAL_FAIL_COUNT.c_str(), totalFailCountPayload.c_str(), true);

  String fwVersionPayload = String("{")
    + "\"name\":\"" + DEVICE_NAME + " Firmware Version\","
    + "\"unique_id\":\"" + DEVICE_ID + "_firmware_version\","
    + "\"entity_category\":\"diagnostic\","
    + "\"icon\":\"mdi:chip\","
    + "\"expire_after\":" + String(EXPIRE_AFTER_SEC) + ","
    + "\"state_topic\":\"" + TOPIC_FW_VERSION + "\","
    + devBlock + "}";
  publishWithAck(DISCOVERY_FW_VERSION.c_str(), fwVersionPayload.c_str(), true);

  String battLowPayload = String("{")
    + "\"name\":\"" + DEVICE_NAME + " Low Battery\","
    + "\"unique_id\":\"" + DEVICE_ID + "_battery_low\","
    + "\"device_class\":\"battery\","
    + "\"entity_category\":\"diagnostic\","
    + "\"payload_on\":\"ON\","
    + "\"payload_off\":\"OFF\","
    + "\"expire_after\":" + String(EXPIRE_AFTER_SEC) + ","
    + "\"state_topic\":\"" + TOPIC_BATTERY_LOW + "\","
    + devBlock + "}";
  publishWithAck(DISCOVERY_BATTERY_LOW.c_str(), battLowPayload.c_str(), true);

  String bootCountPayload = String("{")
    + "\"name\":\"" + DEVICE_NAME + " Boot Count\","
    + "\"unique_id\":\"" + DEVICE_ID + "_boot_count\","
    + "\"entity_category\":\"diagnostic\","
    + "\"state_class\":\"total_increasing\","
    + "\"icon\":\"mdi:counter\","
    + "\"expire_after\":" + String(EXPIRE_AFTER_SEC) + ","
    + "\"state_topic\":\"" + TOPIC_BOOT_COUNT + "\","
    + devBlock + "}";
  publishWithAck(DISCOVERY_BOOT_COUNT.c_str(), bootCountPayload.c_str(), true);

  // Note: no expire_after here -- this is a control, not a reading, and
  // should stay usable in the UI even if the device has been quiet a while.
  // command_topic and state_topic are the same: the device echoes the
  // state back itself once it has actually acted on a request, so the
  // switch reflects reality (ON only briefly, until the next wake handles
  // it and reports back OFF) rather than just optimistically flipping.
  String otaRequestPayload = String("{")
    + "\"name\":\"" + DEVICE_NAME + " OTA Request\","
    + "\"unique_id\":\"" + DEVICE_ID + "_ota_request\","
    + "\"entity_category\":\"config\","
    + "\"icon\":\"mdi:upload\","
    + "\"payload_on\":\"ON\","
    + "\"payload_off\":\"OFF\","
    + "\"optimistic\":false,"
    + "\"retain\":true,"
    + "\"command_topic\":\"" + TOPIC_OTA_REQUEST + "\","
    + "\"state_topic\":\"" + TOPIC_OTA_REQUEST + "\","
    + devBlock + "}";
  publishWithAck(DISCOVERY_OTA_REQUEST.c_str(), otaRequestPayload.c_str(), true);
}

int publishState(float tempC, float humidity, float battV, float battPct, int rssi, const String& resetReasonStr, uint32_t failCount) {
  char buf[16];

  int failed = 0;

  if (!isnan(tempC)) {
    dtostrf(tempC, 4, 2, buf);
    if (!publishWithAck(TOPIC_TEMP.c_str(), buf, true)) failed++;
  }
  if (!isnan(humidity)) {
    dtostrf(humidity, 4, 2, buf);
    if (!publishWithAck(TOPIC_HUMIDITY.c_str(), buf, true)) failed++;
  }

  dtostrf(battV, 4, 2, buf);
  if (!publishWithAck(TOPIC_BATTERY_V.c_str(), buf, true)) failed++;

  dtostrf(battPct, 4, 0, buf);
  if (!publishWithAck(TOPIC_BATTERY_PCT.c_str(), buf, true)) failed++;

  bool batteryLow = battPct < BATTERY_LOW_THRESHOLD_PCT;
  if (!publishWithAck(TOPIC_BATTERY_LOW.c_str(), batteryLow ? "ON" : "OFF", true)) failed++;

  snprintf(buf, sizeof(buf), "%d", rssi);
  if (!publishWithAck(TOPIC_RSSI.c_str(), buf, true)) failed++;

  if (!publishWithAck(TOPIC_RESET_REASON.c_str(), resetReasonStr.c_str(), true)) failed++;

  snprintf(buf, sizeof(buf), "%lu", (unsigned long)failCount);
  if (!publishWithAck(TOPIC_FAIL_COUNT.c_str(), buf, true)) failed++;

  snprintf(buf, sizeof(buf), "%lu", (unsigned long)totalFailCount);
  if (!publishWithAck(TOPIC_TOTAL_FAIL_COUNT.c_str(), buf, true)) failed++;

  if (!publishWithAck(TOPIC_FW_VERSION.c_str(), FIRMWARE_VERSION, true)) failed++;

  snprintf(buf, sizeof(buf), "%lu", (unsigned long)bootCount);
  if (!publishWithAck(TOPIC_BOOT_COUNT.c_str(), buf, true)) failed++;

  // NTP resync happens here, last -- after every other reading has already
  // published successfully. This way a slow or failed sync (a real network
  // round trip to an external server, unlike everything else which only
  // talks to the local broker) can only cost the last_update field, never
  // delay or risk the actual sensor data. Only attempted every N boots (or
  // if we've genuinely never synced) since 10-minute drift is negligible.
  bool dueForResync = !g_timeSynced || (bootCount % NTP_RESYNC_EVERY_N_BOOTS == 0);
  if (dueForResync) {
    syncTimeUtc();
  }

  // Read the timestamp last, right before publishing it, so it reflects
  // when this cycle actually finished rather than when it started.
  String lastUpdate = getCurrentTimestampUtc();
  if (lastUpdate.length() > 0) {
    if (!publishWithAck(TOPIC_LAST_UPDATE.c_str(), lastUpdate.c_str(), true)) failed++;
  }

  Serial.printf("Published: temp=%.2fC hum=%.2f%% battV=%.2f battPct=%.0f%% (low=%s) rssi=%ddBm lastUpdate=%s resetReason=%s failCount=%lu fw=%s (unacked topics: %d)\n",
                tempC, humidity, battV, battPct, batteryLow ? "yes" : "no", rssi, lastUpdate.c_str(), resetReasonStr.c_str(), (unsigned long)failCount, FIRMWARE_VERSION, failed);

  return failed;
}

// ---------------- Battery ----------------

float readBatteryVoltage() {
  // analogReadMilliVolts() uses the ESP32's factory ADC calibration (eFuse)
  // for an accurate mV reading -- far more accurate than manually mapping
  // raw analogRead() counts against an assumed 3.3V reference.
  // Averaged over BATT_ADC_SAMPLES reads to smooth out ADC noise, matching
  // the original ESPHome config's samples: 16.
  uint32_t sumMillivolts = 0;
  for (int i = 0; i < BATT_ADC_SAMPLES; i++) {
    sumMillivolts += analogReadMilliVolts(BATT_PIN);
    delay(2); // small gap between reads
  }
  uint32_t rawMillivolts = sumMillivolts / BATT_ADC_SAMPLES;
  float raw = (rawMillivolts / 1000.0f) * BATT_DIVIDER_RATIO;

  // Apply the same piecewise-linear correction as the ESPHome calibrate_linear filter
  if (raw <= BATT_CAL[0].raw) {
    // Extrapolate below the first point using the first segment's slope
    float slope = (BATT_CAL[1].actual - BATT_CAL[0].actual) / (BATT_CAL[1].raw - BATT_CAL[0].raw);
    return BATT_CAL[0].actual + (raw - BATT_CAL[0].raw) * slope;
  }
  if (raw >= BATT_CAL[BATT_CAL_POINTS - 1].raw) {
    // Extrapolate above the last point using the last segment's slope
    float slope = (BATT_CAL[BATT_CAL_POINTS - 1].actual - BATT_CAL[BATT_CAL_POINTS - 2].actual)
                 / (BATT_CAL[BATT_CAL_POINTS - 1].raw - BATT_CAL[BATT_CAL_POINTS - 2].raw);
    return BATT_CAL[BATT_CAL_POINTS - 1].actual + (raw - BATT_CAL[BATT_CAL_POINTS - 1].raw) * slope;
  }
  for (int i = 0; i < BATT_CAL_POINTS - 1; i++) {
    if (raw >= BATT_CAL[i].raw && raw <= BATT_CAL[i + 1].raw) {
      float slope = (BATT_CAL[i + 1].actual - BATT_CAL[i].actual) / (BATT_CAL[i + 1].raw - BATT_CAL[i].raw);
      return BATT_CAL[i].actual + (raw - BATT_CAL[i].raw) * slope;
    }
  }
  return raw; // unreachable, keeps the compiler happy
}

float batteryPercentage(float v) {
  float pct;

  // Rescaled from the original 4.20V-100%/3.20V-0% curve: every breakpoint
  // proportionally compressed by 0.95 (= 0.95V new span / 1.00V old span)
  // so the curve's shape (same relative tier widths, same emphasis on the
  // 3.5-3.9V "knee") is preserved but fit to 4.15V-100%/3.20V-0% instead of
  // just flattening the top into an instant jump at 4.15V.
  if (v >= 4.15)      pct = 100.0;
  else if (v >= 4.10) pct = 95.0 + (v - 4.10) / 0.05 * 5.0;
  else if (v >= 4.06) pct = 90.0 + (v - 4.06) / 0.04 * 5.0;
  else if (v >= 4.01) pct = 85.0 + (v - 4.01) / 0.05 * 5.0;
  else if (v >= 3.96) pct = 80.0 + (v - 3.96) / 0.05 * 5.0;
  else if (v >= 3.91) pct = 75.0 + (v - 3.91) / 0.05 * 5.0;
  else if (v >= 3.87) pct = 70.0 + (v - 3.87) / 0.04 * 5.0;
  else if (v >= 3.80) pct = 65.0 + (v - 3.80) / 0.07 * 5.0;
  else if (v >= 3.72) pct = 60.0 + (v - 3.72) / 0.08 * 5.0;
  else if (v >= 3.68) pct = 55.0 + (v - 3.68) / 0.04 * 5.0;
  else if (v >= 3.66) pct = 50.0 + (v - 3.66) / 0.02 * 5.0;
  else if (v >= 3.62) pct = 45.0 + (v - 3.62) / 0.04 * 5.0;
  else if (v >= 3.58) pct = 40.0 + (v - 3.58) / 0.04 * 5.0;
  else if (v >= 3.54) pct = 35.0 + (v - 3.54) / 0.04 * 5.0;
  else if (v >= 3.49) pct = 30.0 + (v - 3.49) / 0.05 * 5.0;
  else if (v >= 3.44) pct = 25.0 + (v - 3.44) / 0.05 * 5.0;
  else if (v >= 3.39) pct = 20.0 + (v - 3.39) / 0.05 * 5.0;
  else if (v >= 3.34) pct = 15.0 + (v - 3.34) / 0.05 * 5.0;
  else if (v >= 3.30) pct = 10.0 + (v - 3.30) / 0.04 * 5.0;
  else if (v >= 3.25) pct =  5.0 + (v - 3.25) / 0.05 * 5.0;
  else if (v >= 3.20) pct =  0.0 + (v - 3.20) / 0.05 * 5.0;
  else                pct = 0.0;

  return roundf(pct);
}

// ---------------- Time ----------------

bool syncTimeUtc() {
  // The system clock survives deep sleep, so on every wake after the first,
  // it already holds a "valid-looking" value carried over from last cycle.
  // getLocalTime() only checks whether the year looks sane (>2016) -- it
  // doesn't confirm a fresh NTP packet actually arrived -- so without this,
  // it reports success instantly using the old, drifted value instead of
  // actually waiting for a real sync. Blanking the clock first forces a
  // genuine wait. We snapshot the old value first so a failed attempt can
  // restore it rather than leaving the clock blanked at epoch 0.
  struct timeval previous;
  gettimeofday(&previous, nullptr);

  struct timeval invalidate = {0, 0};
  settimeofday(&invalidate, nullptr);

  configTime(0, 0, NTP_SERVER); // 0, 0 = UTC, no DST offset

  struct tm timeinfo;
  if (!getLocalTime(&timeinfo, NTP_SYNC_TIMEOUT_MS)) {
    Serial.println("NTP sync failed this cycle.");
    if (g_timeSynced) {
      settimeofday(&previous, nullptr); // restore last known-good time rather than leaving it blanked
      Serial.println("Restored previous time (still using last successful sync).");
    }
    return false;
  }

  g_timeSynced = true;
  return true;
}

// Reads the current time from the already-synced system clock -- no network
// call. Call this right before actually publishing the timestamp, not
// earlier, so it reflects when the message really goes out rather than
// when NTP happened to sync at the start of the cycle (QoS 1 retries on
// earlier topics can otherwise leave a visible gap between the two).
String getCurrentTimestampUtc() {
  if (!g_timeSynced) return "";

  time_t now = time(nullptr);
  struct tm timeinfo;
  gmtime_r(&now, &timeinfo);

  char buf[25];
  strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%SZ", &timeinfo);
  return String(buf);
}

// Human-readable form of esp_reset_reason() — the key one to watch for is
// ESP_RST_BROWNOUT, which means the supply voltage sagged below the chip's
// threshold (usually a current spike, e.g. during WiFi TX) and it reset
// mid-cycle rather than completing a normal report.
String resetReasonToString(esp_reset_reason_t reason) {
  switch (reason) {
    case ESP_RST_POWERON:   return "power_on";
    case ESP_RST_EXT:       return "external_pin";
    case ESP_RST_SW:        return "software";
    case ESP_RST_PANIC:     return "panic";
    case ESP_RST_INT_WDT:   return "interrupt_watchdog";
    case ESP_RST_TASK_WDT:  return "task_watchdog";
    case ESP_RST_WDT:       return "other_watchdog";
    case ESP_RST_DEEPSLEEP: return "deep_sleep_wake";
    case ESP_RST_BROWNOUT:  return "brownout";
    case ESP_RST_SDIO:      return "sdio";
    default:                return "unknown";
  }
}

// ---------------- LED / OTA ----------------

void blink(int times, uint32_t onMs, uint32_t gapMs) {
  uint32_t duty = ((uint32_t)LED_BRIGHTNESS_PCT * LED_PWM_MAX_DUTY) / 100;
  for (int i = 0; i < times; i++) {
    ledcWrite(LED_PIN, duty);
    delay(onMs);
    ledcWrite(LED_PIN, 0);
    if (i < times - 1) delay(gapMs);
  }
}

void runOtaWindow() {
  unsigned long start = millis();
  while (millis() - start < OTA_WINDOW_MS) {
    ArduinoOTA.handle();
    delay(10);
  }
  Serial.println("OTA window elapsed, resuming normal sleep cycle.");
}

// Shared by both OTA trigger paths (physical button and remote/MQTT
// request). Assumes WiFi is already connected.
void enterOtaMode() {
  Serial.println("Entering OTA mode, deep sleep prevented...");
  startAwakeWatchdog(OTA_WINDOW_MS + 30000); // OTA legitimately needs to stay awake this long
  ArduinoOTA.setHostname(DEVICE_ID);
  ArduinoOTA.setPassword(OTA_PASSWORD);
  ArduinoOTA.begin();
  ledcWrite(LED_PIN, ((uint32_t)LED_BRIGHTNESS_PCT * LED_PWM_MAX_DUTY) / 100); // solid LED = OTA mode active
  runOtaWindow();
  ledcWrite(LED_PIN, 0);
  blink(1, 50, 50); // brief off/on/off flourish before sleeping, mirrors ota_timeout_monitor
}

// ---------------- Sleep ----------------

void goToSleep() {
  esp_sleep_enable_timer_wakeup(SLEEP_INTERVAL_US);

  // Wake immediately if the OTA button is pressed, rather than waiting for
  // the next scheduled timer wake. The ESP32-C3 has no separate RTC IO
  // domain, so esp_deep_sleep_enable_gpio_wakeup works on any GPIO --
  // pressing the button pulls it LOW, which wakes the device straight into
  // setup(), where the existing digitalRead(OTA_PIN) check already detects
  // it and enters OTA mode. No separate reset needed.
  uint64_t otaPinMask = 1ULL << OTA_PIN;
  esp_deep_sleep_enable_gpio_wakeup(otaPinMask, ESP_GPIO_WAKEUP_GPIO_LOW);

  Serial.println("Going to sleep...");
  Serial.flush();
  esp_deep_sleep_start();
}
