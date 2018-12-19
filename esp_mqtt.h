#ifndef ESP_MQTT_H
#define ESP_MQTT_H

#include <esp_log.h>
#include <sdkconfig.h>
#include <stdbool.h>
#include <stdint.h>
#include <freertos/FreeRTOS.h>
#include <freertos/semphr.h>
#include <freertos/task.h>
#include <freertos/queue.h>
#include <lwmqtt.h>
#include <stdio.h>
#include <string.h>
#include <esp_err.h>

#if defined(CONFIG_ESP_MQTT_TLS_ENABLE)
#include "esp_tls_lwmqtt.h"
#endif

#include "esp_lwmqtt.h"

/**
 * The message callback.
 */
typedef void (*esp_mqtt_message_callback_t)(const char *topic, uint8_t *payload, size_t len);

typedef struct {
  char *host;
  char *port;
  char *client_id;
  char *username;
  char *password;
} esp_mqtt_config_t;

typedef struct
{
    char *topic;
    char *payload;
    int qos;
    bool retained;
} esp_mqtt_lwt_config_t;

/**
 * The statuses emitted by the status callback.
 */
typedef enum esp_mqtt_status_t { ESP_MQTT_STATUS_DISCONNECTED, ESP_MQTT_STATUS_CONNECTED } esp_mqtt_status_t;

/**
 * The status callback.
 *
 */
typedef void (*esp_mqtt_status_callback_t)(void *settings, esp_mqtt_status_t);

/**
 * Structure of mqtt config
 */
typedef struct
{
	esp_mqtt_status_callback_t esp_mqtt_status_callback;
	esp_mqtt_message_callback_t esp_mqtt_message_callback;
	size_t esp_mqtt_buffer_size;
	uint32_t esp_mqtt_command_timeout;
	void *esp_mqtt_write_buffer;
	void *esp_mqtt_read_buffer;
	SemaphoreHandle_t esp_mqtt_main_mutex;
	SemaphoreHandle_t esp_mqtt_select_mutex;
	QueueHandle_t esp_mqtt_event_queue;
	TaskHandle_t esp_mqtt_task;
	bool esp_mqtt_running;
	bool esp_mqtt_connected;
	bool esp_mqtt_error;
	esp_mqtt_config_t esp_mqtt_cfg;
	lwmqtt_client_t esp_mqtt_client;
	#if !(defined(CONFIG_ESP_MQTT_TLS_ENABLE) && defined(CONFIG_ESP_MQTT_TLS_ONLY))
	esp_lwmqtt_network_t esp_mqtt_network;
	#endif
	#if defined(CONFIG_ESP_MQTT_TLS_ENABLE)
	esp_tls_lwmqtt_network_t esp_tls_mqtt_network;
	#endif
	esp_lwmqtt_timer_t esp_mqtt_timer1, esp_mqtt_timer2;
	esp_mqtt_lwt_config_t esp_mqtt_lwt_config;
} esp_mqtt_settings_t;

/**
 * Initialize the MQTT management system.
 *
 * Note: Should only be called once on boot.
 *
 * @param scb - The status callback.
 * @param mcb - The message callback.
 * @param buffer_size - The read and write buffer size.
 * @param command_timeout - The command timeout.
 *
 * @return Identifier of mqtt structure
 */
esp_mqtt_settings_t *esp_mqtt_init(esp_mqtt_status_callback_t scb, esp_mqtt_message_callback_t mcb, size_t buffer_size,
                   int command_timeout);

/**
 * Configure Last Will and Testament.
 *
 * Note: Must be called before esp_mqtt_start.
 *
 * @param topic - The LWT topic.
 * @param payload - The LWT payload.
 * @param qos - The LWT QoS level.
 * @param retained - The LWT retained flag.
 */
void esp_mqtt_lwt(const char *topic, const char *payload, int qos, bool retained, esp_mqtt_settings_t *settings);

#if defined(CONFIG_ESP_MQTT_TLS_ENABLE)
/**
 * Configure TLS connection
 *
 * Note: This method must be called before `esp_mqtt_start`.
 *
 * @param verify - Set verify connection.
 * @param cacert - Pointer to CA certificate.
 * @return Whether TLS configuration successful.
 */
bool esp_mqtt_tls(esp_mqtt_settings_t *settings, bool verify, const unsigned char * cacert, size_t cacert_len);
#endif

/**
 * Start the MQTT process.
 *
 * The background process will attempt to connect to the specified broker once a second until a connection can be
 * established. This process can be interrupted by calling `esp_mqtt_stop();`. If a connection has been established,
 * the status callback will be called with `ESP_MQTT_STATUS_CONNECTED`. From that moment on the functions
 * `esp_mqtt_subscribe`, `esp_mqtt_unsubscribe` and `esp_mqtt_publish` can be used to interact with the broker.
 *
 * @param host - The broker host.
 * @param port - The broker port.
 * @param client_id - The client id.
 * @param username - The client username.
 * @param password - The client password.
 * @param settings - Pointer to esp_mqtt_settings which esp_mqtt_init() initializate before.
 *
 * @return ESP_OK if all good, ESP_ERR_INVALID_ARG if bad arguments or ESP_ERR_NO_MEM if not enough memory
 */
esp_err_t esp_mqtt_start(const char *host, const char *port, const char *client_id, const char *username,
                    const char *password, esp_mqtt_settings_t *settings);

/**
 * Subscribe to specified topic.
 *
 * When false is returned the current operation failed and any subsequent interactions will also fail. This can be used
 * to handle errors early. As soon as the background process unblocks the error will be detected, the connection closed
 * and the status callback invoked with `ESP_MQTT_STATUS_DISCONNECTED`. That callback then can simply call
 * `esp_mqtt_start()` to attempt an reconnection.
 *
 * @param topic - The topic.
 * @param qos - The qos level.
 * @return Whether the operation was successful.
 */
bool esp_mqtt_subscribe(esp_mqtt_settings_t *settings, const char *topic, int qos);

/**
 * Unsubscribe from specified topic.
 *
 * When false is returned the current operation failed and any subsequent interactions will also fail. This can be used
 * to handle errors early. As soon as the background process unblocks the error will be detected, the connection closed
 * and the status callback invoked with `ESP_MQTT_STATUS_DISCONNECTED`. That callback then can simply call
 * `esp_mqtt_start()` to attempt an reconnection.
 *
 * @param topic - The topic.
 * @return Whether the operation was successful.
 */
bool esp_mqtt_unsubscribe(esp_mqtt_settings_t *settings, const char *topic);

/**
 * Publish bytes payload to specified topic.
 *
 * When false is returned the current operation failed and any subsequent interactions will also fail. This can be used
 * to handle errors early. As soon as the background process unblocks the error will be detected, the connection closed
 * and the status callback invoked with `ESP_MQTT_STATUS_DISCONNECTED`. That callback then can simply call
 * `esp_mqtt_start()` to attempt an reconnection.
 *
 * @param topic - The topic.
 * @param payload - The payload.
 * @param len - The payload length.
 * @param qos - The qos level.
 * @param retained - The retained flag.
 * @return Whether the operation was successful.
 */
bool esp_mqtt_publish(esp_mqtt_settings_t *settings, const char *topic, uint8_t *payload, size_t len, int qos, bool retained);

/**
 * Stop the MQTT process.
 *
 * Will stop initial connection attempts or disconnect any active connection.
 */
void esp_mqtt_stop(esp_mqtt_settings_t *settings);

/**
 * Function delete mqtt config
 */
void esp_mqtt_delete(esp_mqtt_settings_t *settings);

/**
 * Function clear esp_mqtt_cfg
 */
void esp_mqtt_clear_cfg(esp_mqtt_config_t *cfg);

/**
 * Function clear esp_mqtt_lwt_cfg
 */
void esp_mqtt_lwt_clear_cfg(esp_mqtt_lwt_config_t *cfg);

#endif  // ESP_MQTT_H
