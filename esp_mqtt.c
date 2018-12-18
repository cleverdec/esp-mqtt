#include <esp_log.h>
#include <freertos/FreeRTOS.h>
#include <freertos/semphr.h>
#include <freertos/task.h>
#include <lwmqtt.h>
#include <stdio.h>
#include <string.h>

#if defined(CONFIG_ESP_MQTT_TLS_ENABLE)
#include "esp_tls_lwmqtt.h"
#endif

#include "esp_lwmqtt.h"
#include "esp_mqtt.h"

#define ESP_MQTT_LOG_TAG "esp_mqtt"

#define ESP_MQTT_LOCK(mutex) \
  do {                       \
  } while (xSemaphoreTake(mutex, portMAX_DELAY) != pdPASS)

#define ESP_MQTT_UNLOCK(mutex) xSemaphoreGive(mutex)

static struct {
  char *topic;
  char *payload;
  int qos;
  bool retained;
} esp_mqtt_lwt_config = {.topic = NULL, .payload = NULL, .qos = 0, .retained = false};

static lwmqtt_client_t esp_mqtt_client;

#if !(defined(CONFIG_ESP_MQTT_TLS_ENABLE) && defined(CONFIG_ESP_MQTT_TLS_ONLY))
static esp_lwmqtt_network_t esp_mqtt_network = {0};
#endif

#if defined(CONFIG_ESP_MQTT_TLS_ENABLE)
static esp_tls_lwmqtt_network_t esp_tls_mqtt_network = {.cacert_buf = NULL, .enable = false };
#endif

static esp_lwmqtt_timer_t esp_mqtt_timer1, esp_mqtt_timer2;

static void *esp_mqtt_write_buffer;
static void *esp_mqtt_read_buffer;

static QueueHandle_t esp_mqtt_event_queue = NULL;

typedef struct {
  lwmqtt_string_t topic;
  lwmqtt_message_t message;
} esp_mqtt_event_t;

#if defined(CONFIG_ESP_MQTT_TLS_ENABLE)
bool esp_mqtt_tls(bool verify, const unsigned char * cacert, size_t cacert_len) {
  // acquire mutex
  ESP_MQTT_LOCK_MAIN();

  if (!cacert) {
      ESP_LOGE(ESP_MQTT_LOG_TAG, "esp_mqtt_tls: cacert must be not NULL.");
      ESP_MQTT_UNLOCK_MAIN();
      return false;
  }
  esp_tls_mqtt_network.verify = verify;
  esp_tls_mqtt_network.cacert_buf = cacert;
  esp_tls_mqtt_network.cacert_len = cacert_len;
  esp_tls_mqtt_network.enable = true;

  // release mutex
  ESP_MQTT_UNLOCK_MAIN();
  return true;
}
#endif

esp_mqtt_config_t *esp_mqtt_init(esp_mqtt_status_callback_t scb, esp_mqtt_message_callback_t mcb, size_t buffer_size,
                   	   	   	   	 int command_timeout)
{
  esp_mqtt_settings_t *settings = calloc(1, sizeof(esp_mqtt_settings_t));
  if (!settings)
  {
	  ESP_LOGE(ESP_MQTT_LOG_TAG, "esp_mqtt_init:%s", "Can't allocate memory");
	  return(NULL);
  }
  // set callbacks
  settings->esp_mqtt_status_callback = scb;
  settings->esp_mqtt_message_callback = mcb;
  settings->esp_mqtt_buffer_size = buffer_size;
  settings->esp_mqtt_command_timeout = (uint32_t)command_timeout;

  // allocate buffers
  settings->esp_mqtt_write_buffer = calloc((size_t)buffer_size, sizeof(char));
  settings->esp_mqtt_read_buffer = calloc((size_t)buffer_size, sizeof(char));

  // create mutexes
  settings->esp_mqtt_main_mutex = xSemaphoreCreateMutex();
  settings->esp_mqtt_select_mutex = xSemaphoreCreateMutex();

  // create queue
  settings->esp_mqtt_event_queue = xQueueCreate(CONFIG_ESP_MQTT_EVENT_QUEUE_SIZE, sizeof(esp_mqtt_event_t *));

  if (!settings->esp_mqtt_write_buffer || !settings->esp_mqtt_read_buffer
	  || !settings->esp_mqtt_main_mutex || !settings->esp_mqtt_select_mutex
	  || !settings->esp_mqtt_event_queue)
  {
	  esp_mqtt_delete(settings);
	  ESP_LOGE(ESP_MQTT_LOG_TAG, "esp_mqtt_init:%s", "Can't allocate memory");
	  return(NULL);
  }
  else
  {
	  return(settings);
  }
}

static void esp_mqtt_message_handler(lwmqtt_client_t *client, void *ref, lwmqtt_string_t topic, lwmqtt_message_t msg) {
  // create message
  esp_mqtt_event_t *evt = malloc(sizeof(esp_mqtt_event_t));

  // copy topic with additional null termination
  evt->topic.len = topic.len;
  evt->topic.data = malloc((size_t)topic.len + 1);
  memcpy(evt->topic.data, topic.data, (size_t)topic.len);
  evt->topic.data[topic.len] = 0;

  // copy message with additional null termination
  evt->message.retained = msg.retained;
  evt->message.qos = msg.qos;
  evt->message.payload_len = msg.payload_len;
  evt->message.payload = malloc((size_t)msg.payload_len + 1);
  memcpy(evt->message.payload, msg.payload, (size_t)msg.payload_len);
  evt->message.payload[msg.payload_len] = 0;

  // queue event
  if (xQueueSend(esp_mqtt_event_queue, &evt, 0) != pdTRUE) {
    ESP_LOGE(ESP_MQTT_LOG_TAG, "xQueueSend: queue is full, dropping message");
    free(evt->topic.data);
    free(evt->message.payload);
    free(evt);
  }
}

static void esp_mqtt_dispatch_events() {
  // prepare event
  esp_mqtt_event_t *evt = NULL;

  // receive next event
  while (xQueueReceive(esp_mqtt_event_queue, &evt, 0) == pdTRUE) {
    // call callback if existing
    if (esp_mqtt_message_callback) {
      esp_mqtt_message_callback(evt->topic.data, evt->message.payload, evt->message.payload_len);
    }

    // free data
    free(evt->topic.data);
    free(evt->message.payload);
    free(evt);
  }
}

static bool esp_mqtt_process_connect() {
  // initialize the client
  lwmqtt_init(&esp_mqtt_client, esp_mqtt_write_buffer, esp_mqtt_buffer_size, esp_mqtt_read_buffer,
              esp_mqtt_buffer_size);

  #if (defined(CONFIG_ESP_MQTT_TLS_ENABLE) && defined(CONFIG_ESP_MQTT_TLS_ONLY))
    lwmqtt_set_network(&esp_mqtt_client, &esp_tls_mqtt_network, esp_tls_lwmqtt_network_read, esp_tls_lwmqtt_network_write);
  #elif defined(CONFIG_ESP_MQTT_TLS_ENABLE)
    if (esp_tls_mqtt_network.enable){
      lwmqtt_set_network(&esp_mqtt_client, &esp_tls_mqtt_network, esp_tls_lwmqtt_network_read, esp_tls_lwmqtt_network_write);
    } else {
      lwmqtt_set_network(&esp_mqtt_client, &esp_mqtt_network, esp_lwmqtt_network_read, esp_lwmqtt_network_write);
    }
  #else
    lwmqtt_set_network(&esp_mqtt_client, &esp_mqtt_network, esp_lwmqtt_network_read, esp_lwmqtt_network_write);
  #endif

  lwmqtt_set_timers(&esp_mqtt_client, &esp_mqtt_timer1, &esp_mqtt_timer2, esp_lwmqtt_timer_set, esp_lwmqtt_timer_get);
  lwmqtt_set_callback(&esp_mqtt_client, NULL, esp_mqtt_message_handler);

  // initiate network connection
  lwmqtt_err_t err;
  #if (defined(CONFIG_ESP_MQTT_TLS_ENABLE) && defined(CONFIG_ESP_MQTT_TLS_ONLY))
    err = esp_tls_lwmqtt_network_connect(&esp_tls_mqtt_network, esp_mqtt_config.host, esp_mqtt_config.port);
  #elif defined(CONFIG_ESP_MQTT_TLS_ENABLE)
    if (esp_tls_mqtt_network.enable) {
      err = esp_tls_lwmqtt_network_connect(&esp_tls_mqtt_network, esp_mqtt_config.host, esp_mqtt_config.port);
    } else {
      err = esp_lwmqtt_network_connect(&esp_mqtt_network, esp_mqtt_config.host, esp_mqtt_config.port);
    }
  #else
    err = esp_lwmqtt_network_connect(&esp_mqtt_network, esp_mqtt_config.host, esp_mqtt_config.port);
  #endif

  if (err != LWMQTT_SUCCESS) {
    ESP_LOGE(ESP_MQTT_LOG_TAG, "esp_lwmqtt_network_connect: %d", err);
    return false;
  }

  // release mutex
  ESP_MQTT_UNLOCK_MAIN();

  // acquire select mutex
  ESP_MQTT_LOCK_SELECT();

  // wait for connection
  bool connected = false;

  #if (defined(CONFIG_ESP_MQTT_TLS_ENABLE) && defined(CONFIG_ESP_MQTT_TLS_ONLY))
    err = esp_tls_lwmqtt_network_wait(&esp_tls_mqtt_network, &connected, esp_mqtt_command_timeout);
  #elif defined(CONFIG_ESP_MQTT_TLS_ENABLE)
    if (esp_tls_mqtt_network.enable) {
      err = esp_tls_lwmqtt_network_wait(&esp_tls_mqtt_network, &connected, esp_mqtt_command_timeout);
    } else {
      err = esp_lwmqtt_network_wait(&esp_mqtt_network, &connected, esp_mqtt_command_timeout);
    }
  #else
    err = esp_lwmqtt_network_wait(&esp_mqtt_network, &connected, esp_mqtt_command_timeout);
  #endif

  if (err != LWMQTT_SUCCESS) {
    ESP_LOGE(ESP_MQTT_LOG_TAG, "esp_lwmqtt_network_wait: %d", err);
    return false;
  }

  // release select mutex
  ESP_MQTT_UNLOCK_SELECT();

  // acquire mutex
  ESP_MQTT_LOCK_MAIN();

  // return if not connected
  if (!connected) {
    return false;
  }

  // setup connect data
  lwmqtt_options_t options = lwmqtt_default_options;
  options.keep_alive = 10;
  options.client_id = lwmqtt_string(esp_mqtt_config.client_id);
  options.username = lwmqtt_string(esp_mqtt_config.username);
  options.password = lwmqtt_string(esp_mqtt_config.password);

  // last will data
  lwmqtt_will_t will;
  will.topic = lwmqtt_string(esp_mqtt_lwt_config.topic);
  will.qos = (lwmqtt_qos_t)esp_mqtt_lwt_config.qos;
  will.retained = esp_mqtt_lwt_config.retained;
  will.payload = lwmqtt_string(esp_mqtt_lwt_config.payload);

  // attempt connection
  lwmqtt_return_code_t return_code;
  err = lwmqtt_connect(&esp_mqtt_client, options,
						 will.topic.len ? &will : NULL, &return_code,
						 esp_mqtt_command_timeout);
  if (err != LWMQTT_SUCCESS) {
    ESP_LOGE(ESP_MQTT_LOG_TAG, "lwmqtt_connect: %d", err);
    return false;
  }

  return true;
}

static void esp_mqtt_process(void *p) {
  // connection loop
  for (;;) {
    // log attempt
    ESP_LOGI(ESP_MQTT_LOG_TAG, "esp_mqtt_process: begin connection attempt");

    // acquire mutex
    ESP_MQTT_LOCK_MAIN();

    // make connection attempt
    if (esp_mqtt_process_connect()) {
      // log success
      ESP_LOGI(ESP_MQTT_LOG_TAG, "esp_mqtt_process: connection attempt successful");

      // set local flag
      esp_mqtt_connected = true;

      // release mutex
      ESP_MQTT_UNLOCK_MAIN();

      // exit loop
      break;
    }

    // release mutex
    ESP_MQTT_UNLOCK_MAIN();

    // log fail
    ESP_LOGW(ESP_MQTT_LOG_TAG, "esp_mqtt_process: connection attempt failed");

    // delay loop by 1s and yield to other processes
    vTaskDelay(1000 / portTICK_PERIOD_MS);
  }

  // call callback if existing
  if (esp_mqtt_status_callback) {
    esp_mqtt_status_callback(ESP_MQTT_STATUS_CONNECTED);
  }

  // yield loop
  for (;;) {
    // check for error
    if (esp_mqtt_error) {
      break;
    }

    // acquire select mutex
    ESP_MQTT_LOCK_SELECT();

    // block until data is available
    bool available = false;

    lwmqtt_err_t err;
    #if (defined(CONFIG_ESP_MQTT_TLS_ENABLE) && defined(CONFIG_ESP_MQTT_TLS_ONLY))
      err = esp_tls_lwmqtt_network_select(&esp_tls_mqtt_network, &available, esp_mqtt_command_timeout);
    #elif defined(CONFIG_ESP_MQTT_TLS_ENABLE)
      if (esp_tls_mqtt_network.enable) {
        err = esp_tls_lwmqtt_network_select(&esp_tls_mqtt_network, &available, esp_mqtt_command_timeout);
      } else {
        err = esp_lwmqtt_network_select(&esp_mqtt_network, &available, esp_mqtt_command_timeout);
      }
    #else
      err = esp_lwmqtt_network_select(&esp_mqtt_network, &available, esp_mqtt_command_timeout);
    #endif

    if (err != LWMQTT_SUCCESS) {
      ESP_LOGE(ESP_MQTT_LOG_TAG, "esp_lwmqtt_network_select: %d", err);
      ESP_MQTT_UNLOCK_SELECT();
      break;
    }

    // release select mutex
    ESP_MQTT_UNLOCK_SELECT();

    // acquire mutex
    ESP_MQTT_LOCK_MAIN();

    // process data if available
    if (available) {
      // get available bytes
      size_t available_bytes = 0;

      #if (defined(CONFIG_ESP_MQTT_TLS_ENABLE) && defined(CONFIG_ESP_MQTT_TLS_ONLY))
        err = esp_tls_lwmqtt_network_peek(&esp_tls_mqtt_network, &available_bytes, esp_mqtt_command_timeout);
      #elif defined(CONFIG_ESP_MQTT_TLS_ENABLE)
        if (esp_tls_mqtt_network.enable) {
            err = esp_tls_lwmqtt_network_peek(&esp_tls_mqtt_network, &available_bytes, esp_mqtt_command_timeout);
        } else {
            err = esp_lwmqtt_network_peek(&esp_mqtt_network, &available_bytes);
        }
      #else
        err = esp_lwmqtt_network_peek(&esp_mqtt_network, &available_bytes);
      #endif

      if (err != LWMQTT_SUCCESS) {
        ESP_LOGE(ESP_MQTT_LOG_TAG, "esp_lwmqtt_network_peek: %d", err);
        ESP_MQTT_UNLOCK_MAIN();
        break;
      }

      // yield client only if there is still data to read since select might unblock because of incoming ack packets
      // that are already handled until we get to this point
      if (available_bytes > 0) {
        err = lwmqtt_yield(&esp_mqtt_client, available_bytes, esp_mqtt_command_timeout);
        if (err != LWMQTT_SUCCESS) {
          ESP_LOGE(ESP_MQTT_LOG_TAG, "lwmqtt_yield: %d", err);
          ESP_MQTT_UNLOCK_MAIN();
          break;
        }
      }
    }

    // do mqtt background work
    err = lwmqtt_keep_alive(&esp_mqtt_client, esp_mqtt_command_timeout);
    if (err != LWMQTT_SUCCESS) {
      ESP_LOGE(ESP_MQTT_LOG_TAG, "lwmqtt_keep_alive: %d", err);
      ESP_MQTT_UNLOCK_MAIN();
      break;
    }

    // release mutex
    ESP_MQTT_UNLOCK_MAIN();

    // dispatch queued events
    esp_mqtt_dispatch_events();
  }

  // acquire mutex
  ESP_MQTT_LOCK_MAIN();

  // disconnect network
  #if (defined(CONFIG_ESP_MQTT_TLS_ENABLE) && defined(CONFIG_ESP_MQTT_TLS_ONLY))
    esp_tls_lwmqtt_network_disconnect(&esp_tls_mqtt_network);
  #elif defined(CONFIG_ESP_MQTT_TLS_ENABLE)
    if (esp_tls_mqtt_network.enable) {
      esp_tls_lwmqtt_network_disconnect(&esp_tls_mqtt_network);
    } else {
      esp_lwmqtt_network_disconnect(&esp_mqtt_network);
    }
  #else
    esp_lwmqtt_network_disconnect(&esp_mqtt_network);
  #endif

  // set local flags
  esp_mqtt_connected = false;
  esp_mqtt_running = false;
  esp_mqtt_error = false;

  // release mutex
  ESP_MQTT_UNLOCK_MAIN();

  ESP_LOGI(ESP_MQTT_LOG_TAG, "esp_mqtt_process: exit task");

  // call callback if existing
  if (esp_mqtt_status_callback) {
    esp_mqtt_status_callback(ESP_MQTT_STATUS_DISCONNECTED);
  }

  // delete task
  vTaskDelete(NULL);
}

void esp_mqtt_lwt(const char *topic, const char *payload, int qos, bool retained) {
  // acquire mutex
  ESP_MQTT_LOCK_MAIN();

  // free topic if set
  if (esp_mqtt_lwt_config.topic != NULL) {
    free(esp_mqtt_lwt_config.topic);
    esp_mqtt_lwt_config.topic = NULL;
  }

  // free payload if set
  if (esp_mqtt_lwt_config.payload != NULL) {
    free(esp_mqtt_lwt_config.payload);
    esp_mqtt_lwt_config.payload = NULL;
  }

  // set topic if provided
  if (topic != NULL) {
    esp_mqtt_lwt_config.topic = strdup(topic);
  }

  // set payload if provided
  if (payload != NULL) {
    esp_mqtt_lwt_config.payload = strdup(payload);
  }

  // set qos
  esp_mqtt_lwt_config.qos = qos;

  // set retained
  esp_mqtt_lwt_config.retained = retained;

  // release mutex
  ESP_MQTT_UNLOCK_MAIN();
}

esp_err_t esp_mqtt_start(const char *host, const char *port, const char *client_id, const char *username,
                    const char *password, esp_mqtt_settings_t *settings)
{
  bool err_memory = false;
  // acquire mutex
  ESP_MQTT_LOCK(settings->esp_mqtt_main_mutex);

  #if (defined(CONFIG_ESP_MQTT_TLS_ONLY))
    if (esp_tls_mqtt_network.enable == false) {
      ESP_LOGE(ESP_MQTT_LOG_TAG, "esp_mqtt_start: Call esp_mqtt_tls() before!");
      ESP_MQTT_UNLOCK_MAIN();
      return ESP_FAIL;
    }
  #endif
  // check if already running
  if (settings->esp_mqtt_running) {
    ESP_LOGW(ESP_MQTT_LOG_TAG, "esp_mqtt_start: already running");
    ESP_MQTT_UNLOCK(settings->esp_mqtt_main_mutex);
    return ESP_OK;
  }

  esp_mqtt_clear_cfg(&settings->esp_mqtt_cfg);

  if (host == NULL || port == NULL)
  {
	 ESP_LOGE(ESP_MQTT_LOG_TAG, "esp_mqtt_start: host or port = NULL");
	 return ESP_ERR_INVALID_ARG;
  }
  else
  {
	  // set host
	  settings->esp_mqtt_cfg.host = strdup(host);
	  (!settings->esp_mqtt_cfg.host) ? err_memory = true : 0;

	  //set port
	  settings->esp_mqtt_cfg.port = strdup(port);
	  (!settings->esp_mqtt_cfg.port) ? err_memory = true : 0;
  }

  // set client id if provided
  if (client_id != NULL)
  {
	settings->esp_mqtt_cfg.client_id = strdup(client_id);
    (!settings->esp_mqtt_cfg.client_id) ? err_memory = true : 0;
  }

  // set username if provided
  if (username != NULL)
  {
    settings->esp_mqtt_cfg.username = strdup(username);
    (!settings->esp_mqtt_cfg.username) ? err_memory = true : 0;
  }

  // set password if provided
  if (password != NULL)
  {
	  settings->esp_mqtt_cfg.password = strdup(password);
	  (!settings->esp_mqtt_cfg.password) ? err_memory = true : 0;
  }

  if (err_memory)
  {
	  esp_mqtt_clear_cfg(&settings->esp_mqtt_cfg);
	  ESP_LOGE(ESP_MQTT_LOG_TAG, "esp_mqtt_start: No memory for allocate esp_mqtt_cfg");
	  ESP_MQTT_UNLOCK(settings->esp_mqtt_main_mutex);
	  return ESP_ERR_NO_MEM;
  }

  // create mqtt thread
  ESP_LOGI(ESP_MQTT_LOG_TAG, "esp_mqtt_start: create task");
  xTaskCreatePinnedToCore(esp_mqtt_process, "esp_mqtt", CONFIG_ESP_MQTT_TASK_STACK_SIZE, NULL,
                          CONFIG_ESP_MQTT_TASK_STACK_PRIORITY, &settings->esp_mqtt_task, 1);

  if (!settings->esp_mqtt_task)
  {
	  esp_mqtt_clear_cfg(&settings->esp_mqtt_cfg);
	  ESP_LOGE(ESP_MQTT_LOG_TAG, "esp_mqtt_start: No memory for allocate esp_mqtt task");
	  ESP_MQTT_UNLOCK(settings->esp_mqtt_main_mutex);
	  return ESP_ERR_NO_MEM;
  }
  // set local flag
  settings->esp_mqtt_running = true;

  // release mutex
  ESP_MQTT_UNLOCK(settings->esp_mqtt_main_mutex);
  return ESP_OK;
}

bool esp_mqtt_subscribe(const char *topic, int qos) {
  // acquire mutex
  ESP_MQTT_LOCK_MAIN();

  // check if still connected
  if (!esp_mqtt_connected) {
    ESP_LOGW(ESP_MQTT_LOG_TAG, "esp_mqtt_subscribe: not connected");
    ESP_MQTT_UNLOCK_MAIN();
    return false;
  }

  // subscribe to topic
  lwmqtt_err_t err =
      lwmqtt_subscribe_one(&esp_mqtt_client, lwmqtt_string(topic), (lwmqtt_qos_t)qos, esp_mqtt_command_timeout);
  if (err != LWMQTT_SUCCESS) {
    esp_mqtt_error = true;
    ESP_LOGE(ESP_MQTT_LOG_TAG, "lwmqtt_subscribe_one: %d", err);
    ESP_MQTT_UNLOCK_MAIN();
    return false;
  }

  // release mutex
  ESP_MQTT_UNLOCK_MAIN();

  return true;
}

bool esp_mqtt_unsubscribe(const char *topic) {
  // acquire mutex
  ESP_MQTT_LOCK_MAIN();

  // check if still connected
  if (!esp_mqtt_connected) {
    ESP_LOGW(ESP_MQTT_LOG_TAG, "esp_mqtt_unsubscribe: not connected");
    ESP_MQTT_UNLOCK_MAIN();
    return false;
  }

  // unsubscribe from topic
  lwmqtt_err_t err = lwmqtt_unsubscribe_one(&esp_mqtt_client, lwmqtt_string(topic), esp_mqtt_command_timeout);
  if (err != LWMQTT_SUCCESS) {
    esp_mqtt_error = true;
    ESP_LOGE(ESP_MQTT_LOG_TAG, "lwmqtt_unsubscribe_one: %d", err);
    ESP_MQTT_UNLOCK_MAIN();
    return false;
  }

  // release mutex
  ESP_MQTT_UNLOCK_MAIN();

  return true;
}

bool esp_mqtt_publish(const char *topic, uint8_t *payload, size_t len, int qos, bool retained) {
  // acquire mutex
  ESP_MQTT_LOCK_MAIN();

  // check if still connected
  if (!esp_mqtt_connected) {
    ESP_LOGW(ESP_MQTT_LOG_TAG, "esp_mqtt_publish: not connected");
    ESP_MQTT_UNLOCK_MAIN();
    return false;
  }

  // prepare message
  lwmqtt_message_t message;
  message.qos = (lwmqtt_qos_t)qos;
  message.retained = retained;
  message.payload = payload;
  message.payload_len = len;

  // publish message
  lwmqtt_err_t err = lwmqtt_publish(&esp_mqtt_client, lwmqtt_string(topic), message, esp_mqtt_command_timeout);
  if (err != LWMQTT_SUCCESS) {
    esp_mqtt_error = true;
    ESP_LOGE(ESP_MQTT_LOG_TAG, "lwmqtt_publish: %d", err);
    ESP_MQTT_UNLOCK_MAIN();
    return false;
  }

  // release mutex
  ESP_MQTT_UNLOCK_MAIN();

  return true;
}

void esp_mqtt_stop() {
  // acquire mutexes
  ESP_MQTT_LOCK_MAIN();
  ESP_MQTT_LOCK_SELECT();

  // return immediately if not running anymore
  if (!esp_mqtt_running) {
    ESP_MQTT_UNLOCK_SELECT();
    ESP_MQTT_UNLOCK_MAIN();
    return;
  }

  // attempt to properly disconnect a connected client
  if (esp_mqtt_connected) {
    lwmqtt_err_t err = lwmqtt_disconnect(&esp_mqtt_client, esp_mqtt_command_timeout);
    if (err != LWMQTT_SUCCESS) {
      ESP_LOGE(ESP_MQTT_LOG_TAG, "lwmqtt_disconnect: %d", err);
    }

    // set flag
    esp_mqtt_connected = false;
  }

  // disconnect network
  #if (defined(CONFIG_ESP_MQTT_TLS_ENABLE) && defined(CONFIG_ESP_MQTT_TLS_ONLY))
    esp_tls_lwmqtt_network_disconnect(&esp_tls_mqtt_network);
  #elif defined(CONFIG_ESP_MQTT_TLS_ENABLE)
    if (esp_tls_mqtt_network.enable) {
      esp_tls_lwmqtt_network_disconnect(&esp_tls_mqtt_network);
    } else {
      esp_lwmqtt_network_disconnect(&esp_mqtt_network);
    }
  #else
    esp_lwmqtt_network_disconnect(&esp_mqtt_network);
  #endif

  // kill mqtt task
  ESP_LOGI(ESP_MQTT_LOG_TAG, "esp_mqtt_stop: deleting task");
  vTaskDelete(esp_mqtt_task);

  // set flag
  esp_mqtt_running = false;

  // release mutexes
  ESP_MQTT_UNLOCK_SELECT();
  ESP_MQTT_UNLOCK_MAIN();
}

void esp_mqtt_delete(esp_mqtt_settings_t *settings)
{
	if (settings)
	{
		if (settings->esp_mqtt_main_mutex)
		{
			ESP_MQTT_LOCK(settings->esp_mqtt_main_mutex);
		}
		if (settings->esp_mqtt_select_mutex)
		{
			ESP_MQTT_LOCK(settings->esp_mqtt_select_mutex);
		}

		if (settings->esp_mqtt_task)
		{
			//TODO delete all in task if need
			{

			}
			vTaskDelete(settings->esp_mqtt_task);
		}
		(settings->esp_mqtt_event_queue) ? vQueueDelete(settings->esp_mqtt_event_queue) : 0;
		if (settings->esp_mqtt_read_buffer)
		{
			memset(settings->esp_mqtt_read_buffer, 0, settings->esp_mqtt_buffer_size);
			free(settings->esp_mqtt_read_buffer);
		}
		if (settings->esp_mqtt_write_buffer)
		{
			memset(settings->esp_mqtt_write_buffer, 0, settings->esp_mqtt_buffer_size);
			free(settings->esp_mqtt_write_buffer);
		}

		if (settings->esp_mqtt_select_mutex)
		{
			ESP_MQTT_UNLOCK(settings->esp_mqtt_select_mutex);
		}
		if (settings->esp_mqtt_main_mutex)
		{
			ESP_MQTT_UNLOCK(settings->esp_mqtt_main_mutex);
		}

		vSemaphoreDelete(settings->esp_mqtt_main_mutex);
		vSemaphoreDelete(settings->esp_mqtt_select_mutex);

		esp_mqtt_clear_cfg(&settings->esp_mqtt_cfg);

		memset(settings, 0, sizeof(esp_mqtt_settings_t));
		free(settings);
	}
}

void esp_mqtt_clear_cfg(esp_mqtt_config_t *cfg)
{
	if (cfg)
	{
		if (cfg->host)
		{
			memset(cfg->host, 0, strlen(cfg->host));
			free(cfg->host);
			cfg->host = NULL;
		}
		if (cfg->port)
		{
			memset(cfg->port, 0, strlen(cfg->port));
			free(cfg->port);
			cfg->port = NULL;
		}
		if (cfg->client_id)
		{
			memset(cfg->client_id, 0, strlen(cfg->client_id));
			free(cfg->client_id);
			cfg->client_id = NULL;
		}
		if (cfg->username)
		{
			memset(cfg->username, 0, strlen(cfg->username));
			free(cfg->username);
			cfg->username = NULL;
		}
		if (cfg->password)
		{
			memset(cfg->password, 0, strlen(cfg->password));
			free(cfg->password);
			cfg->password = NULL;
		}
	}
}
