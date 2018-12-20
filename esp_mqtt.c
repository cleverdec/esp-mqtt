#include "esp_mqtt.h"

#define ESP_MQTT_LOG_TAG "esp_mqtt"

#define ESP_MQTT_LOCK(mutex) \
  do {                       \
  } while (xSemaphoreTake(mutex, portMAX_DELAY) != pdPASS)

#define ESP_MQTT_UNLOCK(mutex) xSemaphoreGive(mutex)

typedef struct
{
    lwmqtt_string_t topic;
    lwmqtt_message_t message;
} esp_mqtt_event_t;

#if defined(CONFIG_ESP_MQTT_TLS_ENABLE)
bool esp_mqtt_tls(esp_mqtt_settings_t *settings, bool verify, const unsigned char * cacert, size_t cacert_len)
{
    // acquire mutex
    ESP_MQTT_LOCK(settings->esp_mqtt_main_mutex);

    if (!cacert)
    {
        ESP_LOGE(ESP_MQTT_LOG_TAG, "esp_mqtt_tls: cacert must be not NULL.");
        ESP_MQTT_UNLOCK(settings->esp_mqtt_main_mutex);
        return false;
    }
    settings->esp_tls_mqtt_network.verify = verify;
    settings->esp_tls_mqtt_network.cacert_buf = cacert;
    settings->esp_tls_mqtt_network.cacert_len = cacert_len;
    settings->esp_tls_mqtt_network.enable = true;

    // release mutex
    ESP_MQTT_UNLOCK(settings->esp_mqtt_main_mutex);
    return true;
}
#endif

esp_mqtt_settings_t *esp_mqtt_init(esp_mqtt_status_callback_t scb, esp_mqtt_message_callback_t mcb,
                                 size_t buffer_size, int command_timeout)
{
    esp_mqtt_settings_t *settings = calloc(1, sizeof(esp_mqtt_settings_t));
    if (!settings)
    {
        ESP_LOGE(ESP_MQTT_LOG_TAG, "esp_mqtt_init: Can't allocate memory");
        return (NULL);
    }
    // set callbacks
    settings->esp_mqtt_status_callback = scb;
    settings->esp_mqtt_message_callback = mcb;
    settings->esp_mqtt_buffer_size = buffer_size;
    settings->esp_mqtt_command_timeout = (uint32_t) command_timeout;

    // allocate buffers
    settings->esp_mqtt_write_buffer = calloc((size_t) buffer_size, sizeof(char));
    settings->esp_mqtt_read_buffer = calloc((size_t) buffer_size, sizeof(char));

    // create mutexes
    settings->esp_mqtt_main_mutex = xSemaphoreCreateMutex();
    settings->esp_mqtt_select_mutex = xSemaphoreCreateMutex();

    // create queue
    settings->esp_mqtt_event_queue = xQueueCreate(CONFIG_ESP_MQTT_EVENT_QUEUE_SIZE,
            sizeof(esp_mqtt_event_t *));

    if (!settings->esp_mqtt_write_buffer || !settings->esp_mqtt_read_buffer
            || !settings->esp_mqtt_main_mutex || !settings->esp_mqtt_select_mutex
            || !settings->esp_mqtt_event_queue)
    {
        esp_mqtt_delete(settings);
        ESP_LOGE(ESP_MQTT_LOG_TAG, "esp_mqtt_init: Can't allocate memory");
        return (NULL);
    }
    else
    {
        return (settings);
    }
}

static void esp_mqtt_message_handler(lwmqtt_client_t *client, void *ref, lwmqtt_string_t topic,
                                     lwmqtt_message_t msg)
{
    QueueHandle_t esp_mqtt_event_queue = (QueueHandle_t)ref;

    // create message
    esp_mqtt_event_t *evt = calloc(1, sizeof(esp_mqtt_event_t));

    // copy topic with additional null termination
    evt->topic.len = topic.len;
    evt->topic.data = malloc((size_t) topic.len + 1);
    memcpy(evt->topic.data, topic.data, (size_t) topic.len);
    evt->topic.data[topic.len] = 0;

    // copy message with additional null termination
    evt->message.retained = msg.retained;
    evt->message.qos = msg.qos;
    evt->message.payload_len = msg.payload_len;
    evt->message.payload = malloc((size_t) msg.payload_len + 1);
    memcpy(evt->message.payload, msg.payload, (size_t) msg.payload_len);
    evt->message.payload[msg.payload_len] = 0;

    // queue event
    if (xQueueSend(esp_mqtt_event_queue, &evt, 0) != pdTRUE)
    {
        ESP_LOGE(ESP_MQTT_LOG_TAG, "xQueueSend: queue is full, dropping message");
        free(evt->topic.data);
        free(evt->message.payload);
        free(evt);
    }
}

static void esp_mqtt_dispatch_events(esp_mqtt_settings_t *settings)
{
    // prepare event
    esp_mqtt_event_t *evt = NULL;

    // receive next event
    while (xQueueReceive(settings->esp_mqtt_event_queue, &evt, 0) == pdTRUE)
    {
        // call callback if existing
        if (settings->esp_mqtt_message_callback)
        {
            settings->esp_mqtt_message_callback(evt->topic.data, evt->message.payload,
                    evt->message.payload_len);
        }

        // free data
        free(evt->topic.data);
        free(evt->message.payload);
        free(evt);
    }
}

static bool esp_mqtt_process_connect(esp_mqtt_settings_t *settings)
{
    // initialize the client
    lwmqtt_init(&settings->esp_mqtt_client, settings->esp_mqtt_write_buffer,
            settings->esp_mqtt_buffer_size, settings->esp_mqtt_read_buffer,
            settings->esp_mqtt_buffer_size);

#if (defined(CONFIG_ESP_MQTT_TLS_ENABLE) && defined(CONFIG_ESP_MQTT_TLS_ONLY))
    {
        lwmqtt_set_network(&settings->esp_mqtt_client, &settings->esp_tls_mqtt_network,
                esp_tls_lwmqtt_network_read, esp_tls_lwmqtt_network_write);
    }
#elif defined(CONFIG_ESP_MQTT_TLS_ENABLE)
    {
        if (settings->esp_tls_mqtt_network.enable)
        {
            lwmqtt_set_network(&settings->esp_mqtt_client, &settings->esp_tls_mqtt_network,
                    esp_tls_lwmqtt_network_read, esp_tls_lwmqtt_network_write);
        }
        else
        {
            lwmqtt_set_network(&settings->esp_mqtt_client, &settings->esp_mqtt_network,
                    esp_lwmqtt_network_read, esp_lwmqtt_network_write);
        }
    }
#else
    {
        lwmqtt_set_network(&settings->esp_mqtt_client, &settings->esp_mqtt_network,
                esp_lwmqtt_network_read, esp_lwmqtt_network_write);
    }
#endif

    lwmqtt_set_timers(&settings->esp_mqtt_client, &settings->esp_mqtt_timer1,
            &settings->esp_mqtt_timer2, esp_lwmqtt_timer_set, esp_lwmqtt_timer_get);
    lwmqtt_set_callback(&settings->esp_mqtt_client, settings->esp_mqtt_event_queue, esp_mqtt_message_handler);

    // initiate network connection
    lwmqtt_err_t err;
#if (defined(CONFIG_ESP_MQTT_TLS_ENABLE) && defined(CONFIG_ESP_MQTT_TLS_ONLY))
    {
        err = esp_tls_lwmqtt_network_connect(&settings->esp_tls_mqtt_network, settings->esp_mqtt_cfg.host, settings->esp_mqtt_cfg.port);
    }
#elif defined(CONFIG_ESP_MQTT_TLS_ENABLE)
    {
        if (settings->esp_tls_mqtt_network.enable)
        {
            err = esp_tls_lwmqtt_network_connect(&settings->esp_tls_mqtt_network, settings->esp_mqtt_cfg.host, settings->esp_mqtt_cfg.port);
        }
        else
        {
            err = esp_lwmqtt_network_connect(&settings->esp_mqtt_network, settings->esp_mqtt_cfg.host, settings->esp_mqtt_cfg.port);
        }
    }
#else
    {
        err = esp_lwmqtt_network_connect(&settings->esp_mqtt_network, settings->esp_mqtt_cfg.host,
                settings->esp_mqtt_cfg.port);
    }
#endif

    if (err != LWMQTT_SUCCESS)
    {
        ESP_LOGE(ESP_MQTT_LOG_TAG, "esp_lwmqtt_network_connect: host: %s:%s : %d",
                settings->esp_mqtt_cfg.host, settings->esp_mqtt_cfg.port, err);
        return false;
    }

    // release mutex
    ESP_MQTT_UNLOCK(settings->esp_mqtt_main_mutex);

    // acquire select mutex
    ESP_MQTT_LOCK(settings->esp_mqtt_select_mutex);

    // wait for connection
    bool connected = false;

#if (defined(CONFIG_ESP_MQTT_TLS_ENABLE) && defined(CONFIG_ESP_MQTT_TLS_ONLY))
    {
        err = esp_tls_lwmqtt_network_wait(&settings->esp_tls_mqtt_network,
                &connected, settings->esp_mqtt_command_timeout);
    }
#elif defined(CONFIG_ESP_MQTT_TLS_ENABLE)
    {
        if (settings->esp_tls_mqtt_network.enable)
        {
            err = esp_tls_lwmqtt_network_wait(&settings->esp_tls_mqtt_network,
                    &connected, settings->esp_mqtt_command_timeout);
        }
        else
        {
            err = esp_lwmqtt_network_wait(&settings->esp_mqtt_network,
                    &connected, settings->esp_mqtt_command_timeout);
        }
    }
#else
    {
        err = esp_lwmqtt_network_wait(&settings->esp_mqtt_network,
                &connected, settings->esp_mqtt_command_timeout);
    }
#endif

    if (err != LWMQTT_SUCCESS)
    {
        ESP_LOGE(ESP_MQTT_LOG_TAG, "esp_lwmqtt_network_wait: host: %s:%s : %d",
                settings->esp_mqtt_cfg.host, settings->esp_mqtt_cfg.port, err);
        return false;
    }

    // release select mutex
    ESP_MQTT_UNLOCK(settings->esp_mqtt_select_mutex);

    // acquire mutex
    ESP_MQTT_LOCK(settings->esp_mqtt_main_mutex);

    // return if not connected
    if (!connected)
    {
        return false;
    }

    // setup connect data
    lwmqtt_options_t options = lwmqtt_default_options;
    options.keep_alive = 10;
    options.client_id = lwmqtt_string(settings->esp_mqtt_cfg.client_id);
    options.username = lwmqtt_string(settings->esp_mqtt_cfg.username);
    options.password = lwmqtt_string(settings->esp_mqtt_cfg.password);

    // last will data
    lwmqtt_will_t will;
    will.topic = lwmqtt_string(settings->esp_mqtt_lwt_config.topic);
    will.qos = (lwmqtt_qos_t) settings->esp_mqtt_lwt_config.qos;
    will.retained = settings->esp_mqtt_lwt_config.retained;
    will.payload = lwmqtt_string(settings->esp_mqtt_lwt_config.payload);

    // attempt connection
    lwmqtt_return_code_t return_code;
    err = lwmqtt_connect(&settings->esp_mqtt_client, options, will.topic.len ? &will : NULL, &return_code,
            settings->esp_mqtt_command_timeout);
    if (err != LWMQTT_SUCCESS)
    {
        ESP_LOGE(ESP_MQTT_LOG_TAG, "lwmqtt_connect: %s:%s : %d",
                settings->esp_mqtt_cfg.host, settings->esp_mqtt_cfg.port, err);
        return false;
    }

    return true;
}

static void esp_mqtt_process(void *pvParameters)
{

    esp_mqtt_settings_t *settings;
    // connection loop
    if (!pvParameters)
    {
        vTaskDelete(NULL);
        return;
    }
    else
    {
        settings = (esp_mqtt_settings_t *) pvParameters;
    }
    for (;;)
    {
        // log attempt
        ESP_LOGI(ESP_MQTT_LOG_TAG, "esp_mqtt_process: begin connection to %s:%s attempt",
                settings->esp_mqtt_cfg.host, settings->esp_mqtt_cfg.port);

        // acquire mutex
        ESP_MQTT_LOCK(settings->esp_mqtt_main_mutex);

        // make connection attempt
        if (esp_mqtt_process_connect(settings))
        {
            // log success
            ESP_LOGI(ESP_MQTT_LOG_TAG, "esp_mqtt_process: connection to %s:%s attempt successful",
                    settings->esp_mqtt_cfg.host, settings->esp_mqtt_cfg.port);

            // set local flag
            settings->esp_mqtt_connected = true;

            // release mutex
            ESP_MQTT_UNLOCK(settings->esp_mqtt_main_mutex);

            // exit loop
            break;
        }

        // release mutex
        ESP_MQTT_UNLOCK(settings->esp_mqtt_main_mutex);

        // log fail
        ESP_LOGW(ESP_MQTT_LOG_TAG, "esp_mqtt_process: connection to %s:%s attempt failed",
                settings->esp_mqtt_cfg.host, settings->esp_mqtt_cfg.port);

        // delay loop by 1s and yield to other processes
        vTaskDelay(1000 / portTICK_PERIOD_MS);
    }

    // call callback if existing
    if (settings->esp_mqtt_status_callback)
    {
        settings->esp_mqtt_status_callback(settings, ESP_MQTT_STATUS_CONNECTED);
    }

    // yield loop
    for (;;)
    {
        // check for error
        if (settings->esp_mqtt_error)
        {
            break;
        }

        // acquire select mutex
        ESP_MQTT_LOCK(settings->esp_mqtt_select_mutex);

        // block until data is available
        bool available = false;

        lwmqtt_err_t err;
#if (defined(CONFIG_ESP_MQTT_TLS_ENABLE) && defined(CONFIG_ESP_MQTT_TLS_ONLY))
        {
            err = esp_tls_lwmqtt_network_select(&settings->esp_tls_mqtt_network, &available, settings->esp_mqtt_command_timeout);
        }
#elif defined(CONFIG_ESP_MQTT_TLS_ENABLE)
        {
            if (settings->esp_tls_mqtt_network.enable)
            {
                err = esp_tls_lwmqtt_network_select(&settings->esp_tls_mqtt_network, &available, settings->esp_mqtt_command_timeout);
            }
            else
            {
                err = esp_lwmqtt_network_select(&settings->esp_mqtt_network, &available, settings->esp_mqtt_command_timeout);
            }
        }
#else
        {
            err = esp_lwmqtt_network_select(&settings->esp_mqtt_network, &available, settings->esp_mqtt_command_timeout);
        }
#endif

        if (err != LWMQTT_SUCCESS)
        {
            ESP_LOGE(ESP_MQTT_LOG_TAG, "esp_lwmqtt_network_select: host: %s:%s : %d",
                    settings->esp_mqtt_cfg.host, settings->esp_mqtt_cfg.port, err);
            ESP_MQTT_UNLOCK(settings->esp_mqtt_select_mutex);
            break;
        }

        // release select mutex
        ESP_MQTT_UNLOCK(settings->esp_mqtt_select_mutex);

        // acquire mutex
        ESP_MQTT_UNLOCK(settings->esp_mqtt_main_mutex);

        // process data if available
        if (available)
        {
            // get available bytes
            size_t available_bytes = 0;

#if (defined(CONFIG_ESP_MQTT_TLS_ENABLE) && defined(CONFIG_ESP_MQTT_TLS_ONLY))
            {
                err = esp_tls_lwmqtt_network_peek(&settings->esp_tls_mqtt_network,
                        &available_bytes, settings->esp_mqtt_command_timeout);
            }
#elif defined(CONFIG_ESP_MQTT_TLS_ENABLE)
            {
                if (settings->esp_tls_mqtt_network.enable)
                {
                    err = esp_tls_lwmqtt_network_peek(&settings->esp_tls_mqtt_network,
                            &available_bytes, settings->esp_mqtt_command_timeout);
                }
                else
                {
                    err = esp_lwmqtt_network_peek(&settings->esp_mqtt_network, &available_bytes);
                }
            }
#else
            {
                err = esp_lwmqtt_network_peek(&settings->esp_mqtt_network, &available_bytes);
            }
#endif

            if (err != LWMQTT_SUCCESS)
            {
                ESP_LOGE(ESP_MQTT_LOG_TAG, "esp_lwmqtt_network_peek: host: %s:%s : %d",
                        settings->esp_mqtt_cfg.host, settings->esp_mqtt_cfg.port, err);
                ESP_MQTT_UNLOCK(settings->esp_mqtt_main_mutex);
                break;
            }

            // yield client only if there is still data to read since select might unblock because of incoming ack packets
            // that are already handled until we get to this point
            if (available_bytes > 0)
            {
                err = lwmqtt_yield(&settings->esp_mqtt_client, available_bytes, settings->esp_mqtt_command_timeout);
                if (err != LWMQTT_SUCCESS)
                {
                    ESP_LOGE(ESP_MQTT_LOG_TAG, "lwmqtt_yield: host: %s:%s : %d",
                            settings->esp_mqtt_cfg.host, settings->esp_mqtt_cfg.port, err);
                    ESP_MQTT_UNLOCK(settings->esp_mqtt_main_mutex);
                    break;
                }
            }
        }

        // do mqtt background work
        err = lwmqtt_keep_alive(&settings->esp_mqtt_client, settings->esp_mqtt_command_timeout);
        if (err != LWMQTT_SUCCESS)
        {
            ESP_LOGE(ESP_MQTT_LOG_TAG, "lwmqtt_keep_alive: host: %s:%s : %d",
                    settings->esp_mqtt_cfg.host, settings->esp_mqtt_cfg.port, err);
            ESP_MQTT_UNLOCK(settings->esp_mqtt_main_mutex);
            break;
        }

        // release mutex
        ESP_MQTT_UNLOCK(settings->esp_mqtt_main_mutex);

        // dispatch queued events
        esp_mqtt_dispatch_events(settings);
    }

    // acquire mutex
    ESP_MQTT_LOCK(settings->esp_mqtt_main_mutex);

    // disconnect network
#if (defined(CONFIG_ESP_MQTT_TLS_ENABLE) && defined(CONFIG_ESP_MQTT_TLS_ONLY))
    {
        esp_tls_lwmqtt_network_disconnect(&esettings->sp_tls_mqtt_network);
    }
#elif defined(CONFIG_ESP_MQTT_TLS_ENABLE)
    {
        if (settings->esp_tls_mqtt_network.enable)
        {
            esp_tls_lwmqtt_network_disconnect(&settings->esp_tls_mqtt_network);
        }
        else
        {
            esp_lwmqtt_network_disconnect(&settings->esp_mqtt_network);
        }
    }
#else
    {
        esp_lwmqtt_network_disconnect(&settings->esp_mqtt_network);
    }
#endif

    // set local flags
    settings->esp_mqtt_connected = false;
    settings->esp_mqtt_running = false;
    settings->esp_mqtt_error = false;

    // release mutex
    ESP_MQTT_UNLOCK(settings->esp_mqtt_main_mutex);

    ESP_LOGI(ESP_MQTT_LOG_TAG, "esp_mqtt_process: host: %s:%s exit task",
            settings->esp_mqtt_cfg.host, settings->esp_mqtt_cfg.port);

    // call callback if existing
    if (settings->esp_mqtt_status_callback)
    {
        settings->esp_mqtt_status_callback(settings, ESP_MQTT_STATUS_DISCONNECTED);
    }

    // delete task
    vTaskDelete(NULL);
}

void esp_mqtt_lwt(const char *topic, const char *payload, int qos,
				  bool retained, esp_mqtt_settings_t *settings)
{
    // acquire mutex
    ESP_MQTT_LOCK(settings->esp_mqtt_main_mutex);

    // free topic if set
    if (settings->esp_mqtt_lwt_config.topic != NULL)
    {
        free(settings->esp_mqtt_lwt_config.topic);
        settings->esp_mqtt_lwt_config.topic = NULL;
    }

    // free payload if set
    if (settings->esp_mqtt_lwt_config.payload != NULL)
    {
        free(settings->esp_mqtt_lwt_config.payload);
        settings->esp_mqtt_lwt_config.payload = NULL;
    }

    // set topic if provided
    if (topic != NULL)
    {
    	settings->esp_mqtt_lwt_config.topic = strdup(topic);
    }

    // set payload if provided
    if (payload != NULL)
    {
        settings->esp_mqtt_lwt_config.payload = strdup(payload);
    }

    // set qos
    settings->esp_mqtt_lwt_config.qos = qos;

    // set retained
    settings->esp_mqtt_lwt_config.retained = retained;

    // release mutex
    ESP_MQTT_UNLOCK(settings->esp_mqtt_main_mutex);
}

esp_err_t esp_mqtt_start(const char *host, const char *port, const char *client_id,
                         const char *username, const char *password, esp_mqtt_settings_t *settings)
{
    bool err_memory = false;
    // acquire mutex
    ESP_LOGI("!!!!!", "settings = %p", settings);
    ESP_MQTT_LOCK(settings->esp_mqtt_main_mutex);

#if (defined(CONFIG_ESP_MQTT_TLS_ONLY))
    if (esp_tls_mqtt_network.enable == false)
    {
        ESP_LOGE(ESP_MQTT_LOG_TAG, "esp_mqtt_start: Call esp_mqtt_tls() before!");
        ESP_MQTT_UNLOCK_MAIN();
        return ESP_FAIL;
    }
#endif
    // check if already running
    if (settings->esp_mqtt_running)
    {
        ESP_LOGW(ESP_MQTT_LOG_TAG, "esp_mqtt_start: already running");
        ESP_MQTT_UNLOCK(settings->esp_mqtt_main_mutex);
        return ESP_OK;
    }

    if (host == NULL || port == NULL)
    {
        ESP_LOGE(ESP_MQTT_LOG_TAG, "esp_mqtt_start: host or port = NULL");
        return ESP_ERR_INVALID_ARG;
    }
    else
    {
        // set host
        if (!settings->esp_mqtt_cfg.host)
        {
            settings->esp_mqtt_cfg.host = strdup(host);
            (!settings->esp_mqtt_cfg.host) ? err_memory = true : 0;
        }

        //set port
        if (!settings->esp_mqtt_cfg.port)
        {
            settings->esp_mqtt_cfg.port = strdup(port);
            (!settings->esp_mqtt_cfg.port) ? err_memory = true : 0;
        }
    }

    // set client id if provided
    if (client_id != NULL && !settings->esp_mqtt_cfg.client_id)
    {
        settings->esp_mqtt_cfg.client_id = strdup(client_id);
        (!settings->esp_mqtt_cfg.client_id) ? err_memory = true : 0;
    }

    // set username if provided
    if (username != NULL && !settings->esp_mqtt_cfg.username)
    {
        settings->esp_mqtt_cfg.username = strdup(username);
        (!settings->esp_mqtt_cfg.username) ? err_memory = true : 0;
    }

    // set password if provided
    if (password != NULL && !settings->esp_mqtt_cfg.password)
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
    xTaskCreatePinnedToCore(esp_mqtt_process, "esp_mqtt", CONFIG_ESP_MQTT_TASK_STACK_SIZE, settings,
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

bool esp_mqtt_subscribe(esp_mqtt_settings_t *settings, const char *topic, int qos)
{
    // acquire mutex
    ESP_MQTT_LOCK(settings->esp_mqtt_main_mutex);

    // check if still connected
    if (!settings->esp_mqtt_connected)
    {
        ESP_LOGW(ESP_MQTT_LOG_TAG, "esp_mqtt_subscribe: not connected");
        ESP_MQTT_UNLOCK(settings->esp_mqtt_main_mutex);
        return false;
    }

    // subscribe to topic
    lwmqtt_err_t err = lwmqtt_subscribe_one(&settings->esp_mqtt_client, lwmqtt_string(topic),
            (lwmqtt_qos_t) qos, settings->esp_mqtt_command_timeout);
    if (err != LWMQTT_SUCCESS)
    {
        settings->esp_mqtt_error = true;
        ESP_LOGE(ESP_MQTT_LOG_TAG, "lwmqtt_subscribe_one: %d", err);
        ESP_MQTT_UNLOCK(settings->esp_mqtt_main_mutex);
        return false;
    }

    // release mutex
    ESP_MQTT_UNLOCK(settings->esp_mqtt_main_mutex);

    return true;
}

bool esp_mqtt_unsubscribe(esp_mqtt_settings_t *settings, const char *topic)
{
    // acquire mutex
    ESP_MQTT_LOCK(settings->esp_mqtt_main_mutex);

    // check if still connected
    if (!settings->esp_mqtt_connected)
    {
        ESP_LOGW(ESP_MQTT_LOG_TAG, "esp_mqtt_unsubscribe: not connected");
        ESP_MQTT_UNLOCK(settings->esp_mqtt_main_mutex);
        return false;
    }

    // unsubscribe from topic
    lwmqtt_err_t err = lwmqtt_unsubscribe_one(&settings->esp_mqtt_client, lwmqtt_string(topic),
            settings->esp_mqtt_command_timeout);
    if (err != LWMQTT_SUCCESS)
    {
        settings->esp_mqtt_error = true;
        ESP_LOGE(ESP_MQTT_LOG_TAG, "lwmqtt_unsubscribe_one: %d", err);
        ESP_MQTT_UNLOCK(settings->esp_mqtt_main_mutex);
        return false;
    }

    // release mutex
    ESP_MQTT_UNLOCK(settings->esp_mqtt_main_mutex);

    return true;
}

bool esp_mqtt_publish(esp_mqtt_settings_t *settings, const char *topic, uint8_t *payload, size_t len, int qos,
bool retained)
{
    // acquire mutex
    ESP_LOGI("!!esp_mqtt_publish!!!", "settings = %p", settings);
    ESP_MQTT_LOCK(settings->esp_mqtt_main_mutex);

    // check if still connected
    if (!settings->esp_mqtt_connected)
    {
        ESP_LOGW(ESP_MQTT_LOG_TAG, "esp_mqtt_publish: not connected");
        ESP_MQTT_UNLOCK(settings->esp_mqtt_main_mutex);
        return false;
    }

    // prepare message
    lwmqtt_message_t message;
    message.qos = (lwmqtt_qos_t) qos;
    message.retained = retained;
    message.payload = payload;
    message.payload_len = len;

    // publish message
    lwmqtt_err_t err = lwmqtt_publish(&settings->esp_mqtt_client, lwmqtt_string(topic), message,
            settings->esp_mqtt_command_timeout);
    if (err != LWMQTT_SUCCESS)
    {
        settings->esp_mqtt_error = true;
        ESP_LOGE(ESP_MQTT_LOG_TAG, "lwmqtt_publish: %d", err);
        ESP_MQTT_UNLOCK(settings->esp_mqtt_main_mutex);
        return false;
    }

    // release mutex
    ESP_MQTT_UNLOCK(settings->esp_mqtt_main_mutex);

    return true;
}

void esp_mqtt_stop(esp_mqtt_settings_t *settings)
{
    // acquire mutexes
    ESP_MQTT_LOCK(settings->esp_mqtt_main_mutex);
    ESP_MQTT_LOCK(settings->esp_mqtt_select_mutex);

    // return immediately if not running anymore
    if (!settings->esp_mqtt_running)
    {
        ESP_MQTT_UNLOCK(settings->esp_mqtt_select_mutex);
        ESP_MQTT_UNLOCK(settings->esp_mqtt_main_mutex);
        return;
    }

    // attempt to properly disconnect a connected client
    if (settings->esp_mqtt_connected)
    {
        lwmqtt_err_t err = lwmqtt_disconnect(&settings->esp_mqtt_client, settings->esp_mqtt_command_timeout);
        if (err != LWMQTT_SUCCESS)
        {
            ESP_LOGE(ESP_MQTT_LOG_TAG, "lwmqtt_disconnect: %d", err);
        }

        // set flag
        settings->esp_mqtt_connected = false;
    }

    // disconnect network
#if (defined(CONFIG_ESP_MQTT_TLS_ENABLE) && defined(CONFIG_ESP_MQTT_TLS_ONLY))
    {
    esp_tls_lwmqtt_network_disconnect(&settings->esp_tls_mqtt_network);
    }
#elif defined(CONFIG_ESP_MQTT_TLS_ENABLE)
    {
    if (settings->esp_tls_mqtt_network.enable)
    {
        esp_tls_lwmqtt_network_disconnect(&settings->esp_tls_mqtt_network);
    }
    else
    {
        esp_lwmqtt_network_disconnect(&settings->esp_mqtt_network);
    }
    }
#else
    {
    esp_lwmqtt_network_disconnect(&settings->esp_mqtt_network);
    }
#endif

    // kill mqtt task
    ESP_LOGI(ESP_MQTT_LOG_TAG, "esp_mqtt_stop: deleting task");
    vTaskDelete(settings->esp_mqtt_task);

    // set flag
    settings->esp_mqtt_running = false;

    // release mutexes
    ESP_MQTT_UNLOCK(settings->esp_mqtt_select_mutex);
    ESP_MQTT_UNLOCK(settings->esp_mqtt_main_mutex);
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
            ESP_MQTT_UNLOCK(settings->esp_mqtt_select_mutex);
            ESP_MQTT_UNLOCK(settings->esp_mqtt_main_mutex);
            esp_mqtt_stop(settings);
            ESP_MQTT_LOCK(settings->esp_mqtt_main_mutex);
            ESP_MQTT_UNLOCK(settings->esp_mqtt_select_mutex);
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

#if !(defined(CONFIG_ESP_MQTT_TLS_ENABLE) && defined(CONFIG_ESP_MQTT_TLS_ONLY))
        {
        	memset(&settings->esp_mqtt_network, 0, sizeof(esp_lwmqtt_network_t));
        }
#endif
#if defined(CONFIG_ESP_MQTT_TLS_ENABLE)
        {
        	memset(&settings->esp_tls_mqtt_network, 0, sizeof(esp_tls_lwmqtt_network_t));
        }
#endif
        esp_mqtt_lwt_clear_cfg(&settings->esp_mqtt_lwt_config);
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

void esp_mqtt_lwt_clear_cfg(esp_mqtt_lwt_config_t *cfg)
{
    if (cfg->topic)
    {
        memset(cfg->topic, 0, strlen(cfg->topic));
        free(cfg->topic);
        cfg->topic = NULL;
    }
    if (cfg->payload)
    {
        memset(cfg->payload, 0, strlen(cfg->payload));
        free(cfg->payload);
        cfg->payload = NULL;
    }
    memset(cfg, 0, sizeof(esp_mqtt_lwt_config_t));
}
