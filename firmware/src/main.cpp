/*
 * XIAO ESP32S3 Sense MJPEG 스트리밍 펌웨어
 *
 * 구성
 *   포트 80 : "/"        안내 텍스트와 링크
 *             "/stream"  MJPEG 스트림 (파이썬은 http://<IP>/stream 하나만 쓴다)
 *   포트 81 : "/status"  상태 JSON
 *             "/capture" JPEG 1장
 *
 * 동시 시청 제한
 *   esp_http_server 는 서버 하나당 작업(task) 하나로 요청을 순서대로 처리한다.
 *   /stream 핸들러는 연결이 끊길 때까지 반환하지 않으므로, 포트 80 은 그동안
 *   다른 요청을 처리하지 못한다. 따라서 /stream 은 동시에 1명만 볼 수 있다.
 *   Flask 서버가 스트림을 받고 있는 동안에는 브라우저로 /stream 이나 "/" 를 열지 말고,
 *   상태 확인은 포트 81 의 /status 와 /capture 를 사용한다.
 *
 * 스트림을 81 로 분리하지 않은 이유
 *   파이썬 설정의 URL 을 http://<IP>/stream 하나로 유지하기 위해서다.
 *   제어용 엔드포인트만 81 로 옮겨, 스트림이 포트 80 을 점유해도 상태를 볼 수 있게 했다.
 */

#include <Arduino.h>
#include <WiFi.h>
#include "esp_camera.h"
#include "esp_http_server.h"
#include "esp_timer.h"
#include "img_converters.h"

#include "camera_pins.h"
#if __has_include("wifi_config.h")
#include "wifi_config.h"
#else
#error "src/wifi_config.example.h 를 src/wifi_config.h 로 복사하고 SSID 와 비밀번호를 입력하세요."
#endif

// ---------------------------------------------------------------------------
// MJPEG 형식 상수
// ---------------------------------------------------------------------------
#define PART_BOUNDARY "xiaoesp32s3frame"
static const char *STREAM_CONTENT_TYPE = "multipart/x-mixed-replace;boundary=" PART_BOUNDARY;
static const char *STREAM_BOUNDARY = "\r\n--" PART_BOUNDARY "\r\n";
static const char *STREAM_PART = "Content-Type: image/jpeg\r\nContent-Length: %u\r\nX-Timestamp: %ld.%06ld\r\n\r\n";

// JPEG 가 아닌 프레임을 변환할 때 쓰는 품질 (frame2jpg 는 0~100, 클수록 고화질)
#define CONVERT_JPEG_QUALITY 80

// ---------------------------------------------------------------------------
// 전역 상태
// ---------------------------------------------------------------------------
static httpd_handle_t g_streamHttpd = NULL;   // 포트 80
static httpd_handle_t g_controlHttpd = NULL;  // 포트 81

static bool g_hasPsram = false;
static framesize_t g_frameSize = CAMERA_FRAME_SIZE;

// 스트림 통계. httpd 작업과 loop 가 함께 접근하므로 스핀락으로 보호한다.
static portMUX_TYPE g_statsMux = portMUX_INITIALIZER_UNLOCKED;
static uint32_t g_windowFrames = 0;    // 현재 집계 구간에서 보낸 프레임 수
static uint64_t g_windowBytes = 0;     // 현재 집계 구간에서 보낸 JPEG 바이트 수
static uint32_t g_totalFrames = 0;     // 부팅 후 누적 프레임 수
static int g_streamClients = 0;        // 현재 /stream 시청자 수
static float g_streamFps = 0.0f;       // 직전 구간의 평균 FPS
static float g_avgKbPerFrame = 0.0f;   // 직전 구간의 평균 KB/frame

static unsigned long g_lastStatsMs = 0;
static unsigned long g_lastWifiCheckMs = 0;

// ---------------------------------------------------------------------------
// 보조 함수
// ---------------------------------------------------------------------------

/// <summary>프레임 크기 열거값을 사람이 읽을 수 있는 이름으로 바꾼다.</summary>
/// <param name="size">esp32-camera 의 framesize_t 값</param>
/// <returns>"VGA" 같은 해상도 이름 문자열</returns>
/// <reason>/status JSON 과 시리얼 출력에서 숫자 대신 해상도 이름을 보여 주기 위해</reason>
static const char *frameSizeName(framesize_t size)
{
    switch (size)
    {
        case FRAMESIZE_QQVGA:
        {
            return "QQVGA";
        }
        case FRAMESIZE_QVGA:
        {
            return "QVGA";
        }
        case FRAMESIZE_CIF:
        {
            return "CIF";
        }
        case FRAMESIZE_HVGA:
        {
            return "HVGA";
        }
        case FRAMESIZE_VGA:
        {
            return "VGA";
        }
        case FRAMESIZE_SVGA:
        {
            return "SVGA";
        }
        case FRAMESIZE_XGA:
        {
            return "XGA";
        }
        case FRAMESIZE_HD:
        {
            return "HD";
        }
        case FRAMESIZE_SXGA:
        {
            return "SXGA";
        }
        case FRAMESIZE_UXGA:
        {
            return "UXGA";
        }
        default:
        {
            return "OTHER";
        }
    }
}

/// <summary>전송에 성공한 프레임 1장을 통계에 더한다.</summary>
/// <param name="bytes">보낸 JPEG 데이터 크기(바이트)</param>
/// <returns>없음</returns>
/// <reason>FPS 와 평균 프레임 크기를 /status 와 시리얼에 보여 주기 위해</reason>
static void recordFrame(size_t bytes)
{
    portENTER_CRITICAL(&g_statsMux);
    g_windowFrames++;
    g_windowBytes += bytes;
    g_totalFrames++;
    portEXIT_CRITICAL(&g_statsMux);
}

/// <summary>스트림 시청자 수를 늘리거나 줄인다.</summary>
/// <param name="delta">+1 은 접속, -1 은 종료</param>
/// <returns>없음</returns>
/// <reason>현재 스트림을 받는 클라이언트가 있는지 확인하기 위해</reason>
static void changeClients(int delta)
{
    portENTER_CRITICAL(&g_statsMux);
    g_streamClients += delta;
    if (g_streamClients < 0)
    {
        g_streamClients = 0;
    }
    portEXIT_CRITICAL(&g_statsMux);
}

/// <summary>집계 구간을 마감해 FPS 와 KB/frame 을 계산하고 시리얼에 출력한다.</summary>
/// <param name="elapsedMs">직전 마감 이후 지난 시간(ms)</param>
/// <returns>없음</returns>
/// <reason>5초마다 스트림 성능을 확인해 해상도와 JPEG 품질을 조정하는 근거로 쓰기 위해</reason>
static void reportStats(unsigned long elapsedMs)
{
    uint32_t frames;
    uint64_t bytes;
    int clients;

    portENTER_CRITICAL(&g_statsMux);
    frames = g_windowFrames;
    bytes = g_windowBytes;
    clients = g_streamClients;
    g_windowFrames = 0;
    g_windowBytes = 0;
    portEXIT_CRITICAL(&g_statsMux);

    float fps = 0.0f;
    float kbPerFrame = 0.0f;
    if (elapsedMs > 0)
    {
        fps = (frames * 1000.0f) / (float)elapsedMs;
    }
    if (frames > 0)
    {
        kbPerFrame = ((float)bytes / (float)frames) / 1024.0f;
    }

    portENTER_CRITICAL(&g_statsMux);
    g_streamFps = fps;
    g_avgKbPerFrame = kbPerFrame;
    portEXIT_CRITICAL(&g_statsMux);

    Serial.printf("[STAT] stream %.1f fps | %.1f KB/frame | clients %d | RSSI %d dBm\n",
                  fps, kbPerFrame, clients, (int)WiFi.RSSI());
}

// ---------------------------------------------------------------------------
// 카메라
// ---------------------------------------------------------------------------

/// <summary>카메라를 JPEG 모드로 초기화한다. PSRAM 이 없으면 저사양 설정으로 낮춘다.</summary>
/// <returns>esp_camera_init 의 결과 코드 (ESP_OK 면 성공)</returns>
/// <reason>프레임 버퍼 2장을 PSRAM 에 두어 촬영과 전송을 겹쳐 FPS 를 높이기 위해</reason>
static esp_err_t initCamera()
{
    // 구조체를 0 으로 채우지 않으면 jpeg_buffer_size 같은 필드에 쓰레기 값이 남아
    // 버퍼가 엉뚱한 크기로 잡히고 모든 프레임이 버려진다(cam_hal: FB-OVF).
    camera_config_t config;
    memset(&config, 0, sizeof(config));
    config.ledc_channel = LEDC_CHANNEL_0;
    config.ledc_timer = LEDC_TIMER_0;
    config.pin_d0 = Y2_GPIO_NUM;
    config.pin_d1 = Y3_GPIO_NUM;
    config.pin_d2 = Y4_GPIO_NUM;
    config.pin_d3 = Y5_GPIO_NUM;
    config.pin_d4 = Y6_GPIO_NUM;
    config.pin_d5 = Y7_GPIO_NUM;
    config.pin_d6 = Y8_GPIO_NUM;
    config.pin_d7 = Y9_GPIO_NUM;
    config.pin_xclk = XCLK_GPIO_NUM;
    config.pin_pclk = PCLK_GPIO_NUM;
    config.pin_vsync = VSYNC_GPIO_NUM;
    config.pin_href = HREF_GPIO_NUM;
    config.pin_sccb_sda = SIOD_GPIO_NUM;
    config.pin_sccb_scl = SIOC_GPIO_NUM;
    config.pin_pwdn = PWDN_GPIO_NUM;
    config.pin_reset = RESET_GPIO_NUM;
    config.xclk_freq_hz = 20000000;
    config.pixel_format = PIXFORMAT_JPEG;
    config.jpeg_quality = CAMERA_JPEG_QUALITY;
    config.sccb_i2c_port = -1;

    g_hasPsram = psramFound();
    if (g_hasPsram)
    {
        // 2장을 번갈아 쓰며 항상 가장 최근 프레임을 가져온다.
        config.frame_size = CAMERA_FRAME_SIZE;
        // 기본값(가로 x 세로 / 5, VGA 면 약 60KB)은 OV3660 의 고화질 JPEG 에 빠듯하다.
        // PSRAM 이 넉넉하므로 장당 256KB 를 잡아 넘침을 막는다.
        config.jpeg_buffer_size = 256 * 1024;
        config.fb_location = CAMERA_FB_IN_PSRAM;
        config.fb_count = 2;
        config.grab_mode = CAMERA_GRAB_LATEST;
    }
    else
    {
        // 내부 DRAM 은 약 320KB 뿐이라 작은 해상도와 버퍼 1장만 가능하다.
        // 버퍼가 1장이면 LATEST 모드가 의미가 없어 WHEN_EMPTY 로 둔다.
        config.frame_size = FRAMESIZE_QVGA;
        config.fb_location = CAMERA_FB_IN_DRAM;
        config.fb_count = 1;
        config.grab_mode = CAMERA_GRAB_WHEN_EMPTY;
        Serial.println("[CAM] PSRAM 없음 -> QVGA, DRAM, 버퍼 1장으로 동작");
    }
    g_frameSize = config.frame_size;

    esp_err_t err = esp_camera_init(&config);
    if (err != ESP_OK)
    {
        return err;
    }

    sensor_t *sensor = esp_camera_sensor_get();
    if (sensor != NULL)
    {
        Serial.printf("[CAM] sensor PID 0x%02X\n", sensor->id.PID);
        if (sensor->id.PID == OV3660_PID)
        {
            // OV3660 은 기본 영상이 뒤집혀 있고 어둡게 나온다.
            sensor->set_vflip(sensor, 1);
            sensor->set_brightness(sensor, 1);
            sensor->set_saturation(sensor, -2);
        }
    }
    return ESP_OK;
}

// ---------------------------------------------------------------------------
// Wi-Fi
// ---------------------------------------------------------------------------

/// <summary>공유기에 접속하고 제한 시간까지 기다린다.</summary>
/// <param name="timeoutMs">최대 대기 시간(ms)</param>
/// <returns>제한 시간 안에 연결되면 true</returns>
/// <reason>접속이 안 될 때 무한히 멈추지 않고 loop 에서 다시 시도하게 하기 위해</reason>
static bool connectWiFi(unsigned long timeoutMs)
{
    WiFi.mode(WIFI_STA);
    // 절전 모드는 패킷 지연을 크게 늘려 스트림이 끊기므로 끈다.
    WiFi.setSleep(false);
    WiFi.begin(WIFI_SSID, WIFI_PASSWORD);

    Serial.printf("[WIFI] '%s' 접속 중", WIFI_SSID);
    unsigned long start = millis();
    while (WiFi.status() != WL_CONNECTED)
    {
        if (millis() - start >= timeoutMs)
        {
            Serial.println();
            Serial.println("[WIFI] 접속 시간 초과");
            return false;
        }
        delay(250);
        Serial.print(".");
    }
    Serial.println();
    Serial.printf("[WIFI] 연결됨  IP %s  RSSI %d dBm\n",
                  WiFi.localIP().toString().c_str(), (int)WiFi.RSSI());
    return true;
}

/// <summary>연결이 끊겼는지 확인하고 끊겼으면 다시 접속한다.</summary>
/// <returns>이번 호출에서 끊김을 발견하고 재접속에 성공했으면 true</returns>
/// <reason>공유기 재부팅이나 신호 약화 뒤에도 보드를 다시 켜지 않고 스트림을 이어 가기 위해</reason>
static bool checkWiFi()
{
    if (WiFi.status() == WL_CONNECTED)
    {
        return false;
    }
    Serial.println("[WIFI] 연결 끊김 -> 재접속 시도");
    WiFi.disconnect();
    delay(100);
    return connectWiFi(WIFI_CONNECT_TIMEOUT_MS);
}

// ---------------------------------------------------------------------------
// HTTP 핸들러 (포트 80)
// ---------------------------------------------------------------------------

/// <summary>"/" 요청에 안내 문구와 링크만 담은 HTML 을 보낸다.</summary>
/// <param name="req">httpd 요청 객체</param>
/// <returns>전송 결과 코드</returns>
/// <reason>영상을 페이지에 넣으면 포트 80 을 점유해 Flask 가 스트림을 받지 못하므로 링크만 둔다</reason>
static esp_err_t indexHandler(httpd_req_t *req)
{
    String ip = WiFi.localIP().toString();
    String html;
    html.reserve(1024);
    html += "<!doctype html><html><head><meta charset=\"utf-8\">";
    html += "<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">";
    html += "<title>XIAO ESP32S3 Camera</title>";
    html += "<style>body{font-family:sans-serif;color:#222;background:#fafafa;max-width:560px;margin:40px auto;padding:0 16px;line-height:1.6}";
    html += "a{color:#2f6f6a}code{background:#eee;padding:2px 4px}hr{border:0;border-top:1px solid #ddd}</style>";
    html += "</head><body><h2>XIAO ESP32S3 Sense</h2><hr>";
    html += "<p>Flask 서버 설정에 아래 주소를 넣으세요.</p>";
    html += "<p><code>http://" + ip + "/stream</code></p>";
    html += "<p>스트림은 동시에 1명만 볼 수 있습니다. Flask 가 받는 중이면 브라우저로 열지 마세요.</p><hr>";
    html += "<ul>";
    html += "<li><a href=\"/stream\">/stream</a> MJPEG 스트림</li>";
    html += "<li><a href=\"http://" + ip + ":81/status\">:81/status</a> 상태 JSON</li>";
    html += "<li><a href=\"http://" + ip + ":81/capture\">:81/capture</a> 사진 1장</li>";
    html += "</ul></body></html>";

    httpd_resp_set_type(req, "text/html; charset=utf-8");
    return httpd_resp_send(req, html.c_str(), html.length());
}

/// <summary>연결이 끊길 때까지 카메라 프레임을 MJPEG 로 계속 보낸다.</summary>
/// <param name="req">httpd 요청 객체</param>
/// <returns>종료 사유 코드 (클라이언트가 끊으면 전송 오류 코드)</returns>
/// <reason>Flask 의 OpenCV 가 HTTP 주소 하나로 연속 영상을 받을 수 있게 하기 위해</reason>
static esp_err_t streamHandler(httpd_req_t *req)
{
    esp_err_t res = httpd_resp_set_type(req, STREAM_CONTENT_TYPE);
    if (res != ESP_OK)
    {
        return res;
    }
    httpd_resp_set_hdr(req, "Access-Control-Allow-Origin", "*");
    httpd_resp_set_hdr(req, "Cache-Control", "no-cache, no-store");

    changeClients(1);
    Serial.println("[HTTP] /stream 클라이언트 접속");

    char partHeader[128];
    while (true)
    {
        camera_fb_t *fb = esp_camera_fb_get();
        if (fb == NULL)
        {
            Serial.println("[HTTP] 프레임 획득 실패");
            res = ESP_FAIL;
            break;
        }

        long tsSec = (long)fb->timestamp.tv_sec;
        long tsUsec = (long)fb->timestamp.tv_usec;
        uint8_t *jpgBuf = NULL;
        size_t jpgLen = 0;
        bool converted = false;

        if (fb->format == PIXFORMAT_JPEG)
        {
            jpgBuf = fb->buf;
            jpgLen = fb->len;
        }
        else
        {
            converted = frame2jpg(fb, CONVERT_JPEG_QUALITY, &jpgBuf, &jpgLen);
            // 변환본은 별도 메모리에 있으므로 원본 버퍼는 바로 돌려준다.
            esp_camera_fb_return(fb);
            fb = NULL;
            if (!converted)
            {
                Serial.println("[HTTP] JPEG 변환 실패");
                res = ESP_FAIL;
                break;
            }
        }

        res = httpd_resp_send_chunk(req, STREAM_BOUNDARY, strlen(STREAM_BOUNDARY));
        if (res == ESP_OK)
        {
            int headerLen = snprintf(partHeader, sizeof(partHeader), STREAM_PART,
                                     (unsigned)jpgLen, tsSec, tsUsec);
            res = httpd_resp_send_chunk(req, partHeader, headerLen);
        }
        if (res == ESP_OK)
        {
            res = httpd_resp_send_chunk(req, (const char *)jpgBuf, jpgLen);
        }

        // 전송 성공 여부와 관계없이 버퍼는 반드시 반환한다.
        if (fb != NULL)
        {
            esp_camera_fb_return(fb);
            fb = NULL;
        }
        if (converted)
        {
            free(jpgBuf);
            jpgBuf = NULL;
        }

        if (res != ESP_OK)
        {
            break;
        }
        recordFrame(jpgLen);
    }

    changeClients(-1);
    Serial.println("[HTTP] /stream 클라이언트 종료");
    return res;
}

// ---------------------------------------------------------------------------
// HTTP 핸들러 (포트 81)
// ---------------------------------------------------------------------------

/// <summary>보드 상태를 JSON 으로 돌려준다.</summary>
/// <param name="req">httpd 요청 객체</param>
/// <returns>전송 결과 코드</returns>
/// <reason>스트림이 포트 80 을 점유한 동안에도 대시보드가 보드 상태를 확인할 수 있게 하기 위해</reason>
static esp_err_t statusHandler(httpd_req_t *req)
{
    float fps;
    float kbPerFrame;
    int clients;
    uint32_t totalFrames;

    portENTER_CRITICAL(&g_statsMux);
    fps = g_streamFps;
    kbPerFrame = g_avgKbPerFrame;
    clients = g_streamClients;
    totalFrames = g_totalFrames;
    portEXIT_CRITICAL(&g_statsMux);

    char json[384];
    int len = snprintf(json, sizeof(json),
                       "{\"ip\":\"%s\",\"rssi\":%d,\"uptime\":%lu,"
                       "\"psram\":%s,\"psram_free\":%u,\"heap_free\":%u,"
                       "\"frame_size\":\"%s\",\"fps\":%.1f,\"kb_per_frame\":%.1f,"
                       "\"clients\":%d,\"total_frames\":%lu}",
                       WiFi.localIP().toString().c_str(),
                       (int)WiFi.RSSI(),
                       (unsigned long)(millis() / 1000),
                       g_hasPsram ? "true" : "false",
                       (unsigned)ESP.getFreePsram(),
                       (unsigned)ESP.getFreeHeap(),
                       frameSizeName(g_frameSize),
                       fps,
                       kbPerFrame,
                       clients,
                       (unsigned long)totalFrames);

    httpd_resp_set_type(req, "application/json");
    httpd_resp_set_hdr(req, "Access-Control-Allow-Origin", "*");
    httpd_resp_set_hdr(req, "Cache-Control", "no-cache");
    return httpd_resp_send(req, json, len);
}

/// <summary>현재 카메라 프레임 1장을 JPEG 로 돌려준다.</summary>
/// <param name="req">httpd 요청 객체</param>
/// <returns>전송 결과 코드</returns>
/// <reason>스트림 없이 화각과 초점을 빠르게 확인하기 위해</reason>
static esp_err_t captureHandler(httpd_req_t *req)
{
    camera_fb_t *fb = esp_camera_fb_get();
    if (fb == NULL)
    {
        Serial.println("[HTTP] /capture 프레임 획득 실패");
        httpd_resp_send_500(req);
        return ESP_FAIL;
    }

    char timestamp[32];
    snprintf(timestamp, sizeof(timestamp), "%ld.%06ld",
             (long)fb->timestamp.tv_sec, (long)fb->timestamp.tv_usec);

    httpd_resp_set_type(req, "image/jpeg");
    httpd_resp_set_hdr(req, "Content-Disposition", "inline; filename=capture.jpg");
    httpd_resp_set_hdr(req, "Access-Control-Allow-Origin", "*");
    httpd_resp_set_hdr(req, "X-Timestamp", timestamp);

    esp_err_t res;
    if (fb->format == PIXFORMAT_JPEG)
    {
        res = httpd_resp_send(req, (const char *)fb->buf, fb->len);
        esp_camera_fb_return(fb);
    }
    else
    {
        uint8_t *jpgBuf = NULL;
        size_t jpgLen = 0;
        bool converted = frame2jpg(fb, CONVERT_JPEG_QUALITY, &jpgBuf, &jpgLen);
        esp_camera_fb_return(fb);
        if (!converted)
        {
            Serial.println("[HTTP] /capture JPEG 변환 실패");
            httpd_resp_send_500(req);
            return ESP_FAIL;
        }
        res = httpd_resp_send(req, (const char *)jpgBuf, jpgLen);
        free(jpgBuf);
    }
    return res;
}

// ---------------------------------------------------------------------------
// HTTP 서버 시작
// ---------------------------------------------------------------------------

/// <summary>URI 핸들러 하나를 서버에 등록한다.</summary>
/// <param name="server">대상 httpd 핸들</param>
/// <param name="uri">경로 문자열</param>
/// <param name="handler">요청을 처리할 함수</param>
/// <returns>없음</returns>
/// <reason>httpd_uri_t 구조체를 채우는 반복 코드를 줄이기 위해</reason>
static void registerUri(httpd_handle_t server, const char *uri, esp_err_t (*handler)(httpd_req_t *))
{
    httpd_uri_t entry;
    memset(&entry, 0, sizeof(entry));
    entry.uri = uri;
    entry.method = HTTP_GET;
    entry.handler = handler;
    entry.user_ctx = NULL;
    httpd_register_uri_handler(server, &entry);
}

/// <summary>포트 80(스트림)과 포트 81(제어) HTTP 서버를 시작한다.</summary>
/// <returns>두 서버가 모두 시작되면 true</returns>
/// <reason>스트림이 포트 80 을 점유해도 81 에서 상태와 사진을 받을 수 있게 서버를 둘로 나눈다</reason>
static bool startServers()
{
    bool ok = true;

    httpd_config_t streamConfig = HTTPD_DEFAULT_CONFIG();
    streamConfig.server_port = 80;
    // 두 서버가 같은 내부 제어 포트를 쓰면 두 번째 서버가 시작되지 않는다.
    streamConfig.ctrl_port = 32768;
    streamConfig.stack_size = 8192;
    // 클라이언트가 응답 없이 사라져도 소켓을 회수해 다음 접속을 받게 한다.
    streamConfig.lru_purge_enable = true;

    if (httpd_start(&g_streamHttpd, &streamConfig) == ESP_OK)
    {
        registerUri(g_streamHttpd, "/", indexHandler);
        registerUri(g_streamHttpd, "/stream", streamHandler);
        Serial.println("[HTTP] 포트 80 시작 (/, /stream)");
    }
    else
    {
        Serial.println("[HTTP] 포트 80 시작 실패");
        ok = false;
    }

    httpd_config_t controlConfig = HTTPD_DEFAULT_CONFIG();
    controlConfig.server_port = 81;
    controlConfig.ctrl_port = 32769;
    controlConfig.stack_size = 8192;
    controlConfig.lru_purge_enable = true;

    if (httpd_start(&g_controlHttpd, &controlConfig) == ESP_OK)
    {
        registerUri(g_controlHttpd, "/status", statusHandler);
        registerUri(g_controlHttpd, "/capture", captureHandler);
        Serial.println("[HTTP] 포트 81 시작 (/status, /capture)");
    }
    else
    {
        Serial.println("[HTTP] 포트 81 시작 실패");
        ok = false;
    }

    return ok;
}

/// <summary>접속에 필요한 주소와 동시 시청 제한을 시리얼에 안내한다.</summary>
/// <returns>없음</returns>
/// <reason>학생이 시리얼 모니터에서 바로 URL 을 복사해 Flask 설정에 붙여 넣게 하기 위해</reason>
static void printUrls()
{
    String ip = WiFi.localIP().toString();
    Serial.println();
    Serial.println("========================================");
    Serial.printf(" IP        : %s\n", ip.c_str());
    Serial.printf(" RSSI      : %d dBm\n", (int)WiFi.RSSI());
    Serial.printf(" 해상도    : %s  (JPEG 품질 %d)\n", frameSizeName(g_frameSize), CAMERA_JPEG_QUALITY);
    Serial.printf(" PSRAM     : %s\n", g_hasPsram ? "사용" : "없음");
    Serial.printf(" 스트림    : http://%s/stream\n", ip.c_str());
    Serial.printf(" 상태      : http://%s:81/status\n", ip.c_str());
    Serial.printf(" 사진      : http://%s:81/capture\n", ip.c_str());
    Serial.println(" 주의      : /stream 은 동시에 1명만 볼 수 있습니다.");
    Serial.println("             Flask 가 받는 중이면 브라우저로 열지 마세요.");
    Serial.println("========================================");
    Serial.println();
}

// ---------------------------------------------------------------------------
// Arduino 진입점
// ---------------------------------------------------------------------------

/// <summary>시리얼, 카메라, Wi-Fi, HTTP 서버를 순서대로 준비한다.</summary>
/// <returns>없음</returns>
/// <reason>카메라를 먼저 확인해야 Wi-Fi 연결 전에 하드웨어 문제를 빨리 발견할 수 있다</reason>
void setup()
{
    Serial.begin(115200);
    unsigned long waitStart = millis();
    while (!Serial && (millis() - waitStart < 3000))
    {
        delay(10);
    }
    Serial.println();
    Serial.println("[BOOT] XIAO ESP32S3 Sense MJPEG streamer");

    esp_err_t camErr = initCamera();
    if (camErr != ESP_OK)
    {
        Serial.printf("[CAM] 초기화 실패 0x%x (%s)\n", camErr, esp_err_to_name(camErr));
        Serial.println("[CAM] 카메라 커넥터와 PSRAM 설정을 확인하세요. 5초 후 재시작");
        delay(5000);
        ESP.restart();
    }
    Serial.printf("[CAM] 초기화 완료  %s, 버퍼 %d장\n", frameSizeName(g_frameSize), g_hasPsram ? 2 : 1);

    if (!connectWiFi(WIFI_CONNECT_TIMEOUT_MS))
    {
        Serial.println("[WIFI] SSID 와 비밀번호를 확인하세요. loop 에서 계속 재시도합니다.");
    }

    startServers();

    if (WiFi.status() == WL_CONNECTED)
    {
        printUrls();
    }

    g_lastStatsMs = millis();
    g_lastWifiCheckMs = millis();
}

/// <summary>Wi-Fi 끊김을 감시하고 5초마다 스트림 통계를 출력한다.</summary>
/// <returns>없음</returns>
/// <reason>영상 전송은 httpd 작업이 맡으므로 loop 는 감시와 보고만 담당한다</reason>
void loop()
{
    unsigned long now = millis();

    if (now - g_lastWifiCheckMs >= WIFI_CHECK_INTERVAL_MS)
    {
        g_lastWifiCheckMs = now;
        if (checkWiFi())
        {
            // DHCP 로 IP 가 바뀌었을 수 있으므로 주소를 다시 알려 준다.
            printUrls();
        }
    }

    now = millis();
    if (now - g_lastStatsMs >= STATS_INTERVAL_MS)
    {
        reportStats(now - g_lastStatsMs);
        g_lastStatsMs = now;
    }

    delay(10);
}
