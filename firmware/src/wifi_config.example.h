#pragma once

// 이 파일을 같은 폴더에 wifi_config.h 라는 이름으로 복사한 뒤 값을 채운다.
// wifi_config.h 는 .gitignore 에 들어 있어 Wi-Fi 비밀번호가 저장소에 올라가지 않는다.
// 주의: ESP32-S3 는 2.4GHz Wi-Fi 만 지원한다. 5GHz 전용 SSID 에는 접속할 수 없다.

// Wi-Fi 접속 정보
#define WIFI_SSID      "YOUR_SSID"
#define WIFI_PASSWORD  "YOUR_PASSWORD"

// 접속 대기 시간(ms). 이 시간 안에 연결되지 않으면 실패로 보고 loop 에서 다시 시도한다.
#define WIFI_CONNECT_TIMEOUT_MS  15000

// 끊김을 확인하는 주기(ms)
#define WIFI_CHECK_INTERVAL_MS   2000

// 카메라 해상도 (PSRAM 이 있을 때 사용)
// FRAMESIZE_QVGA(320x240), FRAMESIZE_VGA(640x480), FRAMESIZE_SVGA(800x600) 등
// YOLO 입력이 640 이므로 VGA 가 속도와 정확도의 균형이 좋다.
#define CAMERA_FRAME_SIZE  FRAMESIZE_VGA

// JPEG 품질 (0~63, 숫자가 작을수록 고화질이지만 프레임이 커진다)
#define CAMERA_JPEG_QUALITY  12

// 시리얼 통계 출력 주기(ms)
#define STATS_INTERVAL_MS  5000
