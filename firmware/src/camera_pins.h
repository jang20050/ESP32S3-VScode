#pragma once

// XIAO ESP32S3 Sense 카메라 커넥터(OV2640 / OV3660) 핀 배치
// 출처: Seeed Studio XIAO ESP32S3 Sense 회로도

// 전원 차단과 리셋 핀은 보드에 연결되어 있지 않다.
#define PWDN_GPIO_NUM   -1
#define RESET_GPIO_NUM  -1

// 카메라 마스터 클럭
#define XCLK_GPIO_NUM   10

// SCCB(I2C 호환) 제어 버스
#define SIOD_GPIO_NUM   40
#define SIOC_GPIO_NUM   39

// 8비트 병렬 데이터 버스 (Y9 가 최상위 비트)
#define Y9_GPIO_NUM     48
#define Y8_GPIO_NUM     11
#define Y7_GPIO_NUM     12
#define Y6_GPIO_NUM     14
#define Y5_GPIO_NUM     16
#define Y4_GPIO_NUM     18
#define Y3_GPIO_NUM     17
#define Y2_GPIO_NUM     15

// 동기 신호
#define VSYNC_GPIO_NUM  38
#define HREF_GPIO_NUM   47
#define PCLK_GPIO_NUM   13
