"""
서버 설정 모음.

모든 값은 같은 이름의 환경변수가 있으면 그 값을 먼저 쓴다.
예) PowerShell:  $env:CAMERA_URL = "http://192.168.45.60/stream"
    bash:        export CAMERA_URL=http://192.168.45.60/stream
"""

import logging
import os
from pathlib import Path

_logger = logging.getLogger("config")

# 이 파일이 있는 server 폴더
BASE_DIR = Path(__file__).resolve().parent


def _env_str(name, default):
    """
    <summary>환경변수에서 문자열 설정을 읽는다.</summary>
    <param name="name">환경변수 이름</param>
    <param name="default">환경변수가 없거나 비어 있을 때 쓸 기본값</param>
    <input>운영체제 환경변수</input>
    <output>앞뒤 공백을 지운 문자열</output>
    <reason>코드를 고치지 않고 실습실마다 다른 카메라 주소를 넣을 수 있게 하기 위해서다.</reason>
    """
    value = os.environ.get(name)
    if value is None or value.strip() == "":
        return default
    return value.strip()


def _env_int(name, default):
    """
    <summary>환경변수에서 정수 설정을 읽는다. 잘못된 값이면 기본값을 쓴다.</summary>
    <param name="name">환경변수 이름</param>
    <param name="default">기본 정수값</param>
    <input>운영체제 환경변수</input>
    <output>정수</output>
    <reason>오타 하나로 서버가 시작되지 않는 일을 막고, 경고만 남기고 계속 실행하기 위해서다.</reason>
    """
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw.strip())
    except ValueError:
        _logger.warning("%s=%r 는 정수가 아니어서 기본값 %r 를 씁니다.", name, raw, default)
        return default


def _env_float(name, default):
    """
    <summary>환경변수에서 실수 설정을 읽는다. 잘못된 값이면 기본값을 쓴다.</summary>
    <param name="name">환경변수 이름</param>
    <param name="default">기본 실수값</param>
    <input>운영체제 환경변수</input>
    <output>실수</output>
    <reason>신뢰도 임계값이나 시간 간격처럼 소수가 필요한 설정을 안전하게 읽기 위해서다.</reason>
    """
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw.strip())
    except ValueError:
        _logger.warning("%s=%r 는 숫자가 아니어서 기본값 %r 를 씁니다.", name, raw, default)
        return default


def _env_choice(name, default, choices):
    """
    <summary>환경변수 값이 허용 목록 안에 있을 때만 받아들인다.</summary>
    <param name="name">환경변수 이름</param>
    <param name="default">기본값 (허용 목록에 들어 있어야 한다)</param>
    <param name="choices">허용하는 값들의 튜플</param>
    <input>운영체제 환경변수</input>
    <output>소문자로 바꾼 허용 값 하나</output>
    <reason>영상 소스처럼 정해진 값만 의미가 있는 설정에서 잘못된 입력을 걸러 내기 위해서다.</reason>
    """
    value = _env_str(name, default).lower()
    if value not in choices:
        _logger.warning("%s=%r 는 %s 중 하나가 아니어서 기본값 %r 를 씁니다.", name, value, choices, default)
        return default
    return value


# ---------------------------------------------------------------------------
# 영상 소스
# ---------------------------------------------------------------------------
# ESP32 의 MJPEG 주소. 시리얼 모니터에 나온 "스트림" 주소를 넣는다.
CAMERA_URL = _env_str("CAMERA_URL", "http://192.168.0.50/stream")

# esp32 = 위 주소에서 받기, webcam = 노트북 웹캠 (보드 없이 연습할 때)
SOURCE = _env_choice("SOURCE", "esp32", ("esp32", "webcam"))

# 웹캠 번호 (보통 0)
WEBCAM_INDEX = _env_int("WEBCAM_INDEX", 0)

# 스트림에서 이 시간(초) 동안 프레임이 오지 않으면 끊긴 것으로 본다.
STREAM_TIMEOUT_SEC = _env_float("STREAM_TIMEOUT_SEC", 5.0)

# 끊긴 뒤 다시 접속하기 전에 기다리는 시간(초)
RECONNECT_DELAY_SEC = _env_float("RECONNECT_DELAY_SEC", 2.0)

# ---------------------------------------------------------------------------
# 인식 모델
# ---------------------------------------------------------------------------
# 처음 실행할 때 ultralytics 가 자동으로 내려받는다.
MODEL_PATH = _env_str("MODEL_PATH", "yolo26n.pt")

# MODEL_PATH 를 불러오지 못하면 대신 쓸 모델
FALLBACK_MODEL_PATH = _env_str("FALLBACK_MODEL_PATH", "yolo11n.pt")

# cpu, cuda:0, mps 등
DEVICE = _env_str("DEVICE", "cpu")

# 추론 입력 크기. 작을수록 빠르고 작은 물체를 놓치기 쉽다. YOLO 는 32 의 배수를 쓴다.
IMG_SIZE = max(32, (_env_int("IMG_SIZE", 320) // 32) * 32)

# 이 신뢰도보다 낮은 검출은 버린다. (0~1)
CONF = min(1.0, max(0.0, _env_float("CONF", 0.4)))

# SKIP 프레임마다 한 번만 추론한다. 나머지 프레임은 직전 결과를 다시 그린다. 1 이면 매 프레임 추론.
SKIP = max(1, _env_int("SKIP", 2))

# ---------------------------------------------------------------------------
# 웹 서버와 대시보드
# ---------------------------------------------------------------------------
# 127.0.0.1 은 이 노트북에서만 접속 가능. 다른 기기에서 보려면 0.0.0.0 으로 바꾼다.
HOST = _env_str("HOST", "127.0.0.1")
PORT = _env_int("PORT", 5000)

# 대시보드로 상태를 밀어 보내는 간격(초)
SSE_INTERVAL_SEC = _env_float("SSE_INTERVAL_SEC", 0.2)

# 한 번의 SSE 메시지에 담는 로그 최대 개수
SSE_MAX_LOGS = max(1, _env_int("SSE_MAX_LOGS", 60))

# 브라우저로 보내는 영상의 JPEG 품질 (1~100)
JPEG_QUALITY = min(100, max(1, _env_int("JPEG_QUALITY", 80)))

# ---------------------------------------------------------------------------
# 계측과 로그
# ---------------------------------------------------------------------------
# 서버 메모리에 보관하는 로그 개수 (링버퍼)
LOG_MAXLEN = max(10, _env_int("LOG_MAXLEN", 500))

# 단계별 평균과 FPS 를 계산할 최근 프레임 수
STAT_WINDOW = max(2, _env_int("STAT_WINDOW", 30))

# INFO, DEBUG 로그는 이 프레임 수마다 한 번만 남긴다.
LOG_EVERY = max(1, _env_int("LOG_EVERY", 5))

# ---------------------------------------------------------------------------
# 저장
# ---------------------------------------------------------------------------
# 캡처 이미지를 저장할 노트북 폴더 (ESP32 의 SD 카드는 쓰지 않는다)
CAPTURE_DIR = Path(_env_str("CAPTURE_DIR", str(BASE_DIR / "captures")))


def as_dict():
    """
    <summary>현재 설정값을 사전 하나로 모은다.</summary>
    <input>이 모듈의 설정 상수</input>
    <output>설정 이름과 값으로 이루어진 사전 (경로는 문자열)</output>
    <reason>대시보드와 시작 로그에서 실제로 적용된 설정을 한눈에 보여 주기 위해서다.</reason>
    """
    return {
        "CAMERA_URL": CAMERA_URL,
        "SOURCE": SOURCE,
        "WEBCAM_INDEX": WEBCAM_INDEX,
        "STREAM_TIMEOUT_SEC": STREAM_TIMEOUT_SEC,
        "RECONNECT_DELAY_SEC": RECONNECT_DELAY_SEC,
        "MODEL_PATH": MODEL_PATH,
        "FALLBACK_MODEL_PATH": FALLBACK_MODEL_PATH,
        "DEVICE": DEVICE,
        "IMG_SIZE": IMG_SIZE,
        "CONF": CONF,
        "SKIP": SKIP,
        "HOST": HOST,
        "PORT": PORT,
        "SSE_INTERVAL_SEC": SSE_INTERVAL_SEC,
        "SSE_MAX_LOGS": SSE_MAX_LOGS,
        "JPEG_QUALITY": JPEG_QUALITY,
        "LOG_MAXLEN": LOG_MAXLEN,
        "STAT_WINDOW": STAT_WINDOW,
        "LOG_EVERY": LOG_EVERY,
        "CAPTURE_DIR": str(CAPTURE_DIR),
    }


if __name__ == "__main__":
    for key, value in as_dict().items():
        print("%-20s %s" % (key, value))
