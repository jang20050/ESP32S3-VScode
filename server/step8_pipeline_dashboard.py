"""
8단계: ESP32 영상 + YOLO 인식 + 파이프라인 대시보드 서버.

흐름
    ESP32 /stream (MJPEG)
      -> MjpegStreamReader 스레드 (JPEG 바이트만 잘라 최신 1장 보관)
      -> Processor 스레드 (캡처, 전처리, 추론, 후처리, 시각화, 출력)
      -> OutputStore (결과 JPEG)
      -> Flask (영상 피드, SSE 상태, 캡처 저장)

실행 예
    python step8_pipeline_dashboard.py --url http://192.168.45.60/stream
"""

import argparse
import json
import logging
import os
import threading
import time
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import cv2
import numpy as np
from flask import Flask, Response, jsonify, render_template, request, send_from_directory

import config
from pipeline import PIPELINE, stage

logger = logging.getLogger("step8")

# ---------------------------------------------------------------------------
# 상수
# ---------------------------------------------------------------------------
# 전처리에서 이보다 넓은 영상은 줄인다. (ESP32 VGA 는 그대로 통과)
MAX_WIDTH = 640

# 스트림에서 한 번에 읽는 최대 바이트 수
READ_CHUNK = 64 * 1024

# JPEG 시작을 찾지 못한 채 버퍼가 이만큼 쌓이면 비운다.
BUFFER_LIMIT = 2 * 1024 * 1024

JPEG_SOI = b"\xff\xd8"
JPEG_EOI = b"\xff\xd9"

# 디자인: 무채색 바탕에 강조색 하나 (BGR). 차분한 청록색.
ACCENT_BGR = (156, 169, 59)
INK_BGR = (28, 28, 28)
PAPER_BGR = (240, 240, 240)

# 런타임 설정 허용 범위
SKIP_RANGE = (1, 30)
IMGSZ_RANGE = (160, 1280)
CONF_RANGE = (0.01, 0.99)

# 프로그램 종료 신호. 피드와 SSE 생성기가 이 값을 보고 끝난다.
SHUTDOWN = threading.Event()


# ---------------------------------------------------------------------------
# 자료 구조
# ---------------------------------------------------------------------------
@dataclass
class RawFrame:
    """
    <summary>리더가 받은 디코딩 전 프레임 한 장.</summary>
    <input>ESP32 의 JPEG 바이트 또는 웹캠의 BGR 배열</input>
    <output>data, is_jpeg, nbytes, received, seq 를 가진 객체</output>
    <reason>디코딩을 capture 단계로 미뤄 그 시간을 계측하려면 원본을 그대로 넘겨야 하기 때문이다.</reason>
    """

    data: object
    is_jpeg: bool
    nbytes: int
    received: float
    seq: int


@dataclass
class Detection:
    """
    <summary>검출된 물체 하나.</summary>
    <input>YOLO 결과의 상자 하나</input>
    <output>클래스 번호, 이름, 신뢰도, 상자 좌표를 가진 객체</output>
    <reason>YOLO 내부 텐서 대신 단순한 파이썬 값으로 바꿔 두면 그리기와 JSON 전송이 쉬워진다.</reason>
    """

    cls_id: int
    name: str
    conf: float
    box: tuple

    def to_dict(self):
        """
        <summary>검출 결과를 JSON 으로 보낼 수 있는 사전으로 바꾼다.</summary>
        <input>Detection 객체</input>
        <output>cls_id, name, conf, box 키를 가진 사전</output>
        <reason>대시보드가 현재 검출 목록을 표로 보여 줄 수 있게 하기 위해서다.</reason>
        """
        return {
            "cls_id": self.cls_id,
            "name": self.name,
            "conf": round(self.conf, 3),
            "box": [int(value) for value in self.box],
        }


# ---------------------------------------------------------------------------
# 영상 리더
# ---------------------------------------------------------------------------
class FrameReader(threading.Thread):
    """
    <summary>최신 프레임 1장만 보관하는 리더의 공통 부분.</summary>
    <input>하위 클래스가 _publish 로 넣어 주는 프레임</input>
    <output>get_latest 로 꺼내는 가장 최근 RawFrame</output>
    <reason>처리가 수신보다 느릴 때 오래된 프레임이 쌓이면 화면이 점점 늦어지므로, 항상 최신 1장만 남기고 나머지는 버리기 위해서다.</reason>
    """

    def __init__(self, name):
        """
        <summary>잠금, 조건 변수, 통계 값을 준비한다.</summary>
        <param name="name">스레드 이름</param>
        <input>없음</input>
        <output>시작 전 리더 객체</output>
        <reason>수신 스레드와 처리 스레드가 프레임 한 칸을 안전하게 주고받기 위해서다.</reason>
        """
        super().__init__(name=name, daemon=True)
        self._stop_event = threading.Event()
        self._cond = threading.Condition()
        self._latest = None
        self._taken = True
        self._seq = 0
        self.dropped = 0
        self._stat_start = time.perf_counter()
        self._stat_frames = 0
        self._stat_bytes = 0

    def stop(self):
        """
        <summary>리더를 멈추라고 알리고 기다리는 쪽을 깨운다.</summary>
        <input>없음</input>
        <output>없음</output>
        <reason>종료할 때 처리 스레드가 프레임을 기다리며 멈춰 있지 않게 하기 위해서다.</reason>
        """
        self._stop_event.set()
        with self._cond:
            self._cond.notify_all()

    def stopped(self):
        """
        <summary>멈춤 요청을 받았는지 알려 준다.</summary>
        <input>없음</input>
        <output>멈춤 요청이 있으면 True</output>
        <reason>수신 반복문이 매번 종료 여부를 확인하게 하기 위해서다.</reason>
        """
        return self._stop_event.is_set()

    def _publish(self, data, is_jpeg, nbytes):
        """
        <summary>새 프레임을 최신 칸에 넣는다. 아직 안 꺼낸 프레임이 있으면 버리고 dropped 를 센다.</summary>
        <param name="data">JPEG 바이트 또는 BGR 배열</param>
        <param name="is_jpeg">data 가 JPEG 바이트이면 True</param>
        <param name="nbytes">수신한 바이트 수</param>
        <input>수신한 프레임</input>
        <output>없음</output>
        <reason>버려진 프레임 수를 보면 처리 속도가 카메라 속도를 따라가는지 알 수 있다.</reason>
        """
        with self._cond:
            if self._latest is not None and not self._taken:
                self.dropped += 1
                PIPELINE.increment("stream_dropped")
            self._seq += 1
            self._latest = RawFrame(data, is_jpeg, nbytes, time.perf_counter(), self._seq)
            self._taken = False
            self._cond.notify_all()
        self._update_stats(nbytes)

    def _update_stats(self, nbytes):
        """
        <summary>1초마다 수신 FPS 와 kbps 를 계산해 상태값에 쓴다.</summary>
        <param name="nbytes">이번 프레임의 바이트 수</param>
        <input>수신한 프레임 크기</input>
        <output>없음</output>
        <reason>무선 구간의 속도를 처리 속도와 따로 보아야 어디가 느린지 구분할 수 있다.</reason>
        """
        self._stat_frames += 1
        self._stat_bytes += nbytes
        now = time.perf_counter()
        elapsed = now - self._stat_start
        if elapsed >= 1.0:
            PIPELINE.set_status(
                stream_fps=round(self._stat_frames / elapsed, 1),
                stream_kbps=round(self._stat_bytes * 8 / 1000.0 / elapsed, 1),
            )
            self._stat_start = now
            self._stat_frames = 0
            self._stat_bytes = 0

    def _reset_stats(self):
        """
        <summary>수신 통계를 0 으로 되돌린다.</summary>
        <input>없음</input>
        <output>없음</output>
        <reason>연결이 끊긴 동안 마지막 속도가 그대로 보이지 않게 하기 위해서다.</reason>
        """
        self._stat_start = time.perf_counter()
        self._stat_frames = 0
        self._stat_bytes = 0
        PIPELINE.set_status(stream_fps=0.0, stream_kbps=0.0)

    def get_latest(self, timeout_sec):
        """
        <summary>아직 꺼내지 않은 새 프레임이 생길 때까지 기다렸다가 꺼낸다.</summary>
        <param name="timeout_sec">최대 대기 시간(초)</param>
        <input>최신 프레임 칸</input>
        <output>RawFrame. 시간 안에 새 프레임이 없거나 멈춤 요청이 오면 None</output>
        <reason>같은 프레임을 두 번 처리하지 않고, 새 프레임이 올 때까지 처리 스레드를 쉬게 하기 위해서다.</reason>
        """
        deadline = time.perf_counter() + timeout_sec
        with self._cond:
            while self._taken or self._latest is None:
                if self.stopped():
                    return None
                remaining = deadline - time.perf_counter()
                if remaining <= 0:
                    return None
                self._cond.wait(remaining)
            self._taken = True
            return self._latest


class MjpegStreamReader(FrameReader):
    """
    <summary>ESP32 의 MJPEG 스트림을 받아 JPEG 바이트를 한 장씩 잘라 내는 스레드.</summary>
    <input>http 주소의 multipart MJPEG 스트림</input>
    <output>최신 JPEG 한 장 (디코딩하지 않은 상태)</output>
    <reason>OpenCV 의 VideoCapture 는 버퍼와 재접속을 제어하기 어려워, 직접 바이트를 읽어 최신 프레임만 남기고 끊기면 다시 접속하기 위해서다.</reason>
    """

    def __init__(self, url, timeout_sec, reconnect_delay_sec):
        """
        <summary>접속 주소와 시간 설정을 저장한다.</summary>
        <param name="url">ESP32 스트림 주소</param>
        <param name="timeout_sec">이 시간 동안 데이터가 없으면 끊긴 것으로 본다</param>
        <param name="reconnect_delay_sec">재접속 전 대기 시간</param>
        <input>설정값</input>
        <output>시작 전 리더</output>
        <reason>실습실마다 다른 주소와 네트워크 상태에 맞출 수 있게 하기 위해서다.</reason>
        """
        super().__init__("MjpegStreamReader")
        self.url = url
        self.timeout_sec = timeout_sec
        self.reconnect_delay_sec = reconnect_delay_sec
        self._response = None
        self._response_lock = threading.Lock()

    def stop(self):
        """
        <summary>멈춤을 알리고 열린 연결을 닫아 읽기를 풀어 준다.</summary>
        <input>없음</input>
        <output>없음</output>
        <reason>네트워크 읽기에서 막혀 있는 스레드를 바로 끝내기 위해서다.</reason>
        """
        super().stop()
        with self._response_lock:
            if self._response is not None:
                try:
                    self._response.close()
                except Exception:
                    pass

    def run(self):
        """
        <summary>접속, 읽기, 재접속을 멈춤 요청이 올 때까지 반복한다.</summary>
        <input>스트림 주소</input>
        <output>없음 (프레임은 _publish 로 넘긴다)</output>
        <reason>공유기 문제나 보드 재시작 뒤에도 서버를 다시 켜지 않고 영상을 이어 받기 위해서다.</reason>
        """
        attempt = 0
        while not self.stopped():
            attempt += 1
            PIPELINE.set_status(camera="connecting")
            try:
                self._read_stream()
            except Exception as exc:
                if self.stopped():
                    break
                PIPELINE.set_status(camera="disconnected")
                PIPELINE.increment("reconnects")
                PIPELINE.log("WARN", "스트림 연결 실패 (%d번째): %s: %s" % (attempt, type(exc).__name__, exc), stage="capture")
                logger.warning("스트림 연결 실패: %s", exc)
            self._reset_stats()
            self._stop_event.wait(self.reconnect_delay_sec)
        PIPELINE.set_status(camera="stopped")

    def _read_stream(self):
        """
        <summary>스트림 하나에 접속해 끊길 때까지 JPEG 를 잘라 낸다.</summary>
        <input>HTTP 응답 바이트</input>
        <output>없음. 연결이 끝나면 예외를 던지거나 반환한다</output>
        <reason>multipart 헤더를 해석하지 않고 JPEG 시작 표시 FFD8 과 끝 표시 FFD9 만 찾으면 간단하고 튼튼하게 프레임을 나눌 수 있다.</reason>
        """
        response = urllib.request.urlopen(self.url, timeout=self.timeout_sec)
        with self._response_lock:
            self._response = response
        try:
            PIPELINE.set_status(camera="connected")
            PIPELINE.log("INFO", "스트림 연결됨: %s" % self.url, stage="capture", always=True)
            logger.info("스트림 연결됨: %s", self.url)
            read = getattr(response, "read1", response.read)
            buffer = bytearray()
            while not self.stopped():
                chunk = read(READ_CHUNK)
                if not chunk:
                    raise ConnectionError("서버가 연결을 닫음")
                buffer.extend(chunk)
                while True:
                    start = buffer.find(JPEG_SOI)
                    if start < 0:
                        if len(buffer) > BUFFER_LIMIT:
                            buffer.clear()
                        else:
                            # 다음 조각과 합쳐질 수 있는 마지막 1바이트만 남긴다.
                            del buffer[:-1]
                        break
                    end = buffer.find(JPEG_EOI, start + 2)
                    if end < 0:
                        if start > 0:
                            del buffer[:start]
                        if len(buffer) > BUFFER_LIMIT:
                            buffer.clear()
                        break
                    jpeg = bytes(buffer[start:end + 2])
                    del buffer[:end + 2]
                    self._publish(jpeg, True, len(jpeg))
        finally:
            with self._response_lock:
                self._response = None
            try:
                response.close()
            except Exception:
                pass


class WebcamReader(FrameReader):
    """
    <summary>노트북 웹캠에서 프레임을 받는 예비 리더.</summary>
    <input>웹캠 장치 번호</input>
    <output>최신 BGR 프레임 한 장</output>
    <reason>보드나 Wi-Fi 에 문제가 있어도 같은 파이프라인으로 실습을 이어 가기 위해 MjpegStreamReader 와 같은 사용법을 제공한다.</reason>
    """

    def __init__(self, index, reconnect_delay_sec):
        """
        <summary>장치 번호와 재시도 간격을 저장한다.</summary>
        <param name="index">웹캠 번호</param>
        <param name="reconnect_delay_sec">열기 실패 시 다시 시도하기 전 대기 시간</param>
        <input>설정값</input>
        <output>시작 전 리더</output>
        <reason>노트북마다 웹캠 번호가 다를 수 있기 때문이다.</reason>
        """
        super().__init__("WebcamReader")
        self.index = index
        self.reconnect_delay_sec = reconnect_delay_sec

    def run(self):
        """
        <summary>웹캠을 열고 프레임을 계속 읽는다. 실패하면 다시 연다.</summary>
        <input>웹캠 장치</input>
        <output>없음 (프레임은 _publish 로 넘긴다)</output>
        <reason>웹캠이 다른 프로그램에 잡혀 있다가 풀리는 경우에도 자동으로 이어 받기 위해서다.</reason>
        """
        while not self.stopped():
            PIPELINE.set_status(camera="connecting")
            if os.name == "nt":
                capture = cv2.VideoCapture(self.index, cv2.CAP_DSHOW)
            else:
                capture = cv2.VideoCapture(self.index)
            try:
                if not capture.isOpened():
                    raise ConnectionError("웹캠 %d 을 열 수 없음" % self.index)
                PIPELINE.set_status(camera="connected")
                PIPELINE.log("INFO", "웹캠 %d 연결됨" % self.index, stage="capture", always=True)
                while not self.stopped():
                    ok, frame = capture.read()
                    if not ok or frame is None:
                        raise ConnectionError("웹캠 프레임 읽기 실패")
                    self._publish(frame, False, int(frame.nbytes))
            except Exception as exc:
                if not self.stopped():
                    PIPELINE.set_status(camera="disconnected")
                    PIPELINE.increment("reconnects")
                    PIPELINE.log("WARN", "웹캠 오류: %s" % exc, stage="capture")
            finally:
                capture.release()
            self._reset_stats()
            self._stop_event.wait(self.reconnect_delay_sec)
        PIPELINE.set_status(camera="stopped")


# ---------------------------------------------------------------------------
# 모델과 설정
# ---------------------------------------------------------------------------
class Detector:
    """
    <summary>YOLO 모델을 불러오고 추론을 실행하는 객체.</summary>
    <input>모델 파일 이름, 장치 이름, 클래스 필터</input>
    <output>predict 로 얻는 ultralytics 결과 목록</output>
    <reason>모델 로드 실패, 장치 미지원 같은 환경 차이를 한 곳에서 처리해 처리 코드가 단순해지게 하기 위해서다.</reason>
    """

    def __init__(self, model_path, fallback_path, device, classes=None):
        """
        <summary>장치를 확인하고 모델을 불러온 뒤 클래스 필터를 정한다.</summary>
        <param name="model_path">먼저 시도할 모델 파일</param>
        <param name="fallback_path">실패하면 쓸 모델 파일</param>
        <param name="device">cpu, cuda, cuda:0, mps 등</param>
        <param name="classes">검출할 클래스 이름이나 번호 목록. None 이면 전부</param>
        <input>설정값</input>
        <output>사용 준비가 된 Detector. 두 모델 모두 실패하면 RuntimeError</output>
        <reason>ultralytics 는 여기서만 불러와 --no-yolo 로 실행할 때는 torch 를 읽지 않게 하기 위해서다.</reason>
        """
        self.device = self._resolve_device(device)
        self.half = self.device.startswith("cuda")
        self._precision = self._half_option() if self.half else dict()
        self.model, self.model_name = self._load(model_path, fallback_path)
        self.names = dict(self.model.names)
        self.class_ids = self._resolve_classes(classes)

    @staticmethod
    def _resolve_device(device):
        """
        <summary>요청한 장치를 쓸 수 있는지 확인하고, 안 되면 CPU 로 바꾼다.</summary>
        <param name="device">요청한 장치 이름</param>
        <input>장치 문자열</input>
        <output>실제로 쓸 장치 문자열</output>
        <reason>GPU 가 없는 노트북에서 --device cuda 로 실행해도 멈추지 않고 경고 후 계속 실행하기 위해서다.</reason>
        """
        name = (device or "cpu").lower()
        if name == "cpu":
            return "cpu"
        try:
            import torch
        except Exception as exc:
            PIPELINE.log("WARN", "torch 를 불러오지 못해 CPU 로 실행: %s" % exc, stage="inference")
            return "cpu"
        if name.startswith("cuda") and not torch.cuda.is_available():
            PIPELINE.log("WARN", "CUDA 를 쓸 수 없어 CPU 로 실행", stage="inference")
            logger.warning("CUDA 를 쓸 수 없어 CPU 로 실행합니다.")
            return "cpu"
        if name == "mps":
            backend = getattr(torch.backends, "mps", None)
            if backend is None or not backend.is_available():
                PIPELINE.log("WARN", "MPS 를 쓸 수 없어 CPU 로 실행", stage="inference")
                logger.warning("MPS 를 쓸 수 없어 CPU 로 실행합니다.")
                return "cpu"
        return name

    @staticmethod
    def _half_option():
        """
        <summary>설치된 ultralytics 버전에 맞는 반정밀도(FP16) 옵션을 고른다.</summary>
        <input>ultralytics 기본 설정 목록</input>
        <output>quantize 16 또는 half True 를 담은 사전</output>
        <reason>최신 버전은 half 대신 quantize 를 쓰고 half 를 쓰면 매 프레임 경고를 찍기 때문에, 버전에 따라 알맞은 이름을 써야 한다.</reason>
        """
        try:
            from ultralytics.cfg import DEFAULT_CFG_DICT
        except Exception:
            return dict(half=True)
        if "quantize" in DEFAULT_CFG_DICT:
            return dict(quantize=16)
        return dict(half=True)

    @staticmethod
    def _load(model_path, fallback_path):
        """
        <summary>기본 모델을 불러오고, 실패하면 예비 모델을 불러온다.</summary>
        <param name="model_path">기본 모델 파일</param>
        <param name="fallback_path">예비 모델 파일</param>
        <input>모델 파일 이름 (없으면 자동 다운로드)</input>
        <output>(YOLO 모델, 실제로 불러온 파일 이름) 튜플</output>
        <reason>새 모델 이름을 아직 지원하지 않는 ultralytics 버전에서도 실습이 진행되게 하기 위해서다.</reason>
        """
        from ultralytics import YOLO

        errors = []
        for path in (model_path, fallback_path):
            if not path:
                continue
            try:
                model = YOLO(path)
                logger.info("모델 로드: %s", path)
                return model, path
            except Exception as exc:
                errors.append("%s: %s" % (path, exc))
                PIPELINE.log("WARN", "모델 로드 실패 %s: %s" % (path, exc), stage="inference")
                logger.warning("모델 로드 실패 %s: %s", path, exc)
        raise RuntimeError("모델을 불러오지 못함 (%s)" % " / ".join(errors))

    def _resolve_classes(self, classes):
        """
        <summary>클래스 이름이나 번호 목록을 모델의 클래스 번호 목록으로 바꾼다.</summary>
        <param name="classes">person, cup 또는 0, 41 같은 문자열 목록</param>
        <input>명령줄에서 받은 클래스 목록</input>
        <output>클래스 번호 목록. 필터가 없으면 None</output>
        <reason>관심 있는 물체만 검출하면 화면이 깔끔해지고 학생이 결과를 읽기 쉬워진다.</reason>
        """
        if not classes:
            return None
        lookup = dict((str(name).lower(), int(cls_id)) for cls_id, name in self.names.items())
        result = []
        for item in classes:
            text = str(item).strip().lower()
            if not text:
                continue
            if text.isdigit() and int(text) in self.names:
                result.append(int(text))
            elif text in lookup:
                result.append(lookup[text])
            else:
                PIPELINE.log("WARN", "알 수 없는 클래스 무시: %s" % item, stage="inference")
        return result or None

    def warmup(self, imgsz):
        """
        <summary>빈 이미지로 추론을 한 번 실행한다.</summary>
        <param name="imgsz">추론 입력 크기</param>
        <input>검은 이미지 한 장</input>
        <output>워밍업에 걸린 시간(ms)</output>
        <reason>첫 추론은 초기화 때문에 몇 배 느리므로, 미리 실행해 두지 않으면 첫 프레임이 병목처럼 보이기 때문이다.</reason>
        """
        started = time.perf_counter()
        blank = np.zeros((imgsz, imgsz, 3), dtype=np.uint8)
        self.predict(blank, imgsz, 0.5)
        return (time.perf_counter() - started) * 1000.0

    def predict(self, image, imgsz, conf):
        """
        <summary>이미지 한 장에 대해 YOLO 추론을 실행한다.</summary>
        <param name="image">BGR uint8 이미지</param>
        <param name="imgsz">추론 입력 크기</param>
        <param name="conf">신뢰도 임계값</param>
        <input>BGR 이미지</input>
        <output>ultralytics Results 목록</output>
        <reason>추론 옵션을 한 곳에 모아 장치, 반정밀도, 클래스 필터가 항상 같이 적용되게 하기 위해서다.</reason>
        """
        return self.model.predict(
            image,
            imgsz=imgsz,
            conf=conf,
            device=self.device,
            classes=self.class_ids,
            verbose=False,
            **self._precision,
        )


class RuntimeSettings:
    """
    <summary>실행 중에 바꿀 수 있는 skip, imgsz, conf 설정.</summary>
    <input>초기값과 대시보드의 변경 요청</input>
    <output>snapshot 으로 얻는 현재 설정 사전</output>
    <reason>처리 스레드가 읽는 도중 Flask 스레드가 값을 바꿔도 섞이지 않도록 lock 으로 보호하기 위해서다.</reason>
    """

    def __init__(self, skip, imgsz, conf):
        """
        <summary>초기 설정값을 저장한다.</summary>
        <param name="skip">몇 프레임마다 추론할지</param>
        <param name="imgsz">추론 입력 크기</param>
        <param name="conf">신뢰도 임계값</param>
        <input>설정값</input>
        <output>설정 객체</output>
        <reason>명령줄 옵션을 시작값으로 쓰기 위해서다.</reason>
        """
        self._lock = threading.Lock()
        self._values = {
            "skip": int(skip),
            "imgsz": int(imgsz),
            "conf": float(conf),
        }

    def snapshot(self):
        """
        <summary>현재 설정의 복사본을 돌려준다.</summary>
        <input>없음</input>
        <output>skip, imgsz, conf 키를 가진 사전</output>
        <reason>한 프레임을 처리하는 동안 같은 설정을 쓰도록 프레임 시작 때 한 번 복사하기 위해서다.</reason>
        """
        with self._lock:
            return dict(self._values)

    def update(self, values):
        """
        <summary>요청 값을 검증하고, 모두 올바를 때만 한꺼번에 적용한다.</summary>
        <param name="values">바꿀 이름과 값의 사전</param>
        <input>대시보드가 보낸 JSON</input>
        <output>(적용된 전체 설정, 오류 사전) 튜플. 오류가 있으면 아무것도 바꾸지 않는다</output>
        <reason>잘못된 값 하나 때문에 추론이 멈추거나 설정 일부만 바뀌는 일을 막기 위해서다.</reason>
        """
        errors = dict()
        parsed = dict()
        if not isinstance(values, dict):
            errors["body"] = "JSON 객체가 필요합니다"
            return self.snapshot(), errors
        for key, raw in values.items():
            try:
                if key == "skip":
                    number = int(raw)
                    if not SKIP_RANGE[0] <= number <= SKIP_RANGE[1]:
                        raise ValueError("%d~%d 사이여야 합니다" % SKIP_RANGE)
                    parsed[key] = number
                elif key == "imgsz":
                    number = int(raw)
                    if not IMGSZ_RANGE[0] <= number <= IMGSZ_RANGE[1]:
                        raise ValueError("%d~%d 사이여야 합니다" % IMGSZ_RANGE)
                    if number % 32 != 0:
                        raise ValueError("32의 배수여야 합니다")
                    parsed[key] = number
                elif key == "conf":
                    number = float(raw)
                    if not CONF_RANGE[0] <= number <= CONF_RANGE[1]:
                        raise ValueError("%.2f~%.2f 사이여야 합니다" % CONF_RANGE)
                    parsed[key] = number
                else:
                    raise ValueError("바꿀 수 없는 설정입니다")
            except (TypeError, ValueError) as exc:
                errors[key] = str(exc)
        if errors:
            return self.snapshot(), errors
        with self._lock:
            self._values.update(parsed)
            return dict(self._values), errors


class OutputStore:
    """
    <summary>최신 결과 JPEG 와 원본 프레임을 보관하고, 새 프레임을 기다리는 피드를 깨운다.</summary>
    <input>output 단계가 넣는 결과</input>
    <output>피드와 캡처 기능이 꺼내 쓰는 JPEG 바이트</output>
    <reason>ESP32 는 동시에 한 명에게만 스트림을 줄 수 있으므로, 서버가 받은 영상을 여기서 여러 브라우저에 나눠 주기 위해서다.</reason>
    """

    def __init__(self, jpeg_quality):
        """
        <summary>빈 보관소를 만든다.</summary>
        <param name="jpeg_quality">웹캠 원본을 JPEG 로 바꿀 때의 품질</param>
        <input>설정값</input>
        <output>빈 OutputStore</output>
        <reason>원본 피드를 위해 웹캠 프레임만 필요할 때 인코딩하기 위해서다.</reason>
        """
        self._cond = threading.Condition()
        self._seq = 0
        self._detect = None
        self._raw = None
        self._raw_jpeg = None
        self._jpeg_quality = int(jpeg_quality)

    def update(self, detect_jpeg, raw):
        """
        <summary>새 결과와 원본을 저장하고 기다리는 피드를 깨운다.</summary>
        <param name="detect_jpeg">박스를 그린 결과 JPEG 바이트</param>
        <param name="raw">같은 프레임의 RawFrame</param>
        <input>한 프레임의 결과</input>
        <output>없음</output>
        <reason>결과 피드와 원본 피드가 같은 프레임 번호를 공유해 나란히 비교할 수 있게 하기 위해서다.</reason>
        """
        with self._cond:
            self._seq += 1
            self._detect = detect_jpeg
            self._raw = raw
            self._raw_jpeg = raw.data if raw.is_jpeg else None
            self._cond.notify_all()

    def latest_detect(self):
        """
        <summary>가장 최근 결과 JPEG 를 돌려준다.</summary>
        <input>없음</input>
        <output>JPEG 바이트. 아직 없으면 None</output>
        <reason>캡처 버튼을 누른 순간의 화면을 저장하기 위해서다.</reason>
        """
        with self._cond:
            return self._detect

    def wait(self, kind, last_seq, timeout_sec):
        """
        <summary>last_seq 보다 새 프레임이 생길 때까지 기다렸다가 돌려준다.</summary>
        <param name="kind">detect 또는 raw</param>
        <param name="last_seq">피드가 마지막으로 보낸 프레임 번호</param>
        <param name="timeout_sec">최대 대기 시간</param>
        <input>보관 중인 결과</input>
        <output>(프레임 번호, JPEG 바이트) 튜플. 새 프레임이 없으면 바이트는 None</output>
        <reason>같은 프레임을 반복 전송하지 않아 브라우저와 네트워크의 부담을 줄이기 위해서다.</reason>
        """
        with self._cond:
            self._cond.wait_for(lambda: self._seq != last_seq, timeout_sec)
            seq = self._seq
            if seq == last_seq:
                return seq, None
            if kind == "detect":
                return seq, self._detect
            if self._raw_jpeg is not None:
                return seq, self._raw_jpeg
            raw = self._raw
        if raw is None:
            return seq, None
        ok, encoded = cv2.imencode(".jpg", raw.data, [int(cv2.IMWRITE_JPEG_QUALITY), self._jpeg_quality])
        if not ok:
            return seq, None
        data = encoded.tobytes()
        with self._cond:
            if self._seq == seq:
                self._raw_jpeg = data
        return seq, data


# ---------------------------------------------------------------------------
# 파이프라인 6단계
# ---------------------------------------------------------------------------
@stage("capture", manual=True)
def capture_frame(reader, timeout_sec):
    """
    <summary>리더에서 최신 프레임을 꺼내 OpenCV 이미지로 디코딩한다.</summary>
    <param name="reader">MjpegStreamReader 또는 WebcamReader</param>
    <param name="timeout_sec">새 프레임을 기다릴 최대 시간(초)</param>
    <input>ESP32 가 Wi-Fi 로 보낸 JPEG 바이트 한 장</input>
    <output>(BGR 이미지, 원본 RawFrame) 튜플. 시간 안에 프레임이 없으면 None</output>
    <reason>카메라가 보낸 것은 압축된 JPEG 바이트라서 그대로는 픽셀 계산을 할 수 없다. 이 단계에서 압축을 풀어 숫자 배열로 바꾼다. 이 단계의 시간에는 새 프레임을 기다린 시간이 포함되므로, 이 단계가 병목이면 컴퓨터가 아니라 무선 전송이 느리다는 뜻이다. 프레임 나이는 받은 뒤 처리 시작까지 얼마나 묵었는지를 보여 준다.</reason>
    """
    started = time.perf_counter()
    raw = reader.get_latest(timeout_sec)
    taken = time.perf_counter()
    if raw is None:
        return None
    wait_ms = (taken - started) * 1000.0
    age_ms = (taken - raw.received) * 1000.0

    if raw.is_jpeg:
        image = cv2.imdecode(np.frombuffer(raw.data, dtype=np.uint8), cv2.IMREAD_COLOR)
        if image is None:
            raise ValueError("JPEG 디코딩 실패 (%d bytes)" % raw.nbytes)
    else:
        image = raw.data
    decode_ms = (time.perf_counter() - taken) * 1000.0

    PIPELINE.record(
        "capture",
        wait_ms + decode_ms,
        "%d bytes" % raw.nbytes,
        "(%s) %s" % (",".join(str(size) for size in image.shape), image.dtype),
    )
    PIPELINE.set_status(
        capture_wait_ms=round(wait_ms, 1),
        decode_ms=round(decode_ms, 1),
        frame_age_ms=round(age_ms, 1),
    )
    PIPELINE.log(
        "DEBUG",
        "wait=%.1fms decode=%.1fms age=%.1fms" % (wait_ms, decode_ms, age_ms),
        stage="capture",
    )
    return image, raw


@stage("preprocess")
def preprocess(image, max_width):
    """
    <summary>이미지를 3채널 BGR uint8 로 맞추고, 너무 넓으면 비율을 유지한 채 줄인다.</summary>
    <param name="image">디코딩된 이미지</param>
    <param name="max_width">허용하는 최대 가로 픽셀 수</param>
    <input>캡처 단계의 이미지 배열 (예: 480 x 640 x 3)</input>
    <output>모델과 그리기 단계가 바로 쓸 수 있는 BGR uint8 이미지</output>
    <reason>카메라나 웹캠에 따라 흑백, 투명도 채널, 다른 자료형이 섞여 들어올 수 있는데, 모델은 정해진 형식만 받는다. 형식을 여기서 한 번 통일해 두면 뒤 단계가 입력을 걱정하지 않아도 된다. 큰 영상은 미리 줄여 추론과 그리기 시간을 아낀다.</reason>
    """
    if image.ndim == 2:
        image = cv2.cvtColor(image, cv2.COLOR_GRAY2BGR)
    elif image.shape[2] == 4:
        image = cv2.cvtColor(image, cv2.COLOR_BGRA2BGR)
    if image.dtype != np.uint8:
        image = np.clip(image, 0, 255).astype(np.uint8)
    height, width = image.shape[:2]
    if width > max_width:
        scale = max_width / float(width)
        image = cv2.resize(image, (max_width, int(round(height * scale))), interpolation=cv2.INTER_AREA)
    return np.ascontiguousarray(image)


@stage("inference", manual=True)
def run_inference(detector, image, settings_now, frame_index, use_yolo):
    """
    <summary>YOLO 모델로 이미지 속 물체를 찾는다. skip 설정에 따라 일부 프레임은 건너뛴다.</summary>
    <param name="detector">Detector 객체 (없으면 None)</param>
    <param name="image">전처리된 BGR 이미지</param>
    <param name="settings_now">이번 프레임의 skip, imgsz, conf 설정</param>
    <param name="frame_index">처리한 프레임의 순번</param>
    <param name="use_yolo">False 이면 항상 건너뛴다</param>
    <input>전처리된 이미지 한 장</input>
    <output>ultralytics 결과 목록. 건너뛴 프레임은 None</output>
    <reason>보통 가장 오래 걸리는 단계다. 모델은 이미지를 imgsz 크기로 바꾼 뒤 신경망을 통과시키고 겹친 상자를 정리하는데, ultralytics 가 잰 이 세 시간을 각각 전처리, 추론, 후처리 단계에 나눠 기록해 신경망 자체가 얼마나 걸리는지 따로 보여 준다. 매 프레임 추론할 수 없을 만큼 느리면 skip 으로 몇 프레임에 한 번만 추론하고 나머지는 직전 결과를 다시 쓴다.</reason>
    """
    if not use_yolo or detector is None:
        PIPELINE.log("WARN", "YOLO 꺼짐(--no-yolo): 추론 생략", stage="inference", throttle=True)
        return None
    skip = settings_now["skip"]
    if frame_index % skip != 0:
        PIPELINE.log("WARN", "skip=%d: 추론 생략, 직전 결과 재사용" % skip, stage="inference", throttle=True, key="skip")
        return None

    started = time.perf_counter()
    results = detector.predict(image, settings_now["imgsz"], settings_now["conf"])
    wall_ms = (time.perf_counter() - started) * 1000.0

    speed = results[0].speed or dict()
    pre_ms = float(speed.get("preprocess", 0.0) or 0.0)
    post_ms = float(speed.get("postprocess", 0.0) or 0.0)
    # 세 값의 합과 실제 걸린 시간의 차이(파이썬 호출 비용)는 추론에 포함한다.
    infer_ms = max(float(speed.get("inference", 0.0) or 0.0), wall_ms - pre_ms - post_ms)
    count = len(results[0].boxes) if results[0].boxes is not None else 0

    shape_text = "(%s) %s" % (",".join(str(size) for size in image.shape), image.dtype)
    PIPELINE.record("preprocess", pre_ms, shape_text, "letterbox %d" % settings_now["imgsz"])
    PIPELINE.record("inference", infer_ms, "imgsz=%d" % settings_now["imgsz"], "%d raw boxes" % count)
    PIPELINE.record("postprocess", post_ms, "raw output", "%d boxes" % count)
    return results


@stage("postprocess")
def postprocess(results, previous):
    """
    <summary>YOLO 결과를 Detection 목록으로 바꾼다. 추론을 건너뛴 프레임은 직전 목록을 그대로 쓴다.</summary>
    <param name="results">run_inference 의 결과 또는 None</param>
    <param name="previous">직전 프레임의 Detection 목록</param>
    <input>모델이 내놓은 상자 좌표, 신뢰도, 클래스 번호 텐서</input>
    <output>Detection 목록 (이름, 신뢰도, 상자 좌표)</output>
    <reason>모델 출력은 GPU 나 CPU 위의 텐서라서 그대로 그리거나 JSON 으로 보낼 수 없다. 좌표를 정수로, 클래스 번호를 사람이 읽는 이름으로 바꿔 다음 단계가 쓰기 쉽게 만든다. 건너뛴 프레임에서 직전 결과를 다시 쓰면 박스가 깜빡이지 않지만, 물체가 빨리 움직이면 박스가 조금 늦게 따라온다.</reason>
    """
    if results is None:
        return previous
    result = results[0]
    boxes = result.boxes
    if boxes is None or len(boxes) == 0:
        return []
    names = result.names
    xyxy = boxes.xyxy.cpu().numpy()
    confs = boxes.conf.cpu().numpy()
    classes = boxes.cls.cpu().numpy().astype(int)
    detections = []
    for coords, conf, cls_id in zip(xyxy, confs, classes):
        x1, y1, x2, y2 = (int(round(float(value))) for value in coords)
        detections.append(Detection(int(cls_id), str(names.get(int(cls_id), cls_id)), float(conf), (x1, y1, x2, y2)))
    return detections


@stage("visualize")
def visualize(image, detections, hud_lines):
    """
    <summary>검출 상자, 이름, 신뢰도, 상태 문구를 이미지 위에 그린다.</summary>
    <param name="image">전처리된 BGR 이미지</param>
    <param name="detections">Detection 목록</param>
    <param name="hud_lines">왼쪽 위에 표시할 상태 문구 목록</param>
    <input>원본 이미지와 검출 목록</input>
    <output>상자가 그려진 새 이미지</output>
    <reason>숫자 목록만으로는 모델이 무엇을 어디서 찾았는지 알기 어렵다. 사람이 바로 확인할 수 있게 그림으로 겹쳐 그린다. 원본을 복사해서 그리므로 원본 피드는 깨끗하게 남는다.</reason>
    """
    canvas = image.copy()
    font = cv2.FONT_HERSHEY_SIMPLEX
    for item in detections:
        x1, y1, x2, y2 = item.box
        cv2.rectangle(canvas, (x1, y1), (x2, y2), ACCENT_BGR, 2, cv2.LINE_AA)
        label = "%s %.2f" % (item.name, item.conf)
        (text_w, text_h), baseline = cv2.getTextSize(label, font, 0.5, 1)
        top = max(0, y1 - text_h - baseline - 4)
        cv2.rectangle(canvas, (x1, top), (x1 + text_w + 6, top + text_h + baseline + 4), ACCENT_BGR, -1)
        cv2.putText(canvas, label, (x1 + 3, top + text_h + 2), font, 0.5, INK_BGR, 1, cv2.LINE_AA)

    y = 22
    for line in hud_lines:
        cv2.putText(canvas, line, (10, y), font, 0.55, INK_BGR, 3, cv2.LINE_AA)
        cv2.putText(canvas, line, (10, y), font, 0.55, PAPER_BGR, 1, cv2.LINE_AA)
        y += 22
    return canvas


@stage("output")
def output(image, raw, store, jpeg_quality):
    """
    <summary>결과 이미지를 JPEG 로 압축해 보관소에 넣는다.</summary>
    <param name="image">상자가 그려진 이미지</param>
    <param name="raw">같은 프레임의 원본 RawFrame</param>
    <param name="store">OutputStore 객체</param>
    <param name="jpeg_quality">JPEG 품질 (1~100)</param>
    <input>시각화된 BGR 이미지</input>
    <output>브라우저로 보낼 JPEG 바이트</output>
    <reason>브라우저는 숫자 배열을 바로 보여 줄 수 없고, 압축하지 않은 영상은 너무 커서 전송이 느리다. JPEG 로 압축해 보관소에 넣으면 여러 브라우저가 같은 결과를 나눠 볼 수 있다. 품질을 낮추면 빨라지지만 화질이 떨어진다.</reason>
    """
    ok, encoded = cv2.imencode(".jpg", image, [int(cv2.IMWRITE_JPEG_QUALITY), int(jpeg_quality)])
    if not ok:
        raise ValueError("JPEG 인코딩 실패")
    data = encoded.tobytes()
    store.update(data, raw)
    return data


# ---------------------------------------------------------------------------
# 처리 스레드
# ---------------------------------------------------------------------------
class Processor(threading.Thread):
    """
    <summary>프레임마다 6단계를 순서대로 실행하는 처리 스레드.</summary>
    <input>리더의 최신 프레임</input>
    <output>OutputStore 의 결과 JPEG 와 PIPELINE 의 계측 기록</output>
    <reason>수신, 처리, 웹 응답을 서로 다른 스레드로 나눠 하나가 느려도 나머지가 멈추지 않게 하기 위해서다.</reason>
    """

    def __init__(self, reader, detector, settings, store, use_yolo):
        """
        <summary>처리에 필요한 객체들을 받아 둔다.</summary>
        <param name="reader">프레임 리더</param>
        <param name="detector">Detector 또는 None</param>
        <param name="settings">RuntimeSettings</param>
        <param name="store">OutputStore</param>
        <param name="use_yolo">추론 사용 여부</param>
        <input>준비된 객체들</input>
        <output>시작 전 처리 스레드</output>
        <reason>필요한 객체를 밖에서 넣어 주면 웹캠과 ESP32 를 같은 코드로 처리할 수 있다.</reason>
        """
        super().__init__(name="Processor", daemon=True)
        self._reader = reader
        self._detector = detector
        self._settings = settings
        self._store = store
        self._use_yolo = use_yolo
        self._stop_event = threading.Event()
        self._index = 0
        self._fps = 0.0
        self._interval = 0.0
        self._last_end = None
        self._last_infer_ms = 0.0

    def stop(self):
        """
        <summary>처리 스레드에 멈춤을 알린다.</summary>
        <input>없음</input>
        <output>없음</output>
        <reason>Ctrl+C 로 종료할 때 진행 중인 프레임까지만 처리하고 끝내기 위해서다.</reason>
        """
        self._stop_event.set()

    def _update_fps(self):
        """
        <summary>프레임 사이 간격의 이동 평균으로 화면 표시용 FPS 를 갱신한다.</summary>
        <input>직전 프레임 종료 시각</input>
        <output>없음</output>
        <reason>FPS 값 자체를 평균하면 짧은 간격이 과대 반영되어 실제보다 높게 나오므로, 간격을 평균한 뒤 역수를 취한다.</reason>
        """
        now = time.perf_counter()
        if self._last_end is not None:
            interval = now - self._last_end
            if interval > 0:
                self._interval = interval if self._interval == 0.0 else self._interval * 0.9 + interval * 0.1
                self._fps = 1.0 / self._interval
        self._last_end = now

    def _hud_lines(self, settings_now, inferred, detections):
        """
        <summary>영상 왼쪽 위에 표시할 상태 문구를 만든다.</summary>
        <param name="settings_now">이번 프레임 설정</param>
        <param name="inferred">이번 프레임에서 추론을 실행했는지</param>
        <param name="detections">이번 프레임의 Detection 목록</param>
        <input>설정과 결과</input>
        <output>영문 문자열 목록 (OpenCV 기본 글꼴은 한글을 그리지 못한다)</output>
        <reason>대시보드를 보지 않아도 영상만으로 속도와 설정을 알 수 있게 하기 위해서다.</reason>
        """
        if not self._use_yolo:
            mode = "YOLO off"
        elif inferred:
            mode = "infer %.0fms" % self._last_infer_ms
        else:
            mode = "reuse (skip %d)" % settings_now["skip"]
        return [
            "FPS %.1f | %s" % (self._fps, mode),
            "imgsz %d | conf %.2f | objects %d" % (settings_now["imgsz"], settings_now["conf"], len(detections)),
        ]

    def run(self):
        """
        <summary>멈춤 요청이 올 때까지 begin_frame, 6단계, end_frame 을 반복한다. 실패하면 abort_frame 한다.</summary>
        <input>리더의 프레임</input>
        <output>없음</output>
        <reason>한 프레임에서 오류가 나도 그 프레임만 버리고 다음 프레임을 계속 처리하기 위해서다.</reason>
        """
        previous = []
        while not self._stop_event.is_set():
            PIPELINE.begin_frame()
            try:
                captured = capture_frame(self._reader, config.STREAM_TIMEOUT_SEC)
                if captured is None:
                    if self._stop_event.is_set():
                        PIPELINE.abort_frame("종료")
                        break
                    PIPELINE.abort_frame("%.0f초 동안 새 프레임 없음" % config.STREAM_TIMEOUT_SEC)
                    continue
                image, raw = captured
                image = preprocess(image, MAX_WIDTH)
                settings_now = self._settings.snapshot()

                started = time.perf_counter()
                results = run_inference(self._detector, image, settings_now, self._index, self._use_yolo)
                inferred = results is not None
                if inferred:
                    self._last_infer_ms = (time.perf_counter() - started) * 1000.0

                detections = postprocess(results, previous)
                previous = detections
                annotated = visualize(image, detections, self._hud_lines(settings_now, inferred, detections))
                output(annotated, raw, self._store, config.JPEG_QUALITY)

                PIPELINE.end_frame(inference_skipped=not inferred)
                PIPELINE.set_status(
                    objects=len(detections),
                    detections=[item.to_dict() for item in detections[:20]],
                )
                self._index += 1
                self._update_fps()
            except Exception as exc:
                logger.exception("프레임 처리 실패")
                PIPELINE.abort_frame("%s: %s" % (type(exc).__name__, exc))
                self._stop_event.wait(0.2)


# ---------------------------------------------------------------------------
# Flask 앱
# ---------------------------------------------------------------------------
FALLBACK_PAGE = """<!doctype html>
<html lang="ko">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Pipeline Dashboard</title>
<style>
body {
  margin: 0;
  padding: 24px 16px;
  font-family: system-ui, sans-serif;
  color: #222;
  background: #f6f6f5;
}
main {
  max-width: 1100px;
  margin: 0 auto;
}
.feeds {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
  gap: 16px;
}
img {
  width: 100%;
  border: 1px solid #ddd;
  background: #111;
}
a {
  color: #3b9ea9;
}
</style>
</head>
<body>
<main>
<h2>Pipeline Dashboard</h2>
<p>templates/dashboard.html 이 아직 없어 기본 화면을 보여 줍니다.</p>
<div class="feeds">
<div><p>결과</p><img src="/detect_feed" alt="detect feed"></div>
<div><p>원본</p><img src="/raw_feed" alt="raw feed"></div>
</div>
<p><a href="/api/pipeline">/api/pipeline</a> · <a href="/api/config">/api/config</a> · <a href="/events">/events</a> · <a href="/api/captures">/api/captures</a></p>
</main>
</body>
</html>
"""


class AppContext:
    """
    <summary>Flask 라우트가 함께 쓰는 실행 중 객체 묶음.</summary>
    <input>main 에서 만든 리더, 처리 스레드, 모델, 설정, 보관소</input>
    <output>라우트에서 읽는 속성들</output>
    <reason>전역 변수 여러 개 대신 객체 하나로 묶어 어떤 상태를 공유하는지 분명히 하기 위해서다.</reason>
    """

    def __init__(self, reader, processor, detector, settings, store, use_yolo, classes):
        """
        <summary>공유 객체를 저장한다.</summary>
        <param name="reader">프레임 리더</param>
        <param name="processor">처리 스레드</param>
        <param name="detector">Detector 또는 None</param>
        <param name="settings">RuntimeSettings</param>
        <param name="store">OutputStore</param>
        <param name="use_yolo">추론 사용 여부</param>
        <param name="classes">클래스 필터 원문 목록</param>
        <input>준비된 객체들</input>
        <output>AppContext</output>
        <reason>create_app 에 한 번에 넘기기 위해서다.</reason>
        """
        self.reader = reader
        self.processor = processor
        self.detector = detector
        self.settings = settings
        self.store = store
        self.use_yolo = use_yolo
        self.classes = classes


def create_app(ctx):
    """
    <summary>Flask 앱을 만들고 라우트를 등록한다.</summary>
    <param name="ctx">AppContext</param>
    <input>공유 객체</input>
    <output>Flask 앱</output>
    <reason>라우트가 공유 객체를 클로저로 참조하게 해 전역 상태를 줄이기 위해서다.</reason>
    """
    app = Flask(__name__, template_folder=str(config.BASE_DIR / "templates"))
    capture_dir = Path(config.CAPTURE_DIR)

    def mjpeg_response(kind):
        """
        <summary>OutputStore 의 프레임을 MJPEG 응답으로 흘려보낸다.</summary>
        <param name="kind">detect 또는 raw</param>
        <input>보관소의 최신 JPEG</input>
        <output>multipart MJPEG 스트리밍 응답</output>
        <reason>img 태그 하나로 브라우저에서 영상을 볼 수 있는 가장 간단한 방법이기 때문이다.</reason>
        """

        def generate():
            """
            <summary>새 프레임이 생길 때마다 MJPEG 파트 하나를 만든다.</summary>
            <input>보관소의 프레임</input>
            <output>경계 문자열, 헤더, JPEG 를 이어 붙인 바이트</output>
            <reason>브라우저가 연결을 끊거나 서버가 종료될 때까지 계속 보내기 위해 생성기로 만든다.</reason>
            """
            last_seq = -1
            while not SHUTDOWN.is_set():
                seq, data = ctx.store.wait(kind, last_seq, 1.0)
                if data is None:
                    continue
                last_seq = seq
                header = b"--frame\r\nContent-Type: image/jpeg\r\nContent-Length: %d\r\n\r\n" % len(data)
                yield header + data + b"\r\n"

        response = Response(generate(), mimetype="multipart/x-mixed-replace; boundary=frame")
        response.headers["Cache-Control"] = "no-cache, no-store"
        return response

    @app.get("/")
    def index():
        """
        <summary>대시보드 페이지를 보여 준다.</summary>
        <input>없음</input>
        <output>dashboard.html, 없으면 기본 안내 페이지</output>
        <reason>템플릿을 만들기 전에도 영상과 API 를 확인할 수 있게 하기 위해서다.</reason>
        """
        if (Path(app.template_folder) / "dashboard.html").exists():
            return render_template("dashboard.html")
        return Response(FALLBACK_PAGE, mimetype="text/html")

    @app.get("/detect_feed")
    def detect_feed():
        """
        <summary>상자가 그려진 결과 영상을 MJPEG 로 보낸다.</summary>
        <input>OutputStore 의 결과 JPEG</input>
        <output>MJPEG 스트림</output>
        <reason>대시보드의 결과 화면에 쓰기 위해서다.</reason>
        """
        return mjpeg_response("detect")

    @app.get("/raw_feed")
    def raw_feed():
        """
        <summary>ESP32 가 보낸 원본 영상을 MJPEG 로 다시 보낸다.</summary>
        <input>OutputStore 의 원본 JPEG</input>
        <output>MJPEG 스트림</output>
        <reason>ESP32 는 한 명에게만 스트림을 주므로, 원본도 서버를 거쳐 나눠 주어야 결과와 나란히 비교할 수 있다.</reason>
        """
        return mjpeg_response("raw")

    @app.get("/api/pipeline")
    def api_pipeline():
        """
        <summary>단계 설명과 현재 계측 상태를 한 번에 돌려준다.</summary>
        <input>after 쿼리 (이미 받은 로그 번호)</input>
        <output>metadata 와 snapshot 을 담은 JSON</output>
        <reason>대시보드가 처음 열릴 때 플로우차트 상자와 설명을 그리기 위해서다.</reason>
        """
        after = request.args.get("after", 0)
        return jsonify(
            {
                "metadata": PIPELINE.metadata(),
                "snapshot": PIPELINE.snapshot(after),
            }
        )

    @app.get("/api/config")
    def api_config():
        """
        <summary>적용된 설정과 실행 중 설정을 돌려준다.</summary>
        <input>없음</input>
        <output>설정 JSON</output>
        <reason>학생이 지금 어떤 주소, 모델, 장치로 실행 중인지 화면에서 확인하게 하기 위해서다.</reason>
        """
        detector = ctx.detector
        return jsonify(
            {
                "config": config.as_dict(),
                "runtime": ctx.settings.snapshot(),
                "use_yolo": ctx.use_yolo,
                "model": detector.model_name if detector is not None else None,
                "device": detector.device if detector is not None else None,
                "half": detector.half if detector is not None else False,
                "classes": ctx.classes,
                "limits": {
                    "skip": list(SKIP_RANGE),
                    "imgsz": list(IMGSZ_RANGE),
                    "conf": list(CONF_RANGE),
                },
            }
        )

    @app.post("/api/settings")
    def api_settings():
        """
        <summary>skip, imgsz, conf 를 검증한 뒤 실행 중에 바꾼다.</summary>
        <input>JSON 본문 (예: skip 3, conf 0.5)</input>
        <output>적용된 설정과 오류를 담은 JSON. 오류가 있으면 400</output>
        <reason>서버를 다시 켜지 않고 설정을 바꿔 가며 속도와 정확도의 변화를 바로 관찰하게 하기 위해서다.</reason>
        """
        body = request.get_json(silent=True)
        if body is None:
            body = request.form.to_dict()
        applied, errors = ctx.settings.update(body)
        if errors:
            return jsonify(
                {
                    "ok": False,
                    "settings": applied,
                    "errors": errors,
                }
            ), 400
        changes = ", ".join("%s=%s" % (key, applied[key]) for key in body)
        PIPELINE.log("INFO", "설정 변경: %s" % changes, always=True)
        PIPELINE.set_status(settings=applied)
        return jsonify(
            {
                "ok": True,
                "settings": applied,
                "errors": errors,
            }
        )

    @app.get("/events")
    def events():
        """
        <summary>SSE 로 계측 상태를 주기적으로 보낸다.</summary>
        <input>after 쿼리 또는 Last-Event-ID 헤더 (이어 받을 로그 번호)</input>
        <output>text/event-stream 응답</output>
        <reason>대시보드가 계속 묻지 않아도 서버가 200ms 마다 새 상태를 밀어 주어 플로우차트와 로그가 실시간으로 움직이게 하기 위해서다.</reason>
        """
        start = request.headers.get("Last-Event-ID") or request.args.get("after", 0)
        try:
            first_cursor = int(start)
        except (TypeError, ValueError):
            first_cursor = 0

        def stream():
            """
            <summary>연결마다 따로 둔 로그 커서로 snapshot 을 이어서 보낸다.</summary>
            <input>PIPELINE snapshot</input>
            <output>id 와 data 줄로 이루어진 SSE 메시지</output>
            <reason>브라우저 탭이 여러 개여도 각 탭이 놓친 로그 없이 자기 위치부터 받게 하기 위해서다.</reason>
            """
            cursor = first_cursor
            yield "retry: 2000\n\n"
            while not SHUTDOWN.is_set():
                snapshot = PIPELINE.snapshot(cursor)
                cursor = snapshot["last_seq"]
                yield "id: %d\ndata: %s\n\n" % (cursor, json.dumps(snapshot, ensure_ascii=False))
                time.sleep(config.SSE_INTERVAL_SEC)

        response = Response(stream(), mimetype="text/event-stream")
        response.headers["Cache-Control"] = "no-cache"
        response.headers["X-Accel-Buffering"] = "no"
        return response

    @app.post("/capture")
    def capture():
        """
        <summary>현재 결과 화면을 노트북의 captures 폴더에 JPEG 로 저장한다.</summary>
        <input>OutputStore 의 최신 결과 JPEG</input>
        <output>저장한 파일 이름과 주소 JSON. 아직 영상이 없으면 503</output>
        <reason>SD 카드를 쓰지 않으므로 인식 결과를 노트북에 남겨 보고서에 쓸 수 있게 하기 위해서다.</reason>
        """
        data = ctx.store.latest_detect()
        if data is None:
            return jsonify(
                {
                    "ok": False,
                    "error": "아직 처리된 영상이 없습니다",
                }
            ), 503
        capture_dir.mkdir(parents=True, exist_ok=True)
        name = datetime.now().strftime("capture_%Y%m%d_%H%M%S_%f")[:-3] + ".jpg"
        (capture_dir / name).write_bytes(data)
        PIPELINE.increment("captures")
        PIPELINE.log("INFO", "캡처 저장: %s" % name, stage="output", always=True)
        return jsonify(
            {
                "ok": True,
                "name": name,
                "url": "/captures/" + name,
                "size": len(data),
            }
        )

    @app.get("/api/captures")
    def api_captures():
        """
        <summary>저장된 캡처 목록을 최신순으로 돌려준다.</summary>
        <input>captures 폴더</input>
        <output>파일 이름, 주소, 크기, 저장 시각 목록 JSON (최대 100개)</output>
        <reason>대시보드에서 저장한 사진을 바로 다시 볼 수 있게 하기 위해서다.</reason>
        """
        items = []
        if capture_dir.exists():
            files = sorted(capture_dir.glob("*.jpg"), key=lambda path: path.stat().st_mtime, reverse=True)
            for path in files[:100]:
                stat = path.stat()
                items.append(
                    {
                        "name": path.name,
                        "url": "/captures/" + path.name,
                        "size": stat.st_size,
                        "time": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                    }
                )
        return jsonify(items)

    @app.get("/captures/<path:filename>")
    def captures(filename):
        """
        <summary>captures 폴더의 파일 하나를 보내 준다.</summary>
        <param name="filename">요청한 파일 이름</param>
        <input>파일 이름</input>
        <output>이미지 파일. 폴더 밖을 가리키거나 없으면 404</output>
        <reason>send_from_directory 는 상위 폴더로 빠져나가는 경로를 막아 주므로 노트북의 다른 파일이 새어 나가지 않는다.</reason>
        """
        return send_from_directory(capture_dir.resolve(), filename)

    return app


# ---------------------------------------------------------------------------
# 시작과 종료
# ---------------------------------------------------------------------------
def parse_args():
    """
    <summary>명령줄 옵션을 읽는다. 기본값은 config.py 에서 가져온다.</summary>
    <input>sys.argv</input>
    <output>argparse 의 Namespace</output>
    <reason>설정 파일을 고치지 않고 실행할 때마다 주소, 장치, 모델을 바꿔 실험하기 위해서다.</reason>
    """
    parser = argparse.ArgumentParser(description="ESP32 + YOLO 파이프라인 대시보드")
    parser.add_argument("--source", choices=("esp32", "webcam"), default=config.SOURCE, help="영상 소스")
    parser.add_argument("--url", default=config.CAMERA_URL, help="ESP32 스트림 주소")
    parser.add_argument("--webcam-index", type=int, default=config.WEBCAM_INDEX, help="웹캠 번호")
    parser.add_argument("--device", default=config.DEVICE, help="cpu, cuda, cuda:0, mps")
    parser.add_argument("--imgsz", type=int, default=config.IMG_SIZE, help="추론 입력 크기 (32의 배수)")
    parser.add_argument("--conf", type=float, default=config.CONF, help="신뢰도 임계값")
    parser.add_argument("--skip", type=int, default=config.SKIP, help="몇 프레임마다 추론할지")
    parser.add_argument("--model", default=config.MODEL_PATH, help="모델 파일")
    parser.add_argument("--no-yolo", action="store_true", help="추론 없이 수신과 표시만 (무선 지연 관찰용)")
    parser.add_argument("--classes", default="", help="검출할 클래스, 쉼표로 구분 (예: person,cup 또는 0,41)")
    parser.add_argument("--host", default=config.HOST, help="서버 주소")
    parser.add_argument("--port", type=int, default=config.PORT, help="서버 포트")
    parser.add_argument("--log-every", type=int, default=config.LOG_EVERY, help="INFO, DEBUG 로그 프레임 간격")
    return parser.parse_args()


def apply_args(args):
    """
    <summary>명령줄 옵션을 검증해 config 모듈 값에 반영한다.</summary>
    <param name="args">parse_args 결과</param>
    <input>명령줄 옵션</input>
    <output>검증된 RuntimeSettings 초기값 사전</output>
    <reason>대시보드의 설정 화면이 실제로 적용된 값을 보여 주도록 config 를 한 곳의 기준으로 맞추기 위해서다.</reason>
    """
    config.SOURCE = args.source
    config.CAMERA_URL = args.url
    config.WEBCAM_INDEX = args.webcam_index
    config.DEVICE = args.device
    config.MODEL_PATH = args.model
    config.HOST = args.host
    config.PORT = args.port
    config.LOG_EVERY = max(1, args.log_every)
    config.SKIP = min(SKIP_RANGE[1], max(SKIP_RANGE[0], args.skip))
    config.IMG_SIZE = min(IMGSZ_RANGE[1], max(IMGSZ_RANGE[0], (args.imgsz // 32) * 32))
    config.CONF = min(CONF_RANGE[1], max(CONF_RANGE[0], args.conf))
    PIPELINE.set_log_every(config.LOG_EVERY)
    return {
        "skip": config.SKIP,
        "imgsz": config.IMG_SIZE,
        "conf": config.CONF,
    }


def make_reader():
    """
    <summary>설정된 소스에 맞는 리더를 만든다.</summary>
    <input>config.SOURCE</input>
    <output>MjpegStreamReader 또는 WebcamReader</output>
    <reason>두 리더의 사용법이 같으므로 여기서만 고르면 나머지 코드는 소스를 신경 쓰지 않아도 된다.</reason>
    """
    if config.SOURCE == "webcam":
        return WebcamReader(config.WEBCAM_INDEX, config.RECONNECT_DELAY_SEC)
    return MjpegStreamReader(config.CAMERA_URL, config.STREAM_TIMEOUT_SEC, config.RECONNECT_DELAY_SEC)


def load_detector(args, imgsz):
    """
    <summary>YOLO 모델을 불러오고 워밍업한다. 실패하면 None 을 돌려준다.</summary>
    <param name="args">명령줄 옵션</param>
    <param name="imgsz">워밍업 입력 크기</param>
    <input>모델 설정</input>
    <output>Detector 또는 None</output>
    <reason>모델이 없어도 서버는 켜져서 무선 영상만이라도 확인할 수 있게 하기 위해서다.</reason>
    """
    classes = [item for item in args.classes.split(",") if item.strip()]
    try:
        detector = Detector(config.MODEL_PATH, config.FALLBACK_MODEL_PATH, config.DEVICE, classes)
    except Exception as exc:
        logger.error("YOLO 를 사용할 수 없습니다: %s", exc)
        PIPELINE.log("WARN", "YOLO 를 사용할 수 없어 추론 없이 실행: %s" % exc, stage="inference")
        return None
    warmup_ms = detector.warmup(imgsz)
    logger.info("워밍업 %.0fms (model=%s, device=%s, half=%s)", warmup_ms, detector.model_name, detector.device, detector.half)
    PIPELINE.log(
        "INFO",
        "모델 준비: %s, %s, 워밍업 %.0fms" % (detector.model_name, detector.device, warmup_ms),
        stage="inference",
        always=True,
    )
    return detector


def main():
    """
    <summary>설정을 읽고 리더, 모델, 처리 스레드, 웹 서버를 차례로 시작한다. 종료 시 모두 정리한다.</summary>
    <input>명령줄 옵션</input>
    <output>없음</output>
    <reason>Ctrl+C 를 눌러도 스레드와 카메라 연결이 남지 않게 finally 에서 한 번에 정리하기 위해서다.</reason>
    """
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    # 영상 피드와 SSE 요청 로그가 콘솔을 덮지 않게 한다.
    logging.getLogger("werkzeug").setLevel(logging.WARNING)

    args = parse_args()
    initial = apply_args(args)
    Path(config.CAPTURE_DIR).mkdir(parents=True, exist_ok=True)
    classes = [item.strip() for item in args.classes.split(",") if item.strip()]

    detector = None
    if not args.no_yolo:
        detector = load_detector(args, initial["imgsz"])
    use_yolo = detector is not None

    settings = RuntimeSettings(initial["skip"], initial["imgsz"], initial["conf"])
    store = OutputStore(config.JPEG_QUALITY)
    reader = make_reader()
    processor = Processor(reader, detector, settings, store, use_yolo)

    PIPELINE.set_status(
        source=config.SOURCE,
        camera_url=config.CAMERA_URL if config.SOURCE == "esp32" else "webcam:%d" % config.WEBCAM_INDEX,
        camera="starting",
        yolo=use_yolo,
        model=detector.model_name if detector is not None else None,
        device=detector.device if detector is not None else None,
        settings=settings.snapshot(),
    )

    reader.start()
    processor.start()
    app = create_app(AppContext(reader, processor, detector, settings, store, use_yolo, classes))

    shown_host = "127.0.0.1" if config.HOST in ("0.0.0.0", "") else config.HOST
    logger.info("소스: %s", config.CAMERA_URL if config.SOURCE == "esp32" else "webcam %d" % config.WEBCAM_INDEX)
    logger.info("YOLO: %s", "사용" if use_yolo else "끔")
    logger.info("대시보드: http://%s:%d", shown_host, config.PORT)
    try:
        app.run(host=config.HOST, port=config.PORT, threaded=True, use_reloader=False)
    except KeyboardInterrupt:
        pass
    finally:
        logger.info("종료 중...")
        SHUTDOWN.set()
        processor.stop()
        reader.stop()
        processor.join(timeout=config.STREAM_TIMEOUT_SEC + 1.0)
        reader.join(timeout=config.STREAM_TIMEOUT_SEC + 1.0)
        logger.info("종료 완료")


if __name__ == "__main__":
    main()
