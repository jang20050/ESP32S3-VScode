"""
파이프라인 계측 모듈.

영상 한 장이 캡처부터 출력까지 지나가는 여섯 단계의 시간, 입출력 모양, 로그를 모은다.
대시보드는 snapshot() 결과로 플로우차트와 로그를 그리고,
metadata() 의 docstring 설명으로 각 단계 상자의 설명을 채운다.

이 모듈은 cv2 와 ultralytics 를 import 하지 않는다. 계측은 가볍고 어디서나 불러올 수 있어야 한다.

사용 예)
    from pipeline import PIPELINE, stage

    @stage("inference")
    def run_model(image):
        ...

    PIPELINE.begin_frame()
    ...
    PIPELINE.end_frame()
"""

import functools
import inspect
import logging
import math
import threading
import time
import xml.etree.ElementTree as ET
from collections import deque
from datetime import datetime

try:
    from . import config
except ImportError:
    import config

logger = logging.getLogger("pipeline")

# ---------------------------------------------------------------------------
# 단계 정의
# ---------------------------------------------------------------------------
STAGES = (
    {
        "id": "capture",
        "label": "캡처",
        "name": "Capture",
    },
    {
        "id": "preprocess",
        "label": "전처리",
        "name": "Preprocess",
    },
    {
        "id": "inference",
        "label": "추론",
        "name": "Inference",
    },
    {
        "id": "postprocess",
        "label": "후처리",
        "name": "Postprocess",
    },
    {
        "id": "visualize",
        "label": "시각화",
        "name": "Visualize",
    },
    {
        "id": "output",
        "label": "출력",
        "name": "Output",
    },
)

STAGE_IDS = tuple(item["id"] for item in STAGES)
STAGE_LABELS = dict((item["id"], item["label"]) for item in STAGES)

LEVELS = ("INFO", "DEBUG", "WARN")
DOC_TAGS = ("summary", "input", "output", "reason")

# 이 시간(초) 동안 프레임이 끝나지 않으면 FPS 를 0 으로 보여 준다.
IDLE_FPS_RESET_SEC = 2.0

# 입출력 요약 문자열의 최대 길이
SUMMARY_MAX_CHARS = 120

# throttle 키를 이 개수 이상 기억하지 않는다.
THROTTLE_KEY_LIMIT = 256


# ---------------------------------------------------------------------------
# docstring 파싱
# ---------------------------------------------------------------------------
class DocstringParseError(ValueError):
    """
    <summary>XML docstring 을 해석하지 못했을 때 쓰는 예외.</summary>
    <input>파싱 실패 사유 문자열</input>
    <output>ValueError 를 상속한 예외 객체</output>
    <reason>파싱 실패를 다른 오류와 구분해, 해당 단계만 이름으로 표시하고 서버는 계속 돌리기 위해서다.</reason>
    """


def _normalize_space(text):
    """
    <summary>줄바꿈과 연속된 공백을 공백 한 칸으로 바꾼다.</summary>
    <param name="text">원본 문자열 (None 이어도 된다)</param>
    <input>여러 줄에 걸친 설명 문자열</input>
    <output>한 줄로 정리된 문자열</output>
    <reason>docstring 의 들여쓰기와 줄바꿈이 대시보드 설명 상자에 그대로 보이지 않게 하기 위해서다.</reason>
    """
    return " ".join((text or "").split())


def parse_xml_docstring(doc):
    """
    <summary>XML 형식 docstring 을 읽어 요약, 입력, 출력, 이유, 매개변수 설명으로 나눈다.</summary>
    <param name="doc">inspect.getdoc 으로 얻은 docstring 문자열</param>
    <input>summary, param, input, output, reason 태그가 들어 있는 문자열</input>
    <output>summary, input, output, reason, params 키를 가진 사전. params 는 name 과 text 를 가진 사전의 목록</output>
    <reason>코드의 설명을 한 곳에만 쓰고, 그것을 대시보드 플로우차트와 검사 도구가 함께 읽게 하기 위해서다.</reason>
    """
    if doc is None or doc.strip() == "":
        raise DocstringParseError("docstring 이 비어 있음")
    try:
        root = ET.fromstring("<doc>" + doc + "</doc>")
    except ET.ParseError as exc:
        raise DocstringParseError("XML 파싱 실패: %s" % exc) from exc

    result = {
        "summary": "",
        "input": "",
        "output": "",
        "reason": "",
        "params": [],
    }
    for tag in DOC_TAGS:
        element = root.find(tag)
        if element is not None:
            result[tag] = _normalize_space("".join(element.itertext()))
    for element in root.findall("param"):
        result["params"].append(
            {
                "name": element.get("name", ""),
                "text": _normalize_space("".join(element.itertext())),
            }
        )
    return result


def describe_function(func):
    """
    <summary>함수의 docstring 을 읽어 파싱 결과와 오류 메시지를 함께 돌려준다.</summary>
    <param name="func">설명을 읽을 함수 또는 메서드</param>
    <input>파이썬 함수 객체</input>
    <output>(파싱 결과 사전 또는 None, 오류 문자열 또는 None) 튜플</output>
    <reason>파싱 실패를 예외 대신 값으로 받아, 호출하는 쪽이 라벨만 보여 주는 식으로 부드럽게 처리하게 하기 위해서다.</reason>
    """
    try:
        return parse_xml_docstring(inspect.getdoc(func)), None
    except DocstringParseError as exc:
        return None, str(exc)


# ---------------------------------------------------------------------------
# 입출력 요약
# ---------------------------------------------------------------------------
def _truncate(text):
    """
    <summary>너무 긴 문자열을 정해진 길이로 자른다.</summary>
    <param name="text">원본 문자열</param>
    <input>요약 문자열</input>
    <output>최대 SUMMARY_MAX_CHARS 글자의 문자열</output>
    <reason>로그 한 줄이 화면을 덮지 않게 하기 위해서다.</reason>
    """
    if len(text) > SUMMARY_MAX_CHARS:
        return text[:SUMMARY_MAX_CHARS - 3] + "..."
    return text


def summarize_value(value):
    """
    <summary>값 하나를 모양, 자료형, 개수 중심의 짧은 문자열로 요약한다.</summary>
    <param name="value">요약할 값. 배열, 바이트, 목록, 숫자 등 무엇이든 된다</param>
    <input>단계 함수의 인자나 반환값</input>
    <output>예를 들어 (480,640,3) uint8 또는 5 items 같은 문자열</output>
    <reason>영상 데이터 자체를 로그에 남길 수는 없으므로, 각 단계에서 데이터 모양이 어떻게 바뀌는지만 보여 주기 위해서다.</reason>
    """
    if value is None:
        return "None"
    shape = getattr(value, "shape", None)
    dtype = getattr(value, "dtype", None)
    if shape is not None and dtype is not None:
        try:
            dims = ",".join(str(int(size)) for size in shape)
        except TypeError:
            dims = str(shape)
        return "(%s) %s" % (dims, dtype)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "%d bytes" % len(value)
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return "%.3g" % value
    if isinstance(value, str):
        return "str(%d)" % len(value)
    if isinstance(value, dict):
        return "dict(%d keys)" % len(value)
    if isinstance(value, tuple) and 0 < len(value) <= 4:
        return ", ".join(summarize_value(item) for item in value)
    if isinstance(value, (list, tuple, set, frozenset, deque)):
        return "%d items" % len(value)
    try:
        return "%s(%d)" % (type(value).__name__, len(value))
    except TypeError:
        return type(value).__name__


def _summarize_args(args, kwargs, skip_first):
    """
    <summary>함수 호출 인자 전체를 한 줄로 요약한다.</summary>
    <param name="args">위치 인자 튜플</param>
    <param name="kwargs">키워드 인자 사전</param>
    <param name="skip_first">첫 인자가 self 나 cls 이면 True</param>
    <input>단계 함수에 들어온 인자</input>
    <output>쉼표로 이어 붙인 요약 문자열. 인자가 없으면 -</output>
    <reason>단계에 무엇이 들어갔는지 DEBUG 로그로 보여 주기 위해서다.</reason>
    """
    start = 1 if skip_first else 0
    parts = [summarize_value(item) for item in args[start:]]
    for key, item in kwargs.items():
        parts.append("%s=%s" % (key, summarize_value(item)))
    if not parts:
        return "-"
    return _truncate(", ".join(parts))


def _as_summary(value):
    """
    <summary>직접 기록할 때 받은 입출력 설명을 문자열로 맞춘다.</summary>
    <param name="value">문자열 설명이나 요약할 값</param>
    <input>record 에 전달된 inputs 또는 outputs</input>
    <output>요약 문자열. 값이 없으면 -</output>
    <reason>manual 단계가 5 boxes 처럼 직접 쓴 설명과 배열 같은 값을 모두 받을 수 있게 하기 위해서다.</reason>
    """
    if value is None:
        return "-"
    if isinstance(value, str):
        return _truncate(value)
    return _truncate(summarize_value(value))


def _to_jsonable(value):
    """
    <summary>값을 JSON 으로 보낼 수 있는 기본 파이썬 자료형으로 바꾼다.</summary>
    <param name="value">상태값으로 받은 임의의 값</param>
    <input>numpy 숫자, 사전, 목록 등</input>
    <output>dict, list, str, int, float, bool, None 으로만 이루어진 값</output>
    <reason>numpy 타입이나 NaN 이 섞이면 SSE 로 보내는 JSON 직렬화가 실패하기 때문이다.</reason>
    """
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return round(value, 3)
    if isinstance(value, dict):
        return dict((str(key), _to_jsonable(item)) for key, item in value.items())
    if isinstance(value, (list, tuple, set, frozenset, deque)):
        return [_to_jsonable(item) for item in value]
    if hasattr(value, "tolist"):
        try:
            return _to_jsonable(value.tolist())
        except (TypeError, ValueError):
            return str(value)
    return str(value)


def _normalize_level(level):
    """
    <summary>로그 레벨 이름을 INFO, DEBUG, WARN 중 하나로 맞춘다.</summary>
    <param name="level">레벨 이름 (대소문자 무관, WARNING 도 허용)</param>
    <input>문자열</input>
    <output>정규화된 레벨 문자열</output>
    <reason>호출하는 쪽의 사소한 표기 차이 때문에 로그가 빠지지 않게 하기 위해서다.</reason>
    """
    name = str(level).upper()
    if name in ("WARNING", "ERROR"):
        name = "WARN"
    if name not in LEVELS:
        raise ValueError("알 수 없는 로그 레벨: %s" % level)
    return name


# ---------------------------------------------------------------------------
# 계측기
# ---------------------------------------------------------------------------
class Pipeline:
    """
    <summary>프레임 단위로 단계별 시간, 입출력, 로그, 카운터, 상태값을 모으는 계측기.</summary>
    <input>stage 데코레이터와 begin_frame, end_frame 호출</input>
    <output>snapshot 과 metadata 가 돌려주는 JSON 직렬화 가능한 사전</output>
    <reason>영상 처리 스레드와 Flask 스레드가 같은 상태를 안전하게 나눠 쓰도록 모든 상태를 lock 하나로 보호하기 위해서다.</reason>
    """

    def __init__(self, log_maxlen=500, stat_window=30, sse_max_logs=60, log_every=5):
        """
        <summary>링버퍼와 통계 저장소를 준비한다.</summary>
        <param name="log_maxlen">보관할 로그 최대 개수</param>
        <param name="stat_window">평균과 FPS 계산에 쓰는 최근 프레임 수</param>
        <param name="sse_max_logs">snapshot 한 번에 돌려주는 로그 최대 개수</param>
        <param name="log_every">INFO, DEBUG 로그를 남기는 프레임 간격</param>
        <input>설정값</input>
        <output>빈 계측기 객체</output>
        <reason>오래된 기록은 자동으로 밀려나게 deque 를 써서 메모리가 늘어나지 않게 하기 위해서다.</reason>
        """
        self._lock = threading.RLock()
        self._stat_window = max(1, int(stat_window))
        self._sse_max_logs = max(1, int(sse_max_logs))
        self._log_every = max(1, int(log_every))

        # 단계별 함수 정보와 docstring
        self._registry = dict()

        # 로그 링버퍼
        self._events = deque(maxlen=max(1, int(log_maxlen)))
        self._seq = 0
        self._throttle_counts = dict()

        # 진행 중인 프레임
        self._frame_id = 0
        self._frame_open = False
        self._frame_start = 0.0
        self._frame_ms = dict()
        self._frame_calls = dict()
        self._frame_io = dict()
        self._active_stage = None

        # 끝난 프레임 기록
        self._last_skipped = False
        self._last_frame_ms = dict()
        self._history = dict((stage_id, deque(maxlen=self._stat_window)) for stage_id in STAGE_IDS)
        self._run_history = dict((stage_id, deque(maxlen=self._stat_window)) for stage_id in STAGE_IDS)
        self._total_history = deque(maxlen=self._stat_window)
        self._end_times = deque(maxlen=self._stat_window + 1)

        self._counters = {
            "frames": 0,
            "inferences": 0,
            "skipped": 0,
            "dropped": 0,
            "errors": 0,
        }
        self._status = dict()
        self._stats = self._compute_stats()

    # ------------------------------------------------------------------
    # 단계 등록
    # ------------------------------------------------------------------
    def stage(self, stage_id, manual=False):
        """
        <summary>함수를 파이프라인 단계로 등록하고, 호출될 때마다 시간과 입출력을 기록하게 만드는 데코레이터를 만든다.</summary>
        <param name="stage_id">capture, preprocess, inference, postprocess, visualize, output 중 하나</param>
        <param name="manual">True 이면 시간 측정만 하고 기록은 함수가 PIPELINE.record 로 직접 한다</param>
        <input>단계 이름과 기록 방식</input>
        <output>함수를 감싸는 데코레이터</output>
        <reason>처리 코드 안에 계측 코드를 섞지 않고, 함수 위에 한 줄만 붙여 단계를 표시하게 하기 위해서다.</reason>
        """
        if stage_id not in STAGE_IDS:
            raise ValueError("알 수 없는 단계: %s (가능: %s)" % (stage_id, ", ".join(STAGE_IDS)))

        def decorator(func):
            """
            <summary>함수의 docstring 을 등록하고 계측 래퍼로 감싼다.</summary>
            <param name="func">단계로 등록할 함수</param>
            <input>원래 함수</input>
            <output>원래 함수와 이름과 docstring 이 같은 래퍼 함수</output>
            <reason>대시보드가 함수 설명을 읽을 수 있도록 데코레이터가 붙는 순간 한 번만 파싱하기 위해서다.</reason>
            """
            skip_first = self._register(stage_id, func)

            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                """
                <summary>원래 함수를 실행하면서 활성 단계 표시, 실행 시간, 입출력 요약을 기록한다.</summary>
                <param name="args">원래 함수에 전달할 위치 인자</param>
                <param name="kwargs">원래 함수에 전달할 키워드 인자</param>
                <input>원래 함수의 인자</input>
                <output>원래 함수의 반환값</output>
                <reason>모든 단계가 같은 방식으로 측정되어야 단계끼리 시간을 공정하게 비교할 수 있기 때문이다.</reason>
                """
                self._enter_stage(stage_id)
                started = time.perf_counter()
                try:
                    result = func(*args, **kwargs)
                except Exception as exc:
                    elapsed_ms = (time.perf_counter() - started) * 1000.0
                    self.record(stage_id, elapsed_ms, _summarize_args(args, kwargs, skip_first), "error")
                    self.increment("errors")
                    self.log("WARN", "%s 단계 예외: %s: %s" % (stage_id, type(exc).__name__, exc), stage=stage_id)
                    raise
                elapsed_ms = (time.perf_counter() - started) * 1000.0
                if manual:
                    self._leave_stage(stage_id)
                else:
                    self.record(stage_id, elapsed_ms, _summarize_args(args, kwargs, skip_first), summarize_value(result))
                return result

            return wrapper

        return decorator

    def _register(self, stage_id, func):
        """
        <summary>단계와 함수 정보를 등록하고 docstring 을 파싱해 둔다.</summary>
        <param name="stage_id">단계 이름</param>
        <param name="func">등록할 함수</param>
        <input>함수 객체</input>
        <output>첫 인자가 self 나 cls 라서 요약에서 빼야 하면 True</output>
        <reason>파싱 실패를 등록 시점에 한 번만 경고해 같은 경고가 반복되지 않게 하기 위해서다.</reason>
        """
        doc, error = describe_function(func)
        try:
            names = list(inspect.signature(func).parameters)
        except (TypeError, ValueError):
            names = []
        skip_first = bool(names) and names[0] in ("self", "cls")
        qualname = "%s.%s" % (func.__module__, func.__qualname__)

        with self._lock:
            previous = self._registry.get(stage_id)
            if previous is not None and previous["function"] != qualname:
                logger.warning("단계 %s 에 %s 대신 %s 를 등록합니다.", stage_id, previous["function"], qualname)
            self._registry[stage_id] = {
                "function": qualname,
                "doc": doc,
                "error": error,
            }
            if error is not None:
                logger.warning("%s (%s) docstring 파싱 실패: %s", stage_id, qualname, error)
                self._append_event("WARN", stage_id, "docstring 파싱 실패 (%s): %s" % (qualname, error))
        return skip_first

    def _enter_stage(self, stage_id):
        """
        <summary>현재 실행 중인 단계를 표시한다.</summary>
        <param name="stage_id">시작하는 단계 이름</param>
        <input>단계 이름</input>
        <output>없음</output>
        <reason>대시보드 플로우차트에서 지금 실행 중인 상자를 강조하기 위해서다.</reason>
        """
        with self._lock:
            self._active_stage = stage_id

    def _leave_stage(self, stage_id):
        """
        <summary>단계가 끝났으면 활성 표시를 지운다.</summary>
        <param name="stage_id">끝난 단계 이름</param>
        <input>단계 이름</input>
        <output>없음</output>
        <reason>다른 단계가 이미 시작했다면 그 표시는 지우지 않기 위해 이름이 같을 때만 지운다.</reason>
        """
        with self._lock:
            if self._active_stage == stage_id:
                self._active_stage = None

    # ------------------------------------------------------------------
    # 프레임 경계
    # ------------------------------------------------------------------
    def begin_frame(self):
        """
        <summary>새 프레임의 계측을 시작한다.</summary>
        <input>없음</input>
        <output>새 프레임 번호</output>
        <reason>여러 단계의 시간을 한 프레임으로 묶어야 프레임당 평균과 병목을 계산할 수 있기 때문이다.</reason>
        """
        with self._lock:
            if self._frame_open:
                self._discard_frame("이전 프레임이 끝나지 않은 채 새 프레임 시작")
            self._frame_id += 1
            self._frame_open = True
            self._frame_start = time.perf_counter()
            self._frame_ms = dict()
            self._frame_calls = dict()
            self._frame_io = dict()
            self._active_stage = None
            return self._frame_id

    def end_frame(self, inference_skipped=None):
        """
        <summary>프레임을 마감하고 단계별 평균, 추론 실행 시 평균, FPS 를 다시 계산한다.</summary>
        <param name="inference_skipped">추론을 건너뛰었는지 여부. None 이면 이번 프레임에 inference 기록이 없을 때 건너뛴 것으로 본다</param>
        <input>이번 프레임에 쌓인 단계별 시간</input>
        <output>프레임 전체 처리 시간(ms). 열린 프레임이 없으면 None</output>
        <reason>통계를 프레임이 끝날 때 한 번만 계산해 두면 대시보드가 자주 물어봐도 부담이 없기 때문이다.</reason>
        """
        with self._lock:
            if not self._frame_open:
                return None
            now = time.perf_counter()
            total_ms = (now - self._frame_start) * 1000.0
            ran_inference = "inference" in self._frame_ms
            if inference_skipped is None:
                skipped = not ran_inference
            else:
                skipped = bool(inference_skipped)

            for stage_id in STAGE_IDS:
                elapsed = self._frame_ms.get(stage_id, 0.0)
                self._history[stage_id].append(elapsed)
                if stage_id in self._frame_ms:
                    self._run_history[stage_id].append(elapsed)
            self._last_frame_ms = dict(self._frame_ms)
            self._total_history.append(total_ms)
            self._end_times.append(now)

            self._counters["frames"] += 1
            if ran_inference:
                self._counters["inferences"] += 1
            if skipped:
                self._counters["skipped"] += 1
            self._last_skipped = skipped
            self._stats = self._compute_stats()

            if self._frame_id % self._log_every == 0:
                for stage_id in STAGE_IDS:
                    if stage_id not in self._frame_ms:
                        continue
                    calls = self._frame_calls.get(stage_id, 1)
                    suffix = " (x%d)" % calls if calls > 1 else ""
                    self._append_event(
                        "INFO",
                        stage_id,
                        "#frame %04d | %s | %.1fms%s" % (self._frame_id, stage_id, self._frame_ms[stage_id], suffix),
                    )
                    inputs, outputs = self._frame_io.get(stage_id, ("-", "-"))
                    self._append_event("DEBUG", stage_id, "in=%s out=%s" % (inputs, outputs))
                self._append_event("INFO", None, "#frame %04d | total | %.1fms" % (self._frame_id, total_ms))

            self._close_frame()
            return round(total_ms, 2)

    def abort_frame(self, reason=""):
        """
        <summary>현재 프레임을 통계에 넣지 않고 버린다.</summary>
        <param name="reason">버리는 이유 (예: 프레임 수신 실패)</param>
        <input>진행 중인 프레임</input>
        <output>없음</output>
        <reason>깨진 프레임의 시간이 평균에 섞이면 병목 판단이 틀어지므로 드롭으로만 집계하기 위해서다.</reason>
        """
        with self._lock:
            self._discard_frame(reason or "이유 없음")

    def _discard_frame(self, reason):
        """
        <summary>프레임을 드롭으로 집계하고 경고 로그를 남긴 뒤 닫는다.</summary>
        <param name="reason">드롭 이유</param>
        <input>진행 중인 프레임 상태</input>
        <output>없음</output>
        <reason>begin_frame 과 abort_frame 이 같은 방식으로 드롭을 처리하게 하기 위해서다.</reason>
        """
        self._counters["dropped"] += 1
        self._append_event("WARN", self._active_stage, "#frame %04d | 프레임 드롭 | %s" % (self._frame_id, reason))
        self._close_frame()

    def _close_frame(self):
        """
        <summary>진행 중인 프레임 상태를 비운다.</summary>
        <input>진행 중인 프레임 상태</input>
        <output>없음</output>
        <reason>다음 프레임에 이전 기록이 섞이지 않게 하기 위해서다.</reason>
        """
        self._frame_open = False
        self._frame_ms = dict()
        self._frame_calls = dict()
        self._frame_io = dict()
        self._active_stage = None

    # ------------------------------------------------------------------
    # 기록
    # ------------------------------------------------------------------
    def record(self, stage_id, ms, inputs=None, outputs=None):
        """
        <summary>단계 하나의 실행 시간과 입출력 요약을 현재 프레임에 더한다.</summary>
        <param name="stage_id">단계 이름</param>
        <param name="ms">실행 시간(ms)</param>
        <param name="inputs">입력 설명 문자열 또는 요약할 값</param>
        <param name="outputs">출력 설명 문자열 또는 요약할 값</param>
        <input>단계 실행 결과</input>
        <output>없음</output>
        <reason>같은 프레임에서 한 단계가 여러 번 불려도 프레임당 시간이 정확하도록 합산하기 위해서다.</reason>
        """
        if stage_id not in STAGE_IDS:
            raise ValueError("알 수 없는 단계: %s" % stage_id)
        with self._lock:
            if self._frame_open:
                self._frame_ms[stage_id] = self._frame_ms.get(stage_id, 0.0) + float(ms)
                self._frame_calls[stage_id] = self._frame_calls.get(stage_id, 0) + 1
                self._frame_io[stage_id] = (_as_summary(inputs), _as_summary(outputs))
            if self._active_stage == stage_id:
                self._active_stage = None

    def log(self, level, message, stage=None, throttle=False, key=None, always=False):
        """
        <summary>로그 한 줄을 링버퍼에 남긴다.</summary>
        <param name="level">INFO, DEBUG, WARN 중 하나</param>
        <param name="message">로그 내용</param>
        <param name="stage">관련 단계 이름 (없으면 None)</param>
        <param name="throttle">True 이면 같은 로그를 LOG_EVERY 번에 한 번만 남긴다</param>
        <param name="key">throttle 에서 같은 로그로 볼 기준. 없으면 레벨, 단계, 내용으로 정한다</param>
        <param name="always">True 이면 프레임 간격 규칙을 무시하고 항상 남긴다. 설정 변경처럼 드문 사건에 쓴다</param>
        <input>로그 정보</input>
        <output>남긴 로그의 일련번호. 걸러졌으면 None</output>
        <reason>INFO 와 DEBUG 를 매 프레임 남기면 읽을 수 없는 속도로 흐르므로 간격을 두고, WARN 은 놓치지 않게 항상 남기기 위해서다.</reason>
        """
        level = _normalize_level(level)
        with self._lock:
            if always:
                return self._append_event(level, stage, message)
            if throttle:
                throttle_key = key if key is not None else (level, stage, message)
                if len(self._throttle_counts) >= THROTTLE_KEY_LIMIT and throttle_key not in self._throttle_counts:
                    self._throttle_counts.clear()
                count = self._throttle_counts.get(throttle_key, 0) + 1
                self._throttle_counts[throttle_key] = count
                if (count - 1) % self._log_every != 0:
                    return None
                if count > 1:
                    message = "%s (누적 %d회)" % (message, count)
            elif level in ("INFO", "DEBUG") and self._frame_open:
                if self._frame_id % self._log_every != 0:
                    return None
            return self._append_event(level, stage, message)

    def _append_event(self, level, stage, message):
        """
        <summary>로그 사전을 만들어 링버퍼 끝에 넣는다.</summary>
        <param name="level">로그 레벨</param>
        <param name="stage">관련 단계 이름 또는 None</param>
        <param name="message">로그 내용</param>
        <input>로그 정보</input>
        <output>새 로그의 일련번호</output>
        <reason>대시보드가 마지막으로 받은 번호 이후의 로그만 이어 받을 수 있도록 번호를 붙이기 위해서다.</reason>
        """
        self._seq += 1
        now = datetime.now()
        self._events.append(
            {
                "seq": self._seq,
                "ts": round(time.time(), 3),
                "time": now.strftime("%H:%M:%S.") + "%03d" % (now.microsecond // 1000),
                "level": level,
                "stage": stage,
                "label": STAGE_LABELS.get(stage),
                "frame": self._frame_id,
                "msg": str(message),
            }
        )
        return self._seq

    def set_status(self, **values):
        """
        <summary>연결 상태, 모델 이름 같은 상태값을 저장한다.</summary>
        <param name="values">저장할 이름과 값 (예: camera="connected", model="yolo26n.pt")</param>
        <input>키워드 인자</input>
        <output>없음</output>
        <reason>파이프라인 바깥의 상태도 snapshot 한 번으로 대시보드에 함께 보내기 위해서다.</reason>
        """
        with self._lock:
            for name, value in values.items():
                self._status[name] = _to_jsonable(value)

    def set_log_every(self, log_every):
        """
        <summary>INFO, DEBUG 로그를 남기는 프레임 간격을 바꾼다.</summary>
        <param name="log_every">새 간격 (1 이상)</param>
        <input>정수</input>
        <output>없음</output>
        <reason>명령줄 옵션으로 수업 중 로그 양을 조절하기 위해서다.</reason>
        """
        with self._lock:
            self._log_every = max(1, int(log_every))

    def increment(self, name, amount=1):
        """
        <summary>이름이 붙은 카운터를 늘린다.</summary>
        <param name="name">카운터 이름 (예: reconnects)</param>
        <param name="amount">늘릴 양</param>
        <input>카운터 이름과 증가량</input>
        <output>늘린 뒤의 값</output>
        <reason>재접속 횟수처럼 누적해서 봐야 하는 사건을 세기 위해서다.</reason>
        """
        with self._lock:
            self._counters[name] = self._counters.get(name, 0) + int(amount)
            return self._counters[name]

    # ------------------------------------------------------------------
    # 조회
    # ------------------------------------------------------------------
    def _compute_stats(self):
        """
        <summary>최근 프레임 기록으로 단계별 평균, 비중, 병목, FPS 를 계산한다.</summary>
        <input>단계별 시간 기록과 프레임 종료 시각</input>
        <output>반올림된 숫자만 담긴 통계 사전</output>
        <reason>end_frame 에서 미리 계산해 두고 snapshot 은 복사만 하게 하기 위해서다.</reason>
        """
        stages = []
        total_avg = 0.0
        for item in STAGES:
            stage_id = item["id"]
            history = self._history[stage_id]
            run_history = self._run_history[stage_id]
            avg_ms = sum(history) / len(history) if history else 0.0
            run_avg_ms = sum(run_history) / len(run_history) if run_history else 0.0
            total_avg += avg_ms
            stages.append(
                {
                    "id": stage_id,
                    "label": item["label"],
                    "avg_ms": avg_ms,
                    "run_avg_ms": run_avg_ms,
                    "last_ms": self._last_frame_ms.get(stage_id, 0.0),
                    "ran_last": stage_id in self._last_frame_ms,
                    "share": 0.0,
                }
            )

        bottleneck = None
        best = 0.0
        for entry in stages:
            if total_avg > 0:
                entry["share"] = entry["avg_ms"] / total_avg
            if entry["avg_ms"] > best:
                best = entry["avg_ms"]
                bottleneck = entry["id"]
            entry["avg_ms"] = round(entry["avg_ms"], 2)
            entry["run_avg_ms"] = round(entry["run_avg_ms"], 2)
            entry["last_ms"] = round(entry["last_ms"], 2)
            entry["share"] = round(entry["share"], 3)

        fps = 0.0
        if len(self._end_times) >= 2:
            span = self._end_times[-1] - self._end_times[0]
            if span > 0:
                fps = (len(self._end_times) - 1) / span

        frame_ms = sum(self._total_history) / len(self._total_history) if self._total_history else 0.0
        run_inference = self._run_history["inference"]
        inference_avg_ms = sum(run_inference) / len(run_inference) if run_inference else 0.0

        return {
            "stages": stages,
            "bottleneck": bottleneck,
            "fps": round(fps, 2),
            "frame_ms": round(frame_ms, 2),
            "inference_avg_ms": round(inference_avg_ms, 2),
        }

    def metadata(self):
        """
        <summary>단계 목록과 각 단계 함수의 docstring 설명을 돌려준다.</summary>
        <input>등록된 단계 정보</input>
        <output>순서, 이름, 라벨, 함수 이름, doc, error 를 담은 사전의 목록</output>
        <reason>대시보드가 처음 열릴 때 플로우차트 상자와 설명을 한 번에 그리기 위해서다.</reason>
        """
        with self._lock:
            result = []
            for order, item in enumerate(STAGES):
                entry = self._registry.get(item["id"])
                doc = None
                if entry is not None and entry["doc"] is not None:
                    doc = dict(entry["doc"])
                    doc["params"] = [dict(param) for param in entry["doc"]["params"]]
                result.append(
                    {
                        "order": order,
                        "id": item["id"],
                        "label": item["label"],
                        "name": item["name"],
                        "function": entry["function"] if entry is not None else None,
                        "doc": doc,
                        "error": entry["error"] if entry is not None else None,
                    }
                )
            return result

    def snapshot(self, after_seq=0):
        """
        <summary>대시보드에 보낼 현재 상태를 한 번에 모은다.</summary>
        <param name="after_seq">이미 받은 마지막 로그 번호. 이 번호보다 큰 로그만 담는다</param>
        <input>통계, 카운터, 상태값, 로그 링버퍼</input>
        <output>단계별 avg_ms 와 share, 병목, 활성 단계, 직전 skip 여부, FPS, 카운터, 상태값, 새 로그를 담은 사전</output>
        <reason>SSE 로 주기적으로 보낼 때 이미 보낸 로그를 반복하지 않고 바뀐 부분만 보내기 위해서다.</reason>
        """
        with self._lock:
            try:
                after = int(after_seq)
            except (TypeError, ValueError):
                after = 0
            if after > self._seq:
                # 서버가 다시 시작되어 번호가 줄었으면 처음부터 다시 보낸다.
                after = 0

            fps = self._stats["fps"]
            if not self._end_times or time.perf_counter() - self._end_times[-1] > IDLE_FPS_RESET_SEC:
                fps = 0.0

            logs = [dict(event) for event in self._events if event["seq"] > after]
            if len(logs) > self._sse_max_logs:
                logs = logs[-self._sse_max_logs:]

            bottleneck = self._stats["bottleneck"]
            return {
                "frame": self._frame_id,
                "fps": fps,
                "frame_ms": self._stats["frame_ms"],
                "stages": [dict(entry) for entry in self._stats["stages"]],
                "bottleneck": bottleneck,
                "bottleneck_label": STAGE_LABELS.get(bottleneck),
                "active_stage": self._active_stage,
                "last_skipped": self._last_skipped,
                "inference_avg_ms": self._stats["inference_avg_ms"],
                "counters": dict(self._counters),
                "status": _to_jsonable(self._status),
                "logs": logs,
                "last_seq": self._seq,
            }


# ---------------------------------------------------------------------------
# 싱글톤
# ---------------------------------------------------------------------------
PIPELINE = Pipeline(
    log_maxlen=config.LOG_MAXLEN,
    stat_window=config.STAT_WINDOW,
    sse_max_logs=config.SSE_MAX_LOGS,
    log_every=config.LOG_EVERY,
)
stage = PIPELINE.stage
