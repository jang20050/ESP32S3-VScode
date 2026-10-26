/*
 * 파이프라인 대시보드
 *
 * - /api/pipeline 으로 단계 설명을 받아 플로우차트 SVG 를 한 번 만든다.
 * - /events (SSE) 로 200ms 마다 오는 상태로 SVG 의 글자와 막대만 바꾼다.
 *   Mermaid 처럼 매번 다시 그리지 않으므로 깜박이지 않고, 외부 CDN 없이 오프라인에서도 동작한다.
 */
"use strict";

const SVG_NS = "http://www.w3.org/2000/svg";
const LOG_DOM_LIMIT = 400;
const PENDING_LIMIT = 4000;
const NEAR_BOTTOM_PX = 32;
const THUMB_COUNT = 10;
const RETRY_MS = 2000;
const SKIP_OPTIONS = [1, 2, 3, 4, 5, 6, 8, 10];
const IMGSZ_OPTIONS = [160, 192, 224, 256, 288, 320, 384, 416, 480, 512, 576, 640];

const GEO = {
    width: 440,
    top: 14,
    nodeX: 44,
    nodeW: 212,
    nodeH: 46,
    gap: 26,
    metricX: 276,
    metricW: 150,
    loopX: 20
};

const CAMERA_TEXT = new Map([
    ["starting", "시작 중"],
    ["connecting", "연결 중"],
    ["connected", "연결됨"],
    ["disconnected", "끊김"],
    ["stopped", "중지"]
]);

const state = {
    metadata: [],
    nodes: new Map(),
    selected: null,
    lastSnapshot: null,
    lastSeq: 0,
    paused: false,
    pending: [],
    events: null,
    eventsTimer: null,
    wasDisconnected: false,
    settingsTimer: null,
    feed: "detect",
    feedRetryTimer: null
};

// ---------------------------------------------------------------------
// 작은 도우미
// ---------------------------------------------------------------------

/**
 * <summary>id 로 요소를 찾는다.</summary>
 * <param name="id">요소 id</param>
 * <returns>HTMLElement 또는 null</returns>
 */
function $(id) {
    return document.getElementById(id);
}

/**
 * <summary>요소의 글자를 바꾼다. 같은 글자면 건드리지 않는다.</summary>
 * <param name="target">요소 또는 요소 id</param>
 * <param name="text">새 글자</param>
 * <returns>없음</returns>
 */
function setText(target, text) {
    const node = typeof target === "string" ? $(target) : target;
    if (!node) {
        return;
    }
    const value = text === null || text === undefined ? "" : String(text);
    if (node.textContent !== value) {
        node.textContent = value;
    }
}

/**
 * <summary>HTML 요소를 만든다.</summary>
 * <param name="tag">태그 이름</param>
 * <param name="className">클래스 이름 (없으면 빈 문자열)</param>
 * <param name="text">안에 넣을 글자 (없으면 생략)</param>
 * <returns>새 HTMLElement</returns>
 */
function el(tag, className, text) {
    const node = document.createElement(tag);
    if (className) {
        node.className = className;
    }
    if (text !== undefined && text !== null) {
        node.textContent = String(text);
    }
    return node;
}

/**
 * <summary>SVG 요소를 만든다.</summary>
 * <param name="tag">SVG 태그 이름</param>
 * <param name="className">클래스 이름 (없으면 빈 문자열)</param>
 * <returns>새 SVGElement</returns>
 */
function svgEl(tag, className) {
    const node = document.createElementNS(SVG_NS, tag);
    if (className) {
        node.setAttribute("class", className);
    }
    return node;
}

/**
 * <summary>이름과 값이 번갈아 나오는 배열로 속성을 한꺼번에 설정한다.</summary>
 * <param name="node">대상 요소</param>
 * <param name="pairs">["x", 10, "y", 20] 형태의 배열</param>
 * <returns>대상 요소</returns>
 */
function setAttrs(node, pairs) {
    for (let i = 0; i + 1 < pairs.length; i += 2) {
        node.setAttribute(pairs[i], String(pairs[i + 1]));
    }
    return node;
}

/**
 * <summary>자식 요소를 모두 지운다.</summary>
 * <param name="node">대상 요소</param>
 * <returns>없음</returns>
 */
function clearChildren(node) {
    while (node.firstChild) {
        node.removeChild(node.firstChild);
    }
}

/**
 * <summary>숫자를 소수점 자리수에 맞춰 문자열로 바꾼다.</summary>
 * <param name="value">숫자 (없으면 - 를 돌려준다)</param>
 * <param name="digits">소수점 자리수</param>
 * <returns>표시용 문자열</returns>
 */
function formatNumber(value, digits) {
    if (value === null || value === undefined || Number.isNaN(Number(value))) {
        return "-";
    }
    return Number(value).toFixed(digits);
}

/**
 * <summary>밀리초 값을 "12.3 ms" 형태로 바꾼다.</summary>
 * <param name="value">밀리초</param>
 * <returns>표시용 문자열</returns>
 */
function formatMs(value) {
    const text = formatNumber(value, 1);
    return text === "-" ? "-" : text + " ms";
}

/**
 * <summary>0~1 비율을 "34%" 형태로 바꾼다.</summary>
 * <param name="value">비율</param>
 * <returns>표시용 문자열</returns>
 */
function formatShare(value) {
    if (value === null || value === undefined) {
        return "-";
    }
    return Math.round(Number(value) * 100) + "%";
}

/**
 * <summary>JSON 을 받아 온다.</summary>
 * <param name="url">요청 주소</param>
 * <param name="options">fetch 옵션 (없으면 GET)</param>
 * <returns>응답 객체와 JSON 본문을 담은 Promise</returns>
 */
async function fetchJson(url, options) {
    const response = await fetch(url, options);
    let body = null;
    try {
        body = await response.json();
    } catch (error) {
        body = null;
    }
    return [response, body];
}

// ---------------------------------------------------------------------
// 플로우차트
// ---------------------------------------------------------------------

/**
 * <summary>순번에 해당하는 노드의 위쪽 y 좌표를 계산한다.</summary>
 * <param name="index">0 부터 시작하는 단계 순번</param>
 * <returns>y 좌표</returns>
 */
function nodeY(index) {
    return GEO.top + index * (GEO.nodeH + GEO.gap);
}

/**
 * <summary>화살촉 모양을 담은 defs 요소를 만든다.</summary>
 * <returns>SVG defs 요소</returns>
 */
function buildDefs() {
    const defs = svgEl("defs", "");
    const marker = setAttrs(svgEl("marker", ""), [
        "id", "arrow",
        "viewBox", "0 0 8 8",
        "refX", 7,
        "refY", 4,
        "markerWidth", 7,
        "markerHeight", 7,
        "orient", "auto-start-reverse"
    ]);
    const head = setAttrs(svgEl("path", "arrow-head"), ["d", "M0,0 L8,4 L0,8 z"]);
    marker.appendChild(head);
    defs.appendChild(marker);
    return defs;
}

/**
 * <summary>단계 하나의 노드(상자, 이름, 표시, 평균 ms, 비중 막대)를 만든다.</summary>
 * <param name="meta">metadata 의 단계 정보</param>
 * <param name="index">단계 순번</param>
 * <returns>나중에 갱신할 요소들을 담은 객체</returns>
 */
function buildNode(meta, index) {
    const y = nodeY(index);
    const group = setAttrs(svgEl("g", "node"), [
        "data-stage", meta.id,
        "tabindex", 0,
        "role", "button",
        "aria-label", meta.label + " 단계 설명 보기"
    ]);

    const box = setAttrs(svgEl("rect", "node-box"), [
        "x", GEO.nodeX,
        "y", y,
        "width", GEO.nodeW,
        "height", GEO.nodeH,
        "rx", 2
    ]);
    const label = setAttrs(svgEl("text", "node-label"), ["x", GEO.nodeX + 14, "y", y + 20]);
    label.textContent = meta.label;
    const ident = setAttrs(svgEl("text", "node-id"), ["x", GEO.nodeX + 14, "y", y + 36]);
    ident.textContent = meta.id;
    const badgeTop = setAttrs(svgEl("text", "badge"), [
        "x", GEO.nodeX + GEO.nodeW - 12,
        "y", y + 20,
        "text-anchor", "end"
    ]);
    const badgeBottom = setAttrs(svgEl("text", "badge"), [
        "x", GEO.nodeX + GEO.nodeW - 12,
        "y", y + 36,
        "text-anchor", "end"
    ]);

    const ms = setAttrs(svgEl("text", "metric-ms"), ["x", GEO.metricX, "y", y + 20]);
    ms.textContent = "-";
    const share = setAttrs(svgEl("text", "metric-share"), [
        "x", GEO.metricX + GEO.metricW,
        "y", y + 20,
        "text-anchor", "end"
    ]);
    share.textContent = "-";
    const track = setAttrs(svgEl("rect", "bar-track"), [
        "x", GEO.metricX,
        "y", y + 29,
        "width", GEO.metricW,
        "height", 2
    ]);
    const bar = setAttrs(svgEl("rect", "bar-fill"), [
        "x", GEO.metricX,
        "y", y + 29,
        "width", 0,
        "height", 2
    ]);
    const title = svgEl("title", "");
    title.textContent = meta.doc && meta.doc.summary ? meta.doc.summary : meta.label;

    group.appendChild(title);
    group.appendChild(box);
    group.appendChild(label);
    group.appendChild(ident);
    group.appendChild(badgeTop);
    group.appendChild(badgeBottom);
    group.appendChild(ms);
    group.appendChild(share);
    group.appendChild(track);
    group.appendChild(bar);

    return {
        group: group,
        badgeTop: badgeTop,
        badgeBottom: badgeBottom,
        ms: ms,
        share: share,
        bar: bar
    };
}

/**
 * <summary>metadata 로 플로우차트 SVG 전체를 새로 만든다.</summary>
 * <param name="metadata">/api/pipeline 의 단계 목록</param>
 * <returns>없음</returns>
 */
function buildFlow(metadata) {
    const svg = $("flow");
    clearChildren(svg);
    state.nodes.clear();

    const count = metadata.length;
    const height = GEO.top * 2 + count * GEO.nodeH + Math.max(0, count - 1) * GEO.gap;
    setAttrs(svg, ["viewBox", "0 0 " + GEO.width + " " + height]);
    svg.appendChild(buildDefs());

    const centerX = GEO.nodeX + GEO.nodeW / 2;
    for (let i = 0; i + 1 < count; i += 1) {
        const edge = setAttrs(svgEl("line", "edge"), [
            "x1", centerX,
            "y1", nodeY(i) + GEO.nodeH,
            "x2", centerX,
            "y2", nodeY(i + 1) - 1,
            "marker-end", "url(#arrow)"
        ]);
        svg.appendChild(edge);
    }

    if (count > 1) {
        const firstMid = nodeY(0) + GEO.nodeH / 2;
        const lastMid = nodeY(count - 1) + GEO.nodeH / 2;
        const loop = setAttrs(svgEl("path", "loop"), [
            "d", "M" + GEO.nodeX + "," + lastMid + " H" + GEO.loopX + " V" + firstMid + " H" + (GEO.nodeX - 1),
            "marker-end", "url(#arrow)"
        ]);
        svg.appendChild(loop);
        const middle = (firstMid + lastMid) / 2;
        const loopLabel = setAttrs(svgEl("text", "loop-label"), [
            "x", GEO.loopX - 6,
            "y", middle,
            "text-anchor", "middle",
            "transform", "rotate(-90 " + (GEO.loopX - 6) + " " + middle + ")"
        ]);
        loopLabel.textContent = "다음 프레임";
        svg.appendChild(loopLabel);
    }

    for (let i = 0; i < count; i += 1) {
        const node = buildNode(metadata[i], i);
        svg.appendChild(node.group);
        state.nodes.set(metadata[i].id, node);
    }

    if (!state.selected || !state.nodes.has(state.selected)) {
        state.selected = count > 0 ? metadata[0].id : null;
    }
    markSelected();
    renderDetail();
}

/**
 * <summary>snapshot 으로 노드의 평균 ms, 비중 막대, 활성, 병목, 생략 표시를 갱신한다.</summary>
 * <param name="snap">/events 로 받은 snapshot</param>
 * <returns>없음</returns>
 */
function updateFlow(snap) {
    const stats = new Map();
    for (const stage of snap.stages || []) {
        stats.set(stage.id, stage);
    }
    for (const [id, node] of state.nodes) {
        const stage = stats.get(id);
        setText(node.ms, stage ? formatMs(stage.avg_ms) : "-");
        setText(node.share, stage ? formatShare(stage.share) : "-");
        const width = stage ? Math.max(0, Math.min(1, stage.share)) * GEO.metricW : 0;
        node.bar.setAttribute("width", width.toFixed(1));

        const isActive = snap.active_stage === id;
        const isBottleneck = snap.bottleneck === id;
        const isSkipped = id === "inference" && Boolean(snap.last_skipped);
        node.group.classList.toggle("is-active", isActive);
        node.group.classList.toggle("is-bottleneck", isBottleneck);
        node.group.classList.toggle("is-skipped", isSkipped);
        setText(node.badgeTop, isBottleneck ? "병목" : "");
        setText(node.badgeBottom, isSkipped ? "생략" : "");
    }
}

/**
 * <summary>선택된 노드에만 is-selected 클래스를 붙인다.</summary>
 * <returns>없음</returns>
 */
function markSelected() {
    for (const [id, node] of state.nodes) {
        node.group.classList.toggle("is-selected", id === state.selected);
        node.group.setAttribute("aria-pressed", id === state.selected ? "true" : "false");
    }
}

/**
 * <summary>단계를 선택하고 상세 패널을 다시 그린다.</summary>
 * <param name="id">단계 id</param>
 * <returns>없음</returns>
 */
function selectStage(id) {
    if (!state.nodes.has(id)) {
        return;
    }
    state.selected = id;
    markSelected();
    renderDetail();
}

/**
 * <summary>플로우차트 클릭을 받아 누른 노드를 선택한다.</summary>
 * <param name="event">click 이벤트</param>
 * <returns>없음</returns>
 */
function onFlowClick(event) {
    const target = event.target.closest("[data-stage]");
    if (target) {
        selectStage(target.getAttribute("data-stage"));
    }
}

/**
 * <summary>노드에 초점이 있을 때 Enter 나 Space 로 선택한다.</summary>
 * <param name="event">keydown 이벤트</param>
 * <returns>없음</returns>
 */
function onFlowKey(event) {
    if (event.key !== "Enter" && event.key !== " ") {
        return;
    }
    const target = event.target.closest("[data-stage]");
    if (target) {
        event.preventDefault();
        selectStage(target.getAttribute("data-stage"));
    }
}

/**
 * <summary>선택된 단계의 metadata 를 찾는다.</summary>
 * <returns>단계 정보 객체 또는 null</returns>
 */
function selectedMeta() {
    for (const meta of state.metadata) {
        if (meta.id === state.selected) {
            return meta;
        }
    }
    return null;
}

/**
 * <summary>상세 패널에 docstring 설명(summary, input, output, reason, params)과 함수 이름을 채운다.</summary>
 * <returns>없음</returns>
 */
function renderDetail() {
    const meta = selectedMeta();
    if (!meta) {
        setText("detail-title", "-");
        return;
    }
    setText("detail-title", meta.label + " · " + meta.name);
    setText("detail-func", meta.function || "등록된 함수 없음");

    const doc = meta.doc;
    const errorBox = $("detail-error");
    const docBox = $("detail-doc");
    if (!doc) {
        errorBox.hidden = false;
        setText(errorBox, meta.error ? "설명을 읽지 못했습니다: " + meta.error : "이 단계에 등록된 설명이 없습니다.");
        docBox.hidden = true;
    } else {
        errorBox.hidden = true;
        docBox.hidden = false;
        setText("detail-summary", doc.summary || "-");
        setText("detail-input", doc.input || "-");
        setText("detail-output", doc.output || "-");
        setText("detail-reason", doc.reason || "-");

        const params = $("detail-params");
        clearChildren(params);
        if (!doc.params || doc.params.length === 0) {
            params.appendChild(el("dd", "", "없음"));
        } else {
            for (const param of doc.params) {
                params.appendChild(el("dt", "", param.name));
                params.appendChild(el("dd", "", param.text));
            }
        }
    }
    renderDetailStats();
}

/**
 * <summary>상세 패널의 실행 시 평균, 프레임당 평균, 비중 숫자만 갱신한다.</summary>
 * <returns>없음</returns>
 */
function renderDetailStats() {
    const snap = state.lastSnapshot;
    let stage = null;
    if (snap && snap.stages) {
        for (const item of snap.stages) {
            if (item.id === state.selected) {
                stage = item;
            }
        }
    }
    setText("detail-run-avg", stage ? formatMs(stage.run_avg_ms) : "-");
    setText("detail-avg", stage ? formatMs(stage.avg_ms) : "-");
    setText("detail-share", stage ? formatShare(stage.share) : "-");
}

// ---------------------------------------------------------------------
// 상단 바
// ---------------------------------------------------------------------

/**
 * <summary>상단 바의 FPS, 수신 FPS, 프레임 지연, 장치, 모델, 소스 상태를 갱신한다.</summary>
 * <param name="snap">snapshot</param>
 * <returns>없음</returns>
 */
function updateTopbar(snap) {
    const status = snap.status || Object.create(null);
    setText("m-fps", formatNumber(snap.fps, 1));
    setText("m-stream-fps", formatNumber(status.stream_fps, 1));

    // 받은 뒤 처리 시작까지 묵은 시간 + 처리 시간(대기 시간 제외)
    if (status.frame_age_ms === undefined || !snap.frame_ms) {
        setText("m-delay", "-");
    } else {
        const wait = Number(status.capture_wait_ms || 0);
        const delay = Number(status.frame_age_ms) + Math.max(0, Number(snap.frame_ms) - wait);
        setText("m-delay", formatMs(delay));
    }

    setText("m-device", status.device || "-");
    setText("m-model", status.yolo ? status.model || "-" : "YOLO 끔");

    const camera = status.camera || "unknown";
    const cameraBox = $("m-camera");
    if (cameraBox.getAttribute("data-state") !== camera) {
        cameraBox.setAttribute("data-state", camera);
    }
    setText("m-camera-text", CAMERA_TEXT.get(camera) || camera);
    cameraBox.title = status.camera_url || "";

    if (status.settings) {
        applySettingsToForm(status.settings);
    }
}

/**
 * <summary>SSE 연결 상태 표시를 바꾼다.</summary>
 * <param name="mode">open, reconnecting, connecting 중 하나</param>
 * <returns>없음</returns>
 */
function setConnection(mode) {
    const box = $("conn");
    box.setAttribute("data-state", mode);
    if (mode === "open") {
        setText(box, "실시간");
    } else if (mode === "reconnecting") {
        setText(box, "재연결 중");
    } else {
        setText(box, "연결 중");
    }
}

// ---------------------------------------------------------------------
// 실행 중 설정
// ---------------------------------------------------------------------

/**
 * <summary>select 의 옵션을 만들고 현재 값을 고른다. 현재 값이 목록에 없으면 추가한다.</summary>
 * <param name="select">대상 select 요소</param>
 * <param name="values">옵션 값 배열</param>
 * <param name="current">현재 값</param>
 * <returns>없음</returns>
 */
function fillSelect(select, values, current) {
    const list = values.slice();
    if (current !== undefined && current !== null && list.indexOf(Number(current)) < 0) {
        list.push(Number(current));
        list.sort(compareNumbers);
    }
    clearChildren(select);
    for (const value of list) {
        const option = el("option", "", value);
        option.value = String(value);
        select.appendChild(option);
    }
    if (current !== undefined && current !== null) {
        select.value = String(current);
    }
}

/**
 * <summary>숫자 오름차순 정렬용 비교 함수.</summary>
 * <param name="a">첫 번째 숫자</param>
 * <param name="b">두 번째 숫자</param>
 * <returns>a 가 작으면 음수</returns>
 */
function compareNumbers(a, b) {
    return a - b;
}

/**
 * <summary>서버의 설정값을 폼에 반영한다. 사용자가 입력 중인 칸은 건드리지 않는다.</summary>
 * <param name="settings">skip, imgsz, conf 를 가진 객체</param>
 * <returns>없음</returns>
 */
function applySettingsToForm(settings) {
    const form = $("settings-form");
    if (form.contains(document.activeElement)) {
        return;
    }
    const skip = $("set-skip");
    const imgsz = $("set-imgsz");
    const conf = $("set-conf");
    if (skip.options.length === 0 || skip.value !== String(settings.skip)) {
        fillSelect(skip, SKIP_OPTIONS, settings.skip);
    }
    if (imgsz.options.length === 0 || imgsz.value !== String(settings.imgsz)) {
        fillSelect(imgsz, IMGSZ_OPTIONS, settings.imgsz);
    }
    const confText = Number(settings.conf).toFixed(2);
    if (conf.value !== confText) {
        conf.value = confText;
    }
}

/**
 * <summary>설정 메시지를 잠깐 보여 준다.</summary>
 * <param name="text">메시지</param>
 * <param name="kind">ok 또는 error</param>
 * <returns>없음</returns>
 */
function showSettingsMessage(text, kind) {
    const box = $("settings-msg");
    box.setAttribute("data-kind", kind);
    setText(box, text);
    if (state.settingsTimer) {
        clearTimeout(state.settingsTimer);
    }
    state.settingsTimer = setTimeout(clearSettingsMessage, kind === "error" ? 6000 : 2000);
}

/**
 * <summary>설정 메시지를 지운다.</summary>
 * <returns>없음</returns>
 */
function clearSettingsMessage() {
    setText("settings-msg", "");
}

/**
 * <summary>폼의 skip, imgsz, conf 를 서버에 보내 실행 중 설정을 바꾼다.</summary>
 * <returns>Promise (완료 시 없음)</returns>
 */
async function submitSettings() {
    const payload = {
        skip: Number($("set-skip").value),
        imgsz: Number($("set-imgsz").value),
        conf: Number($("set-conf").value)
    };
    try {
        const result = await fetchJson("/api/settings", {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify(payload)
        });
        const response = result[0];
        const body = result[1];
        if (response.ok) {
            showSettingsMessage("적용됨", "ok");
            return;
        }
        const messages = [];
        if (body && body.errors) {
            for (const key of Object.keys(body.errors)) {
                messages.push(key + ": " + body.errors[key]);
            }
        }
        showSettingsMessage(messages.join(", ") || "적용 실패", "error");
        if (body && body.settings) {
            $("set-conf").blur();
            applySettingsToForm(body.settings);
        }
    } catch (error) {
        showSettingsMessage("서버에 보내지 못했습니다", "error");
    }
}

/**
 * <summary>설정 칸이 바뀌면 바로 서버에 보낸다.</summary>
 * <param name="event">change 이벤트</param>
 * <returns>없음</returns>
 */
function onSettingsChange(event) {
    submitSettings();
    if (event.target.tagName === "SELECT") {
        event.target.blur();
    }
}

/**
 * <summary>conf 칸에서 Enter 를 눌러도 페이지가 다시 열리지 않게 막고 설정을 보낸다.</summary>
 * <param name="event">submit 이벤트</param>
 * <returns>없음</returns>
 */
function onSettingsSubmit(event) {
    event.preventDefault();
    submitSettings();
}

/**
 * <summary>서버의 실행 중 설정을 읽어 폼을 처음 채운다.</summary>
 * <returns>Promise (완료 시 없음)</returns>
 */
async function loadConfig() {
    try {
        const result = await fetchJson("/api/config");
        const body = result[1];
        if (body && body.runtime) {
            applySettingsToForm(body.runtime);
        }
    } catch (error) {
        setTimeout(loadConfig, RETRY_MS);
    }
}

// ---------------------------------------------------------------------
// 영상과 캡처
// ---------------------------------------------------------------------

/**
 * <summary>탐지 영상과 원본 영상을 바꾼다.</summary>
 * <param name="kind">detect 또는 raw</param>
 * <param name="force">같은 영상이어도 다시 연결하려면 true</param>
 * <returns>없음</returns>
 */
function setFeed(kind, force) {
    const image = $("feed");
    if (kind === state.feed && !force) {
        return;
    }
    state.feed = kind;
    const base = kind === "raw" ? "/raw_feed" : "/detect_feed";
    image.src = base + "?t=" + Date.now();
    image.alt = kind === "raw" ? "ESP32 원본 영상" : "탐지 결과 영상";
    for (const button of document.querySelectorAll("[data-feed]")) {
        button.setAttribute("aria-pressed", button.getAttribute("data-feed") === kind ? "true" : "false");
    }
}

/**
 * <summary>영상 전환 버튼을 처리한다.</summary>
 * <param name="event">click 이벤트</param>
 * <returns>없음</returns>
 */
function onFeedButton(event) {
    setFeed(event.currentTarget.getAttribute("data-feed"), false);
}

/**
 * <summary>영상 연결이 끊기면 잠시 뒤 다시 연결한다.</summary>
 * <returns>없음</returns>
 */
function onFeedError() {
    if (state.feedRetryTimer) {
        return;
    }
    state.feedRetryTimer = setTimeout(retryFeed, RETRY_MS);
}

/**
 * <summary>현재 영상 종류로 다시 연결한다.</summary>
 * <returns>없음</returns>
 */
function retryFeed() {
    state.feedRetryTimer = null;
    setFeed(state.feed, true);
}

/**
 * <summary>현재 결과 화면을 서버에 저장하고 썸네일을 새로 고친다.</summary>
 * <returns>Promise (완료 시 없음)</returns>
 */
async function takeCapture() {
    const button = $("btn-capture");
    button.disabled = true;
    try {
        const result = await fetchJson("/capture", {
            method: "POST"
        });
        const response = result[0];
        const body = result[1];
        if (response.ok && body) {
            setText("capture-msg", "저장됨: " + body.name);
            await loadCaptures();
        } else {
            setText("capture-msg", body && body.error ? body.error : "저장 실패");
        }
    } catch (error) {
        setText("capture-msg", "서버에 연결할 수 없습니다");
    } finally {
        button.disabled = false;
    }
}

/**
 * <summary>최근 캡처 목록을 받아 썸네일을 그린다.</summary>
 * <returns>Promise (완료 시 없음)</returns>
 */
async function loadCaptures() {
    try {
        const result = await fetchJson("/api/captures");
        const list = Array.isArray(result[1]) ? result[1] : [];
        renderCaptures(list.slice(0, THUMB_COUNT));
    } catch (error) {
        renderCaptures([]);
    }
}

/**
 * <summary>캡처 썸네일을 그린다. 누르면 새 탭에서 원본 크기로 열린다.</summary>
 * <param name="list">name, url, time 을 가진 캡처 목록</param>
 * <returns>없음</returns>
 */
function renderCaptures(list) {
    const box = $("thumbs");
    clearChildren(box);
    if (list.length === 0) {
        box.appendChild(el("p", "hint", "아직 저장한 사진이 없습니다."));
        return;
    }
    for (const item of list) {
        const link = el("a", "", null);
        link.href = item.url;
        link.target = "_blank";
        link.rel = "noopener";
        link.title = item.time + "  " + item.name;
        const image = el("img", "", null);
        image.src = item.url;
        image.alt = item.name;
        image.loading = "lazy";
        link.appendChild(image);
        box.appendChild(link);
    }
}

// ---------------------------------------------------------------------
// 로그
// ---------------------------------------------------------------------

/**
 * <summary>로그 상자가 바닥 근처까지 스크롤되어 있는지 확인한다.</summary>
 * <param name="box">로그 상자</param>
 * <returns>바닥 근처면 true</returns>
 */
function isNearBottom(box) {
    return box.scrollHeight - box.scrollTop - box.clientHeight < NEAR_BOTTOM_PX;
}

/**
 * <summary>로그 한 줄의 DOM 을 만든다.</summary>
 * <param name="entry">seq, time, level, label, msg 를 가진 로그</param>
 * <returns>로그 줄 요소</returns>
 */
function renderLogLine(entry) {
    const line = el("div", "log-line lv-" + entry.level, null);
    line.appendChild(el("span", "log-time", entry.time));
    line.appendChild(el("span", "log-level", entry.level));
    line.appendChild(el("span", "log-stage", entry.label ? "[" + entry.label + "]" : ""));
    line.appendChild(el("span", "log-msg", entry.msg));
    return line;
}

/**
 * <summary>로그 줄이 LOG_DOM_LIMIT 를 넘으면 오래된 줄부터 지운다.</summary>
 * <param name="box">로그 상자</param>
 * <returns>없음</returns>
 */
function trimLog(box) {
    let extra = box.childElementCount - LOG_DOM_LIMIT;
    while (extra > 0 && box.firstElementChild) {
        box.removeChild(box.firstElementChild);
        extra -= 1;
    }
}

/**
 * <summary>로그 줄 수 표시를 갱신한다.</summary>
 * <returns>없음</returns>
 */
function updateLogCount() {
    const count = $("log").childElementCount;
    let text = count + "줄";
    if (state.paused && state.pending.length > 0) {
        text += " · 대기 " + state.pending.length;
    }
    setText("log-count", text);
}

/**
 * <summary>로그 여러 줄을 아래에 붙인다. 바닥을 보고 있을 때만 자동으로 스크롤한다.</summary>
 * <param name="list">로그 목록</param>
 * <returns>없음</returns>
 */
function appendLogLines(list) {
    if (list.length === 0) {
        return;
    }
    const box = $("log");
    const stick = isNearBottom(box);
    const start = Math.max(0, list.length - LOG_DOM_LIMIT);
    const fragment = document.createDocumentFragment();
    for (let i = start; i < list.length; i += 1) {
        fragment.appendChild(renderLogLine(list[i]));
    }
    box.appendChild(fragment);
    trimLog(box);
    if (stick) {
        box.scrollTop = box.scrollHeight;
    }
    updateLogCount();
}

/**
 * <summary>새 로그를 받는다. 일시정지 중이면 쌓아 두었다가 재개할 때 반영한다.</summary>
 * <param name="list">새 로그 목록</param>
 * <returns>없음</returns>
 */
function handleLogs(list) {
    if (list.length === 0) {
        return;
    }
    if (state.paused) {
        for (const entry of list) {
            state.pending.push(entry);
        }
        if (state.pending.length > PENDING_LIMIT) {
            state.pending.splice(0, state.pending.length - PENDING_LIMIT);
        }
        updatePauseButton();
        updateLogCount();
        return;
    }
    appendLogLines(list);
}

/**
 * <summary>일시정지 버튼의 글자와 상태를 갱신한다.</summary>
 * <returns>없음</returns>
 */
function updatePauseButton() {
    const button = $("btn-pause");
    button.setAttribute("aria-pressed", state.paused ? "true" : "false");
    if (state.paused) {
        setText(button, state.pending.length > 0 ? "재개 (" + state.pending.length + ")" : "재개");
    } else {
        setText(button, "일시정지");
    }
}

/**
 * <summary>로그 일시정지와 재개를 바꾼다. 재개하면 쌓인 로그를 한 번에 붙인다.</summary>
 * <returns>없음</returns>
 */
function togglePause() {
    state.paused = !state.paused;
    if (!state.paused) {
        const waiting = state.pending;
        state.pending = [];
        appendLogLines(waiting);
        const box = $("log");
        box.scrollTop = box.scrollHeight;
    }
    updatePauseButton();
    updateLogCount();
}

/**
 * <summary>화면의 로그와 대기 중인 로그를 지운다.</summary>
 * <returns>없음</returns>
 */
function clearLog() {
    clearChildren($("log"));
    state.pending = [];
    updatePauseButton();
    updateLogCount();
}

/**
 * <summary>레벨 필터 체크박스에 맞춰 로그 줄을 보이거나 숨긴다.</summary>
 * <param name="event">change 이벤트</param>
 * <returns>없음</returns>
 */
function onFilterChange(event) {
    const box = $("log");
    const stick = isNearBottom(box);
    const level = event.target.getAttribute("data-level");
    box.classList.toggle("hide-" + level, !event.target.checked);
    if (stick) {
        box.scrollTop = box.scrollHeight;
    }
}

// ---------------------------------------------------------------------
// 데이터 수신
// ---------------------------------------------------------------------

/**
 * <summary>snapshot 하나를 화면 전체에 반영한다.</summary>
 * <param name="snap">/api/pipeline 또는 /events 의 snapshot</param>
 * <returns>없음</returns>
 */
function applySnapshot(snap) {
    if (snap.last_seq < state.lastSeq) {
        // 서버가 다시 시작되어 로그 번호가 처음부터 다시 매겨졌다.
        state.lastSeq = 0;
        reloadMetadata();
    }
    state.lastSnapshot = snap;
    updateTopbar(snap);
    updateFlow(snap);
    renderDetailStats();

    const fresh = [];
    for (const entry of snap.logs || []) {
        if (entry.seq > state.lastSeq) {
            fresh.push(entry);
        }
    }
    if (snap.last_seq > state.lastSeq) {
        state.lastSeq = snap.last_seq;
    }
    handleLogs(fresh);
}

/**
 * <summary>단계 설명과 첫 snapshot 을 받아 플로우차트를 만든다.</summary>
 * <returns>성공하면 true 를 담은 Promise</returns>
 */
async function loadPipeline() {
    try {
        const result = await fetchJson("/api/pipeline?after=" + state.lastSeq);
        const body = result[1];
        if (!result[0].ok || !body) {
            return false;
        }
        state.metadata = body.metadata || [];
        buildFlow(state.metadata);
        applySnapshot(body.snapshot);
        return true;
    } catch (error) {
        return false;
    }
}

/**
 * <summary>서버가 다시 시작된 뒤 단계 설명을 다시 받아 플로우차트를 새로 만든다.</summary>
 * <returns>Promise (완료 시 없음)</returns>
 */
async function reloadMetadata() {
    try {
        const result = await fetchJson("/api/pipeline?after=0");
        const body = result[1];
        if (body && body.metadata) {
            state.metadata = body.metadata;
            buildFlow(state.metadata);
        }
    } catch (error) {
        setTimeout(reloadMetadata, RETRY_MS);
    }
}

/**
 * <summary>SSE 연결을 연다. 이미 열려 있으면 닫고 새로 연다.</summary>
 * <returns>없음</returns>
 */
function connectEvents() {
    state.eventsTimer = null;
    if (state.events) {
        state.events.close();
    }
    const source = new EventSource("/events?after=" + state.lastSeq);
    source.onopen = onEventsOpen;
    source.onmessage = onEventsMessage;
    source.onerror = onEventsError;
    state.events = source;
}

/**
 * <summary>SSE 가 연결되면 표시를 바꾸고, 끊겼다가 돌아온 경우 영상과 캡처도 다시 불러온다.</summary>
 * <returns>없음</returns>
 */
function onEventsOpen() {
    setConnection("open");
    if (state.wasDisconnected) {
        state.wasDisconnected = false;
        setFeed(state.feed, true);
        loadCaptures();
        loadConfig();
    }
}

/**
 * <summary>SSE 메시지(snapshot JSON)를 화면에 반영한다.</summary>
 * <param name="event">message 이벤트</param>
 * <returns>없음</returns>
 */
function onEventsMessage(event) {
    let snap = null;
    try {
        snap = JSON.parse(event.data);
    } catch (error) {
        return;
    }
    applySnapshot(snap);
}

/**
 * <summary>SSE 가 끊기면 "재연결 중"을 표시한다. 브라우저가 포기한 경우 직접 다시 연결한다.</summary>
 * <returns>없음</returns>
 */
function onEventsError() {
    setConnection("reconnecting");
    state.wasDisconnected = true;
    // 서버와 끊긴 동안에는 카메라 상태를 알 수 없으므로 마지막 값을 지운다.
    $("m-camera").setAttribute("data-state", "unknown");
    setText("m-camera-text", "알 수 없음");
    if (state.events && state.events.readyState === EventSource.CLOSED && !state.eventsTimer) {
        state.eventsTimer = setTimeout(connectEvents, RETRY_MS);
    }
}

/**
 * <summary>플로우차트를 만든 뒤 SSE 를 연결한다. 서버가 아직 없으면 잠시 뒤 다시 시도한다.</summary>
 * <returns>Promise (완료 시 없음)</returns>
 */
async function start() {
    const ok = await loadPipeline();
    if (!ok) {
        setConnection("reconnecting");
        setTimeout(start, RETRY_MS);
        return;
    }
    connectEvents();
}

/**
 * <summary>이벤트를 연결하고 첫 데이터를 불러온다.</summary>
 * <returns>없음</returns>
 */
function init() {
    const flow = $("flow");
    flow.addEventListener("click", onFlowClick);
    flow.addEventListener("keydown", onFlowKey);

    for (const button of document.querySelectorAll("[data-feed]")) {
        button.addEventListener("click", onFeedButton);
    }
    $("feed").addEventListener("error", onFeedError);
    $("btn-capture").addEventListener("click", takeCapture);

    const form = $("settings-form");
    form.addEventListener("change", onSettingsChange);
    form.addEventListener("submit", onSettingsSubmit);

    for (const box of document.querySelectorAll("[data-level]")) {
        box.addEventListener("change", onFilterChange);
    }
    $("btn-pause").addEventListener("click", togglePause);
    $("btn-clear").addEventListener("click", clearLog);

    fillSelect($("set-skip"), SKIP_OPTIONS, null);
    fillSelect($("set-imgsz"), IMGSZ_OPTIONS, null);
    setConnection("connecting");
    loadConfig();
    loadCaptures();
    start();
}

init();
