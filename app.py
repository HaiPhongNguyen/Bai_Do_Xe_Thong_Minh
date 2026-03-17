from __future__ import annotations

import sys
import os
import json
import math
import threading
import time
from pathlib import Path
from datetime import datetime

# UI
from PySide6 import QtCore, QtGui, QtWidgets

# Camera
import cv2

# Serial
try:
    import serial
    from serial.tools import list_ports
except Exception:
    serial = None
    list_ports = None

# Google Sheets
try:
    import gspread
    from google.oauth2.service_account import Credentials
except Exception:
    gspread = None
    Credentials = None

# LPR deps (keep your existing pipeline)
try:
    import torch
    import function.utils_rotate as utils_rotate
    import function.helper as helper
except Exception:
    torch = None
    utils_rotate = None
    helper = None


# =========================
# Paths / config
# =========================
BASE_DIR = Path(__file__).parent
CONFIG_PATH = BASE_DIR / "config.json"

PRICE_PER_HOUR = 15000  # VND

DEFAULT_CONFIG = {
    "cam_in":  {"device": 0, "width": 1280, "height": 720},
    "cam_out": {"device": 1, "width": 1280, "height": 720},
    "serial":  {"port": "", "baud": 115200},
    # snapshot_mode:
    # - "fresh": khi có lệnh detect_in/out -> lấy frame "tươi" từ stream (thread-safe)
    # - "latest": dùng frame mới nhất từ stream
    "app": {"snapshot_mode": "fresh"},
    # google sheet
    "gsheet": {
        "enabled": True,
        "creds_path": str(BASE_DIR / "service_account.json"),
        "spreadsheet_id": "1FtpRneaEMJZygdqjjd_sM4OzCm38GFpDeEGQKASeKOM",
        "worksheet_title": "Sheet1",
        # nếu mất mạng, ghi tạm vào file queue để retry
        "queue_path": str(BASE_DIR / "gsheet_queue.jsonl"),
    },
}

# YOLOv5 LPR
YOLOV5_DIR = BASE_DIR / "yolov5"
LP_DET_PATH = BASE_DIR / "model" / "LP_detector_nano_61.pt"
LP_OCR_PATH = BASE_DIR / "model" / "LP_ocr.pt"

DET_IMG_SIZE = 960
DET_CONF = 0.25
DET_IOU = 0.45
OCR_CONF = 0.5
OCR_TRIES_ROT = [(0, 0), (0, 1), (1, 0), (1, 1)]
OCR_TRIES_PREP = 3
PAD_RATIO_W = 0.12
PAD_RATIO_H = 0.18


def now_text():
    return datetime.now().strftime("%d/%m/%Y %H:%M:%S")


# =========================
# Config helpers
# =========================
def load_config():
    if not CONFIG_PATH.exists():
        save_config(DEFAULT_CONFIG)
        return json.loads(json.dumps(DEFAULT_CONFIG))
    try:
        cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        for k in ("cam_in", "cam_out"):
            cfg.setdefault(k, {})
            cfg[k].setdefault("device", DEFAULT_CONFIG[k]["device"])
            cfg[k].setdefault("width", DEFAULT_CONFIG[k]["width"])
            cfg[k].setdefault("height", DEFAULT_CONFIG[k]["height"])

        cfg.setdefault("serial", {})
        cfg["serial"].setdefault("port", DEFAULT_CONFIG["serial"]["port"])
        cfg["serial"].setdefault("baud", DEFAULT_CONFIG["serial"]["baud"])

        cfg.setdefault("app", {})
        cfg["app"].setdefault("snapshot_mode", DEFAULT_CONFIG["app"]["snapshot_mode"])

        cfg.setdefault("gsheet", {})
        for kk, vv in DEFAULT_CONFIG["gsheet"].items():
            cfg["gsheet"].setdefault(kk, vv)

        return cfg
    except Exception:
        save_config(DEFAULT_CONFIG)
        return json.loads(json.dumps(DEFAULT_CONFIG))


def save_config(cfg):
    CONFIG_PATH.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")


def dt_to_text(dt: datetime):
    return dt.strftime("%d/%m/%Y %H:%M:%S")


def parse_dt(val):
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    s = str(val).strip()
    if not s:
        return None
    for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    return None


def calc_fee(in_dt: datetime, out_dt: datetime) -> int:
    if not in_dt or not out_dt:
        return 0
    seconds = (out_dt - in_dt).total_seconds()
    if seconds < 0:
        seconds = 0
    hours = int(math.ceil(seconds / 3600.0))
    hours = max(1, hours)
    return hours * PRICE_PER_HOUR


# =========================
# Google Sheets helpers
# =========================
# “Nội dung như bạn đề xuất” -> dùng header như file Excel trước đó
HEADERS = ["STT", "Biển số", "Slot gửi", "Thời gian vào", "Thời gian ra", "Thành tiền", "RFID", "Ghi chú"]

# Cache worksheet để giảm reconnect
_GS_LOCK = threading.Lock()
_GS_CLIENT = None
_GS_WS = None


def _gs_queue_append(payload: dict):
    cfg = load_config()
    qpath = Path(cfg["gsheet"]["queue_path"])
    try:
        qpath.parent.mkdir(parents=True, exist_ok=True)
        with qpath.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception:
        pass


def _gs_open():
    """Return gspread worksheet."""
    global _GS_CLIENT, _GS_WS
    cfg = load_config()
    gcfg = cfg.get("gsheet", {})
    if not gcfg.get("enabled", True):
        raise RuntimeError("Google Sheet disabled in config.json")

    if gspread is None or Credentials is None:
        raise RuntimeError("Missing deps: pip install gspread google-auth")

    creds_path = Path(gcfg["creds_path"])
    if not creds_path.exists():
        raise RuntimeError(f"Missing service account file: {creds_path}")

    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    with _GS_LOCK:
        if _GS_WS is not None:
            return _GS_WS

        creds = Credentials.from_service_account_file(str(creds_path), scopes=scope)
        _GS_CLIENT = gspread.authorize(creds)

        ss = _GS_CLIENT.open_by_key(gcfg["spreadsheet_id"])
        title = gcfg.get("worksheet_title", "Sheet1")
        try:
            ws = ss.worksheet(title)
        except Exception:
            ws = ss.add_worksheet(title=title, rows=2000, cols=20)

        _GS_WS = ws

    _gs_ensure_headers()
    _gs_flush_queue_best_effort()
    return _GS_WS


def _gs_ensure_headers():
    ws = _GS_WS
    if ws is None:
        return
    try:
        row1 = ws.row_values(1)
        if not row1:
            ws.insert_row(HEADERS, 1)
            return
        # nếu header khác thì set lại (nhẹ nhàng)
        if [c.strip() for c in row1[:len(HEADERS)]] != HEADERS:
            ws.update("A1", [HEADERS])
    except Exception:
        pass


def _gs_flush_queue_best_effort():
    """Try to replay queued writes (if any)."""
    cfg = load_config()
    qpath = Path(cfg["gsheet"]["queue_path"])
    if not qpath.exists():
        return
    try:
        lines = qpath.read_text(encoding="utf-8").splitlines()
        if not lines:
            return
        ok_lines = []
        for ln in lines:
            try:
                payload = json.loads(ln)
            except Exception:
                continue
            try:
                _gs_apply_payload(payload)
                ok_lines.append(ln)
            except Exception:
                break

        if ok_lines and len(ok_lines) == len(lines):
            qpath.unlink(missing_ok=True)
        elif ok_lines:
            remain = lines[len(ok_lines):]
            qpath.write_text("\n".join(remain) + ("\n" if remain else ""), encoding="utf-8")
    except Exception:
        pass


def _gs_apply_payload(payload: dict):
    """
    payload types:
      {"type":"append_row","row":[...]}
      {"type":"update_cells","range":"A2:H2","values":[[...]]}
    """
    ws = _GS_WS
    if ws is None:
        raise RuntimeError("Worksheet not ready")
    typ = payload.get("type")
    if typ == "append_row":
        ws.append_row(payload["row"], value_input_option="USER_ENTERED")
    elif typ == "update_cells":
        ws.update(payload["range"], payload["values"], value_input_option="USER_ENTERED")
    else:
        raise RuntimeError("Unknown payload type")


def gs_read_all_rows():
    """
    Return list of dict rows for UI:
      {"stt","plate","slot","in_time","out_time","fee","rfid","note","row_index"}
    """
    ws = _gs_open()
    values = ws.get_all_values()
    if not values or len(values) < 2:
        return []

    header = values[0]
    idx = {h: i for i, h in enumerate(header)}

    def getv(row, key):
        i = idx.get(key, None)
        if i is None or i >= len(row):
            return ""
        return row[i].strip()

    out = []
    for r_i in range(2, len(values) + 1):  # 1-based row index; skip header row 1
        row = values[r_i - 1]
        plate = getv(row, "Biển số")
        if not plate:
            continue
        fee_txt = getv(row, "Thành tiền")
        try:
            fee = int(float(fee_txt)) if fee_txt else 0
        except Exception:
            fee = 0

        out.append({
            "row_index": r_i,
            "stt": getv(row, "STT"),
            "plate": plate,
            "slot": getv(row, "Slot gửi"),
            "in_time": getv(row, "Thời gian vào"),
            "out_time": getv(row, "Thời gian ra"),
            "fee": fee,
            "rfid": getv(row, "RFID"),
            "note": getv(row, "Ghi chú"),
        })
    return out


def gs_next_stt():
    ws = _gs_open()
    col_a = ws.col_values(1)  # STT
    cnt = 0
    for v in col_a[1:]:
        if str(v).strip():
            cnt += 1
    return cnt + 1


def gs_checkin(rfid: str, plate: str, slot: str, in_dt: datetime) -> int:
    _gs_open()
    stt = gs_next_stt()
    row = [
        stt,
        plate,
        slot,
        dt_to_text(in_dt),
        "",
        0,
        rfid,
        "",
    ]
    payload = {"type": "append_row", "row": row}
    try:
        _gs_apply_payload(payload)
    except Exception:
        _gs_queue_append(payload)
        raise
    return stt


def gs_checkout(rfid: str, plate_out: str, out_dt: datetime):
    """
    Find latest row where RFID matches and out_time empty.
    Validate plate mismatch logic like before.
    Update out_time + fee + note.
    Return: ok(bool), msg, plate_in, fee
    """
    ws = _gs_open()
    values = ws.get_all_values()
    if not values or len(values) < 2:
        return False, "Sheet trống", "", 0

    header = values[0]
    idx = {h: i for i, h in enumerate(header)}

    def get_cell(row, key):
        i = idx.get(key, None)
        if i is None or i >= len(row):
            return ""
        return row[i].strip()

    # find latest match from bottom
    target_row_index = None
    plate_in = ""
    in_time_txt = ""
    for r_i in range(len(values), 1, -1):
        row = values[r_i - 1]
        rv = get_cell(row, "RFID")
        outv = get_cell(row, "Thời gian ra")
        if rv == rfid and outv == "":
            target_row_index = r_i
            plate_in = get_cell(row, "Biển số")
            in_time_txt = get_cell(row, "Thời gian vào")
            break

    if target_row_index is None:
        return False, "Không tìm thấy lượt vào", "", 0

    if not plate_out or plate_out.upper() == "UNKNOWN":
        # set note
        note = f"⚠ OUT OCR=UNKNOWN lúc {dt_to_text(out_dt)}"
        _gs_update_row_fields(target_row_index, out_time="", fee="", note=note)
        return False, "Không nhận diện được (OUT)", plate_in, 0

    if plate_in and plate_in.upper() != "UNKNOWN" and plate_in != plate_out:
        note = f"⚠ MISMATCH in={plate_in} out={plate_out} lúc {dt_to_text(out_dt)}"
        _gs_update_row_fields(target_row_index, out_time="", fee="", note=note)
        return False, "Biển số không khớp", plate_in, 0

    in_dt = parse_dt(in_time_txt)
    fee = calc_fee(in_dt, out_dt)
    _gs_update_row_fields(target_row_index, out_time=dt_to_text(out_dt), fee=str(fee), note="")
    return True, "OK", plate_in, fee


def _gs_update_row_fields(row_index: int, out_time: str, fee: str, note: str):
    """
    Update only (Thời gian ra, Thành tiền, Ghi chú) columns by header position.
    """
    ws = _GS_WS
    if ws is None:
        ws = _gs_open()

    header = ws.row_values(1)
    idx = {h: i + 1 for i, h in enumerate(header)}  # 1-based col index

    c_out = idx.get("Thời gian ra", None)
    c_fee = idx.get("Thành tiền", None)
    c_note = idx.get("Ghi chú", None)

    updates = []
    if c_out:
        updates.append((row_index, c_out, out_time))
    if c_fee:
        updates.append((row_index, c_fee, fee))
    if c_note:
        updates.append((row_index, c_note, note))

    if not updates:
        return

    # batch update via ranges
    # Build minimal range like A{row}:H{row} then put row array
    last_col = max(u[1] for u in updates)
    row_vals = ws.row_values(row_index)
    if len(row_vals) < last_col:
        row_vals += [""] * (last_col - len(row_vals))

    for r, c, v in updates:
        row_vals[c - 1] = v

    # update from A..last_col
    start_col_letter = "A"
    end_col_letter = gspread.utils.rowcol_to_a1(1, last_col).rstrip("1")
    rng = f"{start_col_letter}{row_index}:{end_col_letter}{row_index}"
    payload = {"type": "update_cells", "range": rng, "values": [row_vals[:last_col]]}
    try:
        _gs_apply_payload(payload)
    except Exception:
        _gs_queue_append(payload)
        raise


# =========================
# LPR engine (YOLOv5)
# =========================
class LPREngine:
    def __init__(self):
        self._lock = threading.Lock()
        self._ready = False
        self._yolo_det = None
        self._yolo_ocr = None

    def _torch_monkey_patch(self):
        if torch is None:
            return
        try:
            old = torch.load

            def torch_load_unsafe(*args, **kwargs):
                kwargs["weights_only"] = False
                return old(*args, **kwargs)

            torch.load = torch_load_unsafe
        except Exception:
            pass

    def init_if_needed(self) -> bool:
        if self._ready:
            return True
        if torch is None or helper is None or utils_rotate is None:
            print("[LPR] Missing deps -> disabled")
            return False
        if not YOLOV5_DIR.exists() or not LP_DET_PATH.exists() or not LP_OCR_PATH.exists():
            print("[LPR] Missing yolov5/model files -> disabled")
            return False

        self._torch_monkey_patch()
        import sys as _sys
        if str(YOLOV5_DIR) not in _sys.path:
            _sys.path.insert(0, str(YOLOV5_DIR))

        device = "cuda" if torch.cuda.is_available() else "cpu"
        try:
            self._yolo_det = torch.hub.load(str(YOLOV5_DIR), "custom", path=str(LP_DET_PATH), source="local")
            self._yolo_ocr = torch.hub.load(str(YOLOV5_DIR), "custom", path=str(LP_OCR_PATH), source="local")
            self._yolo_det.conf = DET_CONF
            self._yolo_det.iou = DET_IOU
            self._yolo_ocr.conf = OCR_CONF
            self._yolo_det.to(device).eval()
            self._yolo_ocr.to(device).eval()
            self._ready = True
            print(f"[LPR] Ready device={device}")
            return True
        except Exception as e:
            print(f"[LPR] Init failed: {e}")
            return False

    def _clamp(self, x, a, b):
        return max(a, min(b, x))

    def _expand_bbox(self, xmin, ymin, xmax, ymax, W, H, pad_w_ratio=0.1, pad_h_ratio=0.15):
        w = xmax - xmin
        h = ymax - ymin
        pad_w = int(w * pad_w_ratio)
        pad_h = int(h * pad_h_ratio)
        x1 = self._clamp(xmin - pad_w, 0, W - 1)
        y1 = self._clamp(ymin - pad_h, 0, H - 1)
        x2 = self._clamp(xmax + pad_w, 0, W - 1)
        y2 = self._clamp(ymax + pad_h, 0, H - 1)
        if x2 <= x1 or y2 <= y1:
            return xmin, ymin, xmax, ymax
        return x1, y1, x2, y2

    def _preprocess_variants(self, bgr):
        out = [bgr]
        gray = cv2.cvtColor(bgr, cv2.COLOR_BGR2GRAY)
        clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
        g1 = clahe.apply(gray)
        out.append(cv2.cvtColor(g1, cv2.COLOR_GRAY2BGR))

        g2 = cv2.fastNlMeansDenoising(g1, None, h=12, templateWindowSize=7, searchWindowSize=21)
        blur = cv2.GaussianBlur(g2, (0, 0), 1.0)
        sharp = cv2.addWeighted(g2, 1.6, blur, -0.6, 0)
        out.append(cv2.cvtColor(sharp, cv2.COLOR_GRAY2BGR))

        th = cv2.adaptiveThreshold(sharp, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
                                   cv2.THRESH_BINARY, 31, 7)
        out.append(cv2.cvtColor(th, cv2.COLOR_GRAY2BGR))
        return out

    def _score_text(self, lp: str):
        if not lp:
            return 0
        lp = lp.strip()
        if lp == "" or lp.lower() == "unknown":
            return 0
        score = len(lp)
        bad = sum(1 for c in lp if not (c.isalnum() or c in "-."))
        return score - 2 * bad

    def _pick_best_plate(self, crop_bgr):
        best_lp = "UNKNOWN"
        best_score = 0
        variants = self._preprocess_variants(crop_bgr)[:max(1, OCR_TRIES_PREP)]
        for v in variants:
            for cc, ct in OCR_TRIES_ROT:
                try:
                    img = utils_rotate.deskew(v, cc, ct)
                    lp = helper.read_plate(self._yolo_ocr, img)
                except Exception:
                    lp = "UNKNOWN"
                sc = self._score_text(lp)
                if sc > best_score:
                    best_score = sc
                    best_lp = lp
        return best_lp

    def recognize(self, frame_bgr) -> str:
        if frame_bgr is None:
            return "UNKNOWN"
        with self._lock:
            if not self.init_if_needed():
                return "UNKNOWN"
            try:
                H, W = frame_bgr.shape[:2]
                plates = self._yolo_det(frame_bgr, size=DET_IMG_SIZE)
                det = plates.pandas().xyxy[0].values.tolist()

                best = None
                best_metric = -1.0
                for r in det:
                    xmin, ymin, xmax, ymax, conf = r[0], r[1], r[2], r[3], r[4]
                    xmin = int(xmin); ymin = int(ymin); xmax = int(xmax); ymax = int(ymax)
                    xmin = self._clamp(xmin, 0, W - 1); ymin = self._clamp(ymin, 0, H - 1)
                    xmax = self._clamp(xmax, 0, W - 1); ymax = self._clamp(ymax, 0, H - 1)
                    if xmax <= xmin or ymax <= ymin:
                        continue
                    area = (xmax - xmin) * (ymax - ymin)
                    metric = float(conf) * 1000.0 + min(area, 200000) / 500.0
                    if metric > best_metric:
                        best_metric = metric
                        best = (xmin, ymin, xmax, ymax, float(conf))

                if best is None:
                    return "UNKNOWN"

                x1, y1, x2, y2 = self._expand_bbox(best[0], best[1], best[2], best[3], W, H,
                                                   pad_w_ratio=PAD_RATIO_W, pad_h_ratio=PAD_RATIO_H)
                crop = frame_bgr[y1:y2, x1:x2]
                lp = self._pick_best_plate(crop)
                lp = (lp or "UNKNOWN").strip()
                return lp if lp else "UNKNOWN"
            except Exception as e:
                print(f"[LPR] OCR error: {e}")
                return "UNKNOWN"


# =========================
# Camera streaming worker
# =========================
class CameraWorker(QtCore.QObject):
    frame_ready = QtCore.Signal(str, object, object)  # side, frame_bgr, qimage
    status = QtCore.Signal(str, str)                  # side, text

    def __init__(self, side: str, device: int, width: int, height: int):
        super().__init__()
        self.side = side
        self.device = device
        self.width = width
        self.height = height

        self._stop = False
        self._cap = None
        self._cap_lock = threading.Lock()

        self._last_frame_lock = threading.Lock()
        self._last_frame = None

        self._fail_count = 0

    def stop(self):
        self._stop = True
        with self._cap_lock:
            try:
                if self._cap:
                    self._cap.release()
            except Exception:
                pass
            self._cap = None

    def _open_capture(self):
        cap = None
        try:
            if hasattr(cv2, "CAP_DSHOW"):
                cap = cv2.VideoCapture(self.device, cv2.CAP_DSHOW)
            else:
                cap = cv2.VideoCapture(self.device)

            if not cap.isOpened():
                try:
                    cap.release()
                except Exception:
                    pass
                cap = cv2.VideoCapture(self.device)

            if not cap.isOpened():
                return None

            try:
                cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
            except Exception:
                pass
            try:
                cap.set(cv2.CAP_PROP_FOURCC, cv2.VideoWriter_fourcc(*"MJPG"))
            except Exception:
                pass

            cap.set(cv2.CAP_PROP_FRAME_WIDTH, int(self.width))
            cap.set(cv2.CAP_PROP_FRAME_HEIGHT, int(self.height))

            for _ in range(20):
                cap.read()
                time.sleep(0.01)

            return cap
        except Exception:
            try:
                if cap:
                    cap.release()
            except Exception:
                pass
            return None

    @QtCore.Slot()
    def run(self):
        while not self._stop:
            cap = self._open_capture()
            if cap is None:
                self.status.emit(self.side, f"Cannot open cam #{self.device}. Retry...")
                time.sleep(1.0)
                continue

            with self._cap_lock:
                self._cap = cap
            self.status.emit(self.side, f"Streaming cam #{self.device} {self.width}x{self.height}")

            self._fail_count = 0

            try:
                while not self._stop:
                    ok, frame = cap.read()
                    if not ok or frame is None:
                        self._fail_count += 1
                        if self._fail_count >= 30:
                            self.status.emit(self.side, "Camera read failed. Reconnecting...")
                            break
                        time.sleep(0.02)
                        continue

                    self._fail_count = 0

                    with self._last_frame_lock:
                        self._last_frame = frame

                    rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                    h, w, ch = rgb.shape
                    bytes_per_line = ch * w
                    qimg = QtGui.QImage(rgb.data, w, h, bytes_per_line, QtGui.QImage.Format_RGB888).copy()

                    self.frame_ready.emit(self.side, frame, qimg)
                    time.sleep(0.03)
            finally:
                with self._cap_lock:
                    try:
                        if self._cap:
                            self._cap.release()
                    except Exception:
                        pass
                    self._cap = None


# =========================
# Serial worker
# =========================
class SerialWorker(QtCore.QObject):
    log = QtCore.Signal(str)
    detect = QtCore.Signal(str, str, str)  # side(in/out), rfid, slot

    def __init__(self):
        super().__init__()
        self._stop = False
        self._ser = None
        self._lock = threading.Lock()

    def stop(self):
        self._stop = True
        with self._lock:
            try:
                if self._ser:
                    self._ser.close()
            except Exception:
                pass
            self._ser = None

    def send_line(self, s: str):
        with self._lock:
            if not self._ser:
                return False
            try:
                self._ser.write((s.strip() + "\n").encode("utf-8", errors="ignore"))
                self._ser.flush()
                return True
            except Exception:
                return False

    @QtCore.Slot(str, int)
    def apply_config(self, port: str, baud: int):
        with self._lock:
            try:
                if self._ser:
                    self._ser.close()
            except Exception:
                pass
            self._ser = None
        self.log.emit(f"[SERIAL] Config updated: {port} @ {baud}")

    @QtCore.Slot()
    def run(self):
        if serial is None:
            self.log.emit("[SERIAL] pyserial not installed.")
            return

        while not self._stop:
            cfg = load_config()
            port = (os.environ.get("SERIAL_PORT") or cfg.get("serial", {}).get("port", "")).strip()
            baud = int(os.environ.get("SERIAL_BAUD") or cfg.get("serial", {}).get("baud", 115200))

            if not port:
                time.sleep(0.5)
                continue

            try:
                self.log.emit(f"[SERIAL] Opening {port} @ {baud} ...")
                ser = serial.Serial(port, baudrate=baud, timeout=1)
                with self._lock:
                    self._ser = ser
                self.log.emit("[SERIAL] Connected.")

                while not self._stop:
                    line = ser.readline().decode(errors="ignore").strip()
                    if not line:
                        continue

                    parts = line.split("+")
                    if len(parts) >= 2:
                        cmd = parts[0].strip().lower()
                        rfid = parts[1].strip()
                        slot = parts[2].strip() if len(parts) >= 3 else ""
                        if cmd == "detect_in" and rfid:
                            self.log.emit(f"[SERIAL] {line}")
                            self.detect.emit("in", rfid, slot)
                            continue
                        if cmd == "detect_out" and rfid:
                            self.log.emit(f"[SERIAL] {line}")
                            self.detect.emit("out", rfid, slot)
                            continue

                    self.log.emit(f"[SERIAL] {line}")

            except Exception as e:
                self.log.emit(f"[SERIAL] Error: {e} (retry)")
                time.sleep(1.5)
            finally:
                with self._lock:
                    try:
                        if self._ser:
                            self._ser.close()
                    except Exception:
                        pass
                    self._ser = None


# =========================
# Settings dialog
# =========================
class SettingsDialog(QtWidgets.QDialog):
    saved = QtCore.Signal(dict)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Cấu hình hệ thống")
        self.setModal(True)
        self.setMinimumWidth(560)

        cfg = load_config()

        lay = QtWidgets.QVBoxLayout(self)
        form = QtWidgets.QFormLayout()
        lay.addLayout(form)

        self.cb_cam_in = QtWidgets.QComboBox()
        self.cb_cam_out = QtWidgets.QComboBox()
        for i in self._probe_cams(10):
            self.cb_cam_in.addItem(f"Webcam #{i}", i)
            self.cb_cam_out.addItem(f"Webcam #{i}", i)

        self.cb_cam_in.setCurrentIndex(max(0, self.cb_cam_in.findData(cfg["cam_in"]["device"])))
        self.cb_cam_out.setCurrentIndex(max(0, self.cb_cam_out.findData(cfg["cam_out"]["device"])))

        self.cb_in_res = QtWidgets.QComboBox()
        self.cb_out_res = QtWidgets.QComboBox()
        for r in ["1280x720", "1920x1080", "640x480"]:
            self.cb_in_res.addItem(r)
            self.cb_out_res.addItem(r)
        self.cb_in_res.setCurrentText(f'{cfg["cam_in"]["width"]}x{cfg["cam_in"]["height"]}')
        self.cb_out_res.setCurrentText(f'{cfg["cam_out"]["width"]}x{cfg["cam_out"]["height"]}')

        self.cb_port = QtWidgets.QComboBox()
        self.cb_baud = QtWidgets.QComboBox()
        for b in [9600, 19200, 38400, 57600, 115200, 230400, 460800, 921600]:
            self.cb_baud.addItem(str(b), b)

        self._load_ports()
        if cfg["serial"]["port"]:
            idx = self.cb_port.findData(cfg["serial"]["port"])
            if idx >= 0:
                self.cb_port.setCurrentIndex(idx)
        self.cb_baud.setCurrentText(str(cfg["serial"]["baud"]))

        self.cb_snap_mode = QtWidgets.QComboBox()
        self.cb_snap_mode.addItem("Fresh (lấy frame tươi từ stream)", "fresh")
        self.cb_snap_mode.addItem("Latest (dùng frame mới nhất)", "latest")
        cur_mode = (cfg.get("app", {}).get("snapshot_mode", "fresh") or "fresh").strip().lower()
        idxm = self.cb_snap_mode.findData(cur_mode)
        self.cb_snap_mode.setCurrentIndex(idxm if idxm >= 0 else 0)

        # Google sheet status (read-only)
        self.lbl_gs = QtWidgets.QLabel("Google Sheet: ON" if cfg["gsheet"].get("enabled", True) else "Google Sheet: OFF")
        self.lbl_gs.setStyleSheet("color:#667085;")

        form.addRow("Webcam vào", self.cb_cam_in)
        form.addRow("Webcam ra", self.cb_cam_out)
        form.addRow("Độ phân giải (IN)", self.cb_in_res)
        form.addRow("Độ phân giải (OUT)", self.cb_out_res)
        form.addRow("Serial port (ESP32)", self.cb_port)
        form.addRow("Baudrate", self.cb_baud)
        form.addRow("Snapshot mode", self.cb_snap_mode)
        form.addRow("Cloud", self.lbl_gs)

        btns = QtWidgets.QHBoxLayout()
        lay.addLayout(btns)
        btns.addStretch(1)
        self.btn_refresh = QtWidgets.QPushButton("Refresh COM")
        self.btn_save = QtWidgets.QPushButton("Lưu")
        self.btn_cancel = QtWidgets.QPushButton("Hủy")
        btns.addWidget(self.btn_refresh)
        btns.addWidget(self.btn_save)
        btns.addWidget(self.btn_cancel)

        self.btn_refresh.clicked.connect(self._load_ports)
        self.btn_cancel.clicked.connect(self.reject)
        self.btn_save.clicked.connect(self._on_save)

    def _probe_cams(self, n=10):
        ok = []
        for i in range(n):
            cap = None
            try:
                if hasattr(cv2, "CAP_DSHOW"):
                    cap = cv2.VideoCapture(i, cv2.CAP_DSHOW)
                else:
                    cap = cv2.VideoCapture(i)
                if cap.isOpened():
                    ok.append(i)
            except Exception:
                pass
            finally:
                try:
                    if cap:
                        cap.release()
                except Exception:
                    pass
        return ok

    def _load_ports(self):
        self.cb_port.clear()
        self.cb_port.addItem("(Chưa chọn)", "")
        if list_ports is None:
            return
        for p in list_ports.comports():
            self.cb_port.addItem(f"{p.device} — {p.description}", p.device)

    def _on_save(self):
        cfg = load_config()

        in_dev = int(self.cb_cam_in.currentData())
        out_dev = int(self.cb_cam_out.currentData())
        in_w, in_h = map(int, self.cb_in_res.currentText().split("x"))
        out_w, out_h = map(int, self.cb_out_res.currentText().split("x"))

        port = str(self.cb_port.currentData() or "").strip()
        baud = int(self.cb_baud.currentData())

        snap_mode = str(self.cb_snap_mode.currentData() or "fresh").strip().lower()

        cfg["cam_in"] = {"device": in_dev, "width": in_w, "height": in_h}
        cfg["cam_out"] = {"device": out_dev, "width": out_w, "height": out_h}
        cfg["serial"] = {"port": port, "baud": baud}
        cfg.setdefault("app", {})
        cfg["app"]["snapshot_mode"] = snap_mode

        save_config(cfg)
        self.saved.emit(cfg)
        self.accept()


# =========================
# Main Window
# =========================
class MainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Smart Parking System (Google Sheet)")
        self.resize(1200, 720)

        self.cfg = load_config()
        self.lpr = LPREngine()

        self._last_frame = {"in": None, "out": None}

        central = QtWidgets.QWidget()
        self.setCentralWidget(central)
        root = QtWidgets.QVBoxLayout(central)
        root.setContentsMargins(16, 16, 16, 16)
        root.setSpacing(12)

        top = QtWidgets.QHBoxLayout()
        root.addLayout(top)

        title = QtWidgets.QLabel("🚗 Smart Parking System")
        font = title.font()
        font.setPointSize(16)
        font.setBold(True)
        title.setFont(font)

        self.lbl_time = QtWidgets.QLabel(now_text())
        self.lbl_time.setStyleSheet("color:#667085;")

        self.btn_settings = QtWidgets.QPushButton("⚙ Settings")
        self.btn_settings.clicked.connect(self.open_settings)

        top.addWidget(title)
        top.addStretch(1)
        top.addWidget(self.lbl_time)
        top.addSpacing(12)
        top.addWidget(self.btn_settings)

        kpi = QtWidgets.QHBoxLayout()
        root.addLayout(kpi)
        self.kpi_total = self._kpi_card("Tổng xe", "0")
        self.kpi_parked = self._kpi_card("Đang đỗ", "0")
        self.kpi_free = self._kpi_card("Còn trống", "0")
        kpi.addWidget(self.kpi_total)
        kpi.addWidget(self.kpi_parked)
        kpi.addWidget(self.kpi_free)

        main = QtWidgets.QHBoxLayout()
        root.addLayout(main, 1)

        left = QtWidgets.QVBoxLayout()
        main.addLayout(left, 1)

        self.search = QtWidgets.QLineEdit()
        self.search.setPlaceholderText("Tìm biển số...")
        self.search.textChanged.connect(self.refresh_list)
        left.addWidget(self.search)

        self.list = QtWidgets.QListWidget()
        left.addWidget(self.list, 1)

        right = QtWidgets.QVBoxLayout()
        main.addLayout(right, 2)

        cams = QtWidgets.QHBoxLayout()
        right.addLayout(cams, 2)

        self.cam_in_view = self._cam_card("Camera vào")
        self.cam_out_view = self._cam_card("Camera ra")
        cams.addWidget(self.cam_in_view["box"])
        cams.addWidget(self.cam_out_view["box"])

        pay = QtWidgets.QGroupBox("Thanh toán")
        pay_lay = QtWidgets.QVBoxLayout(pay)

        self.lbl_pay_plate = QtWidgets.QLabel("Biển số: —")
        self.lbl_pay_status = QtWidgets.QLabel("Trạng thái: —")
        self.lbl_pay_time = QtWidgets.QLabel("Thời gian: —")
        self.lbl_pay_fee = QtWidgets.QLabel("Phí: 0 VND")

        pay_lay.addWidget(self.lbl_pay_plate)
        pay_lay.addWidget(self.lbl_pay_status)
        pay_lay.addWidget(self.lbl_pay_time)
        pay_lay.addWidget(self.lbl_pay_fee)

        right.addWidget(pay, 1)

        self.log = QtWidgets.QPlainTextEdit()
        self.log.setReadOnly(True)
        self.log.setMaximumHeight(180)
        root.addWidget(self.log)

        self.timer_clock = QtCore.QTimer(self)
        self.timer_clock.timeout.connect(lambda: self.lbl_time.setText(now_text()))
        self.timer_clock.start(1000)

        self.timer_dash = QtCore.QTimer(self)
        self.timer_dash.timeout.connect(self.refresh_dashboard)
        self.timer_dash.start(2000)

        self.cam_threads = {}
        self.cam_workers = {}
        self.start_cameras()

        self.serial_thread = QtCore.QThread(self)
        self.serial_worker = SerialWorker()
        self.serial_worker.moveToThread(self.serial_thread)
        self.serial_worker.log.connect(self.append_log)
        self.serial_worker.detect.connect(self.on_detect_request)
        self.serial_thread.started.connect(self.serial_worker.run)
        self.serial_thread.start()

        # warm gsheet
        try:
            _gs_open()
            self.append_log("[GSHEET] Connected OK.")
        except Exception as e:
            self.append_log(f"[GSHEET] Not ready: {e}")

        self.append_log("[APP] Ready. Streaming cams. Waiting detect_in/detect_out from ESP32...")

    def closeEvent(self, event):
        try:
            for w in self.cam_workers.values():
                w.stop()
            for t in self.cam_threads.values():
                t.quit()
                t.wait(800)
        except Exception:
            pass
        try:
            self.serial_worker.stop()
            self.serial_thread.quit()
            self.serial_thread.wait(800)
        except Exception:
            pass
        super().closeEvent(event)

    def _kpi_card(self, label: str, value: str):
        box = QtWidgets.QGroupBox(label)
        lay = QtWidgets.QVBoxLayout(box)
        lbl = QtWidgets.QLabel(value)
        f = lbl.font()
        f.setPointSize(22)
        f.setBold(True)
        lbl.setFont(f)
        lbl.setObjectName("value")
        lay.addWidget(lbl)
        box._value_label = lbl
        return box

    def _cam_card(self, title: str):
        box = QtWidgets.QGroupBox(title)
        lay = QtWidgets.QVBoxLayout(box)

        view = QtWidgets.QLabel()
        view.setMinimumSize(480, 270)
        view.setAlignment(QtCore.Qt.AlignCenter)
        view.setStyleSheet("background:#111827;color:#fff;border-radius:8px;")
        lay.addWidget(view)

        meta = QtWidgets.QLabel("Chưa có ảnh")
        meta.setStyleSheet("color:#667085;")
        lay.addWidget(meta)

        return {"box": box, "view": view, "meta": meta}

    def append_log(self, s: str):
        self.log.appendPlainText(f"{now_text()} {s}")

    def start_cameras(self):
        self.stop_cameras()
        cfg = load_config()
        self.cfg = cfg

        self._start_cam_thread("in", cfg["cam_in"]["device"], cfg["cam_in"]["width"], cfg["cam_in"]["height"])
        self._start_cam_thread("out", cfg["cam_out"]["device"], cfg["cam_out"]["width"], cfg["cam_out"]["height"])

    def _start_cam_thread(self, side: str, device: int, w: int, h: int):
        th = QtCore.QThread(self)
        worker = CameraWorker(side, device, w, h)
        worker.moveToThread(th)
        th.started.connect(worker.run)
        worker.frame_ready.connect(self.on_frame_ready)
        worker.status.connect(self.on_cam_status)
        th.start()
        self.cam_threads[side] = th
        self.cam_workers[side] = worker
        self.append_log(f"[CAM {side}] start device={device} {w}x{h}")

    def stop_cameras(self):
        for w in self.cam_workers.values():
            try:
                w.stop()
            except Exception:
                pass
        for t in self.cam_threads.values():
            try:
                t.quit()
                t.wait(800)
            except Exception:
                pass
        self.cam_workers = {}
        self.cam_threads = {}

    @QtCore.Slot(str, str)
    def on_cam_status(self, side: str, text: str):
        self.append_log(f"[CAM {side}] {text}")
        if side == "in":
            self.cam_in_view["meta"].setText(text)
        else:
            self.cam_out_view["meta"].setText(text)

    @QtCore.Slot(str, object, object)
    def on_frame_ready(self, side: str, frame_bgr, qimg: QtGui.QImage):
        self._last_frame[side] = frame_bgr

        pix = QtGui.QPixmap.fromImage(qimg).scaled(
            640, 360, QtCore.Qt.KeepAspectRatio, QtCore.Qt.SmoothTransformation
        )
        if side == "in":
            self.cam_in_view["view"].setPixmap(pix)
        else:
            self.cam_out_view["view"].setPixmap(pix)

    def refresh_dashboard(self):
        try:
            rows = gs_read_all_rows()
            parked = [x for x in rows if not x["out_time"]]
            total = len(rows)
            parked_count = len(parked)
            free = 0  # nếu có tổng slot thì tính vào đây

            self.kpi_total._value_label.setText(str(total))
            self.kpi_parked._value_label.setText(str(parked_count))
            self.kpi_free._value_label.setText(str(free))

            self._all_rows = rows
            self._parked_rows = parked
            self.refresh_list()
        except Exception as e:
            self.append_log(f"[DASH] Error: {e}")

    def refresh_list(self):
        t = (self.search.text() or "").strip().lower()
        self.list.clear()
        rows = getattr(self, "_parked_rows", [])
        for x in rows:
            plate = x.get("plate", "")
            if t and t not in plate.lower():
                continue
            slot = x.get("slot", "")
            it = QtWidgets.QListWidgetItem(f"{plate}   | Slot: {slot} | {x.get('in_time','')}")
            self.list.addItem(it)

    def open_settings(self):
        dlg = SettingsDialog(self)
        dlg.saved.connect(self.on_settings_saved)
        dlg.exec()

    @QtCore.Slot(dict)
    def on_settings_saved(self, cfg: dict):
        self.append_log("[SETTINGS] Saved. Restarting cameras / serial config...")
        self.start_cameras()

        port = cfg.get("serial", {}).get("port", "")
        baud = int(cfg.get("serial", {}).get("baud", 115200))
        QtCore.QMetaObject.invokeMethod(
            self.serial_worker, "apply_config",
            QtCore.Qt.QueuedConnection,
            QtCore.Q_ARG(str, port),
            QtCore.Q_ARG(int, baud),
        )

    # =========================
    # detect_in/out => snapshot => OCR => write GoogleSheet => send to ESP32
    # =========================
    @QtCore.Slot(str, str, str)
    def on_detect_request(self, side: str, rfid: str, slot: str):
        cfg = load_config()
        mode = (cfg.get("app", {}).get("snapshot_mode", "fresh") or "fresh").strip().lower()

        def latest_frame_copy():
            f = self._last_frame.get(side, None)
            return None if f is None else f.copy()

        def fresh_from_stream(delay_s: float = 0.15):
            f1 = latest_frame_copy()
            if f1 is None:
                return None
            time.sleep(delay_s)
            f2 = latest_frame_copy()
            return f2 if f2 is not None else f1

        if mode == "latest":
            snap = latest_frame_copy()
            if snap is None:
                snap = fresh_from_stream()
        else:
            snap = fresh_from_stream()
            if snap is None:
                snap = latest_frame_copy()

        if snap is None:
            self.append_log(f"[{side.upper()}] NO_FRAME (mode={mode})")
            self.serial_worker.send_line(f"{side}_err+{rfid}+NO_FRAME")
            return

        # resize nhẹ nếu quá lớn
        try:
            h, w = snap.shape[:2]
            if max(w, h) > 1400:
                scale = 1400 / max(w, h)
                snap = cv2.resize(snap, (int(w * scale), int(h * scale)))
        except Exception:
            pass

        tries = 3 if side == "out" else 2
        plate = "UNKNOWN"
        for i in range(tries):
            self.append_log(f"[{side.upper()}] OCR try {i+1}/{tries} rfid={rfid} slot={slot} mode={mode} ...")
            plate = self.lpr.recognize(snap)
            if plate and plate.strip().upper() != "UNKNOWN":
                break
            snap2 = fresh_from_stream(0.18)
            if snap2 is not None:
                snap = snap2

        ts = datetime.now()

        if side == "in":
            self.cam_in_view["meta"].setText(f"IN • {dt_to_text(ts)} • Plate: {plate}")
        else:
            self.cam_out_view["meta"].setText(f"OUT • {dt_to_text(ts)} • Plate: {plate}")

        # OCR fail => báo ESP để KHÔNG mở barrie
        if not plate or plate.strip().upper() == "UNKNOWN":
            if side == "in":
                self.serial_worker.send_line(f"in_plate+{rfid}+UNKNOWN+{slot}+0")
            else:
                self.serial_worker.send_line(f"alarm+{rfid}++UNKNOWN+OCR_FAIL")
            self.append_log(f"[{side.upper()}] OCR=UNKNOWN -> sent fail to ESP32")
            return

        # Write Google Sheet + send back
        if side == "in":
            try:
                stt = gs_checkin(rfid=rfid, plate=plate, slot=slot, in_dt=ts)
                self.serial_worker.send_line(f"in_plate+{rfid}+{plate}+{slot}+{stt}")
                self.append_log(f"[IN] OK plate={plate} slot={slot} stt={stt}")
            except Exception as e:
                self.serial_worker.send_line(f"in_err+{rfid}+GSHEET")
                self.append_log(f"[IN] GSHEET error: {e}")

        else:
            try:
                ok, msg, plate_in, fee = gs_checkout(rfid=rfid, plate_out=plate, out_dt=ts)
                if ok:
                    self.serial_worker.send_line(f"out_plate+{rfid}+{plate}+{slot}+{fee}")
                    self.append_log(f"[OUT] OK plate={plate} fee={fee}")

                    self.lbl_pay_plate.setText(f"Biển số: {plate}")
                    self.lbl_pay_status.setText("Trạng thái: Đã thanh toán")
                    self.lbl_pay_time.setText(f"Thời gian ra: {dt_to_text(ts)}")
                    self.lbl_pay_fee.setText(f"Phí: {fee:,} VND".replace(",", "."))
                else:
                    self.serial_worker.send_line(f"alarm+{rfid}+{plate_in}+{plate}+{msg}")
                    self.append_log(f"[OUT] FAIL {msg} in={plate_in} out={plate}")
            except Exception as e:
                self.serial_worker.send_line(f"out_err+{rfid}+GSHEET")
                self.append_log(f"[OUT] GSHEET error: {e}")


def main():
    app = QtWidgets.QApplication(sys.argv)
    w = MainWindow()
    w.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
