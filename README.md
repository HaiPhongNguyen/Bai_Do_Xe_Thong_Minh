# Parking Desktop App (Webcam + Serial + LPR)

Chạy giao diện trên máy tính (localhost) và mở thành cửa sổ app bằng **pywebview** (nếu có).

## Cấu trúc
- `parking_app.py`: server + desktop window
- `index.html`: giao diện
- `config.json`: cấu hình webcam/serial
- `data.xlsx`: dữ liệu
- Thư mục runtime: `uploads/`, `in/`, `out/`

## Cài đặt
```bash
pip install flask opencv-python pyserial openpyxl pywebview torch numpy
```

## Chạy
```bash
python parking_app.py
```

## Ghi chú LPR
- Cần thư mục `yolov5/` và model:
  - `model/LP_detector_nano_61.pt`
  - `model/LP_ocr.pt`
- Cần module `function/utils_rotate.py` và `function/helper.py` như codebase của bạn.

## Serial
ESP gửi:
- `detect_in+<RFID>`
- `detect_out+<RFID>`

App sẽ gửi `alarm` khi OUT OCR UNKNOWN hoặc biển số mismatch.
