# CÁC KHÁI NIỆM CƠ BẢN CỦA GO

## 1. BIẾN VÀ KIỂU DỮ LIỆU
- var: khai báo biến với từ khóa var
- :=: khai báo và gán giá trị ngắn gọn (chỉ dùng trong hàm)
- const: khai báo hằng số
- Kiểu dữ liệu cơ bản: string, int, float64, bool
- Kiểu dữ liệu phức tạp: array, slice, map, struct, pointer

## 2. HÀM (FUNCTIONS)
- func: từ khóa khai báo hàm
- Hàm có thể trả về nhiều giá trị
- Ví dụ: func add(a, b int) int { return a + b }

## 3. CẤU TRÚC ĐIỀU KHIỂN
- if/else: điều kiện
- for: vòng lặp duy nhất trong Go (thay thế while)
- switch: lựa chọn nhiều trường hợp
- break, continue: điều khiển vòng lặp

## 4. MẢNG VÀ SLICE
- Array: kích thước cố định [n]type
- Slice: kích thước động []type
- len(): độ dài
- append(): thêm phần tử vào slice
- make(): tạo slice với kích thước ban đầu

## 5. MAP
- map[keyType]valueType: lưu trữ key-value
- make(map[type]type): tạo map
- delete(map, key): xóa phần tử

## 6. STRUCT
- struct: kiểu dữ liệu tùy chỉnh, nhóm các field
- type Person struct { Name string; Age int }
- Truy cập field: person.Name

## 7. INTERFACE
- Interface: định nghĩa hành vi (method set)
- type Writer interface { Write([]byte) (int, error) }
- Go hỗ trợ duck typing (implicit implementation)

## 8. POINTER
- &variable: lấy địa chỉ
- *pointer: lấy giá trị tại địa chỉ
- Pointer dùng để truyền tham chiếu, tránh copy dữ liệu lớn

## 9. ERROR HANDLING
- error là một interface built-in
- Hàm thường trả về (result, error)
- if err != nil: kiểm tra lỗi
- errors.New(): tạo error mới

## 10. CONCURRENCY
- Goroutine: luồng nhẹ, chạy song song (go func())
- Channel: giao tiếp giữa goroutines (make(chan type))
- select: chờ nhiều channel operations
- mutex: đồng bộ hóa truy cập shared data

## 11. PACKAGE
- package: tổ chức code thành module
- import: nhập package từ bên ngoài
- package main: entry point của chương trình
- func main(): hàm bắt đầu chạy

## 12. CÁC QUY ƯỚC QUAN TRỌNG
- Tên được export (viết hoa) có thể dùng từ package khác
- Tên private (viết thường) chỉ dùng trong package
- Go fmt: format code tự động
- go build: biên dịch
- go run: chạy trực tiếp
- go test: chạy tests

## 13. CÁC PACKAGE QUAN TRỌNG TRONG GO

### fmt - Format I/O
- fmt.Println(): in và xuống dòng
- fmt.Print(): in không xuống dòng
- fmt.Printf(): in với định dạng (%d, %s, %v)
- fmt.Scan(), fmt.Scanf(): đọc input từ bàn phím

### net/http - HTTP Server và Client
- Tạo web server
- Gọi HTTP request
- Xử lý routing

### encoding/json - Xử lý JSON
- json.Marshal(): chuyển struct sang JSON
- json.Unmarshal(): chuyển JSON sang struct

### database/sql - Database
- Kết nối database
- Thực thi SQL queries

### os - Hệ điều hành
- Làm việc với file system
- Environment variables
- Process management

### io - Input/Output cơ bản
- Đọc/ghi dữ liệu stream
- Copy dữ liệu giữa reader/writer

### strings - Xử lý chuỗi
- strings.Contains(): kiểm tra chuỗi con
- strings.Split(): tách chuỗi
- strings.Join(): nối chuỗi

### math - Toán học
- math.Sqrt(): căn bậc 2
- math.Pow(): lũy thừa
- math.Abs(): giá trị tuyệt đối

### time - Thời gian
- time.Now(): thời gian hiện tại
- time.Parse(): parse string sang time
- time.Format(): format time sang string

### log - Logging
- log.Println(): ghi log
- log.Fatal(): ghi log và exit
- log.SetFlags(): cấu hình format log

### context - Context
- Quản lý deadline, cancellation
- Truyền request-scoped values

### sync - Synchronization
- sync.Mutex: khóa mutex
- sync.WaitGroup: đợi goroutines
- sync.Once: chạy một lần