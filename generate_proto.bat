@echo off
echo [AGV Control] Bat dau build gRPC tu WMS_Contracts...

docker run --rm -v "%cd%:/workspace" -w /workspace rvolosatovs/protoc ^
  -I=./WMS_Contracts ^
  --go_out=. --go_opt=module=github.com/devil/wmss/agv ^
  --go-grpc_out=. --go-grpc_opt=module=github.com/devil/wmss/agv ^
  ./WMS_Contracts/agv.proto ^
  ./WMS_Contracts/wms.proto

if %ERRORLEVEL% equ 0 (
    echo [AGV Control] Build gRPC thanh cong!
) else (
    echo [AGV Control] Loi khi build gRPC!
)
