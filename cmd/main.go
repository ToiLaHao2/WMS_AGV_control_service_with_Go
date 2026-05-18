package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/devil/wmss/agv/internal/modules/agv"
)

// Manager quản lý toàn bộ các AGV đang chạy
var (
	agvManager = agv.NewManager()
)


func main() {
	// === HTTP Server để WMS gửi lệnh Execution Plan ===
	mux := http.NewServeMux()

	// WMS gọi endpoint này để giao nhiệm vụ chạy cho AGV
	mux.HandleFunc("/execute", handleExecutePlan)

	addr := ":8081"
	log.Printf("[AGV Control] HTTP Server dang lang nghe tai %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("Loi khoi dong server: %v", err)
	}
}

func handleExecutePlan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse JSON body từ WMS
	var plan agv.ExecutionPlan
	if err := plan.FromJSON(r.Body); err != nil {
		http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Kiểm tra AGV ID
	if plan.AgvID == "" {
		http.Error(w, "agv_id is required", http.StatusBadRequest)
		return
	}

	// Chạy AGV trong goroutine riêng (non-blocking)
	go agvManager.RunAGV(plan)

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"status":"dispatched","agv_id":"%s","steps":%d}`, plan.AgvID, len(plan.Waypoints))
}

