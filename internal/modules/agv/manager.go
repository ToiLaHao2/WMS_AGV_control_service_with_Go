package agv

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

// Tọa độ 1 điểm trên bản đồ
type Point struct {
	X int `json:"x"`
	Y int `json:"y"`
}

// 1 bước trong kế hoạch chạy (tọa độ + hành động tại điểm đó)
type Waypoint struct {
	Position Point  `json:"position"`
	Action   string `json:"action"` // "MOVE", "PICK_UP", "DROP_OFF"
}

// Toàn bộ kế hoạch di chuyển nhận từ WMS
type ExecutionPlan struct {
	AgvID          string     `json:"agv_id"`
	InboundOrderID string     `json:"inbound_order_id"`
	WmsCallbackURL string     `json:"wms_callback_url"`
	Waypoints      []Waypoint `json:"waypoints"`
}

// Parse JSON từ request body
func (p *ExecutionPlan) FromJSON(r io.Reader) error {
	return json.NewDecoder(r).Decode(p)
}

// Manager quản lý trạng thái các AGV
type Manager struct {
	mu   sync.Mutex
	agvs map[string]*AGVState
}

type AGVState struct {
	ID     string
	X, Y   int
	Status string // "IDLE", "MOVING", "PICKING", "DROPPING"
}

func NewManager() *Manager {
	return &Manager{
		agvs: make(map[string]*AGVState),
	}
}

// RunAGV chạy lộ trình của 1 AGV trong goroutine riêng
func (m *Manager) RunAGV(plan ExecutionPlan) {
	m.mu.Lock()
	m.agvs[plan.AgvID] = &AGVState{ID: plan.AgvID, Status: "MOVING"}
	m.mu.Unlock()

	log.Printf("[AGV %s] ===== BAT DAU THUC THI LENH =====", plan.AgvID)
	log.Printf("[AGV %s] Tong so buoc: %d", plan.AgvID, len(plan.Waypoints))

	for i, wp := range plan.Waypoints {
		time.Sleep(1 * time.Second)

		m.mu.Lock()
		state := m.agvs[plan.AgvID]
		state.X = wp.Position.X
		state.Y = wp.Position.Y
		state.Status = wp.Action
		m.mu.Unlock()

		switch wp.Action {
		case "PICK_UP":
			log.Printf("[AGV %s] Buoc %d/%d | TAI: (%d,%d) | [GAP HANG]", plan.AgvID, i+1, len(plan.Waypoints), wp.Position.X, wp.Position.Y)
			time.Sleep(2 * time.Second)
		case "DROP_OFF":
			log.Printf("[AGV %s] Buoc %d/%d | TAI: (%d,%d) | [CAT HANG]", plan.AgvID, i+1, len(plan.Waypoints), wp.Position.X, wp.Position.Y)
			time.Sleep(2 * time.Second)
		default:
			log.Printf("[AGV %s] Buoc %d/%d | TAI: (%d,%d) | [DI CHUYEN]", plan.AgvID, i+1, len(plan.Waypoints), wp.Position.X, wp.Position.Y)
		}

		if plan.WmsCallbackURL != "" {
			go m.reportToWMS(plan.WmsCallbackURL, plan.AgvID, wp, plan.InboundOrderID)
		}
	}

	m.mu.Lock()
	m.agvs[plan.AgvID].Status = "IDLE"
	m.mu.Unlock()

	log.Printf("[AGV %s] ===== HOAN THANH NHIEM VU =====", plan.AgvID)

	if plan.WmsCallbackURL != "" {
		go m.reportCompletionToWMS(plan.WmsCallbackURL, plan.AgvID, plan.InboundOrderID)
	}
}

func (m *Manager) reportToWMS(url, agvID string, wp Waypoint, orderID string) {
	payload := map[string]interface{}{
		"agv_id":           agvID,
		"inbound_order_id": orderID,
		"x":                wp.Position.X,
		"y":                wp.Position.Y,
		"action":           wp.Action,
	}
	body, _ := json.Marshal(payload)
	resp, err := http.Post(url+"/api/agv/update", "application/json", bytes.NewBuffer(body))
	if err != nil {
		log.Printf("[AGV %s] Loi bao cao WMS: %v", agvID, err)
		return
	}
	defer resp.Body.Close()
}

func (m *Manager) reportCompletionToWMS(url, agvID, orderID string) {
	payload := map[string]interface{}{
		"agv_id":           agvID,
		"inbound_order_id": orderID,
		"status":           "COMPLETED",
	}
	body, _ := json.Marshal(payload)
	resp, err := http.Post(url+"/api/agv/complete", "application/json", bytes.NewBuffer(body))
	if err != nil {
		log.Printf("[AGV %s] Loi bao cao hoan thanh WMS: %v", agvID, err)
		return
	}
	defer resp.Body.Close()
	fmt.Printf("[AGV %s] Da bao cao hoan thanh cho WMS.\n", agvID)
}
