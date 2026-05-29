package agv

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	pbWms "github.com/devil/wmss/agv/internal/core/contracts/wms_grpc"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
	WmsGrpcURL     string     `json:"wms_grpc_url"`
	WarehouseID    string     `json:"warehouse_id"`
	Waypoints      []Waypoint `json:"waypoints"`
}

// Parse JSON từ request body
func (p *ExecutionPlan) FromJSON(r io.Reader) error {
	return json.NewDecoder(r).Decode(p)
}

// Manager quản lý trạng thái các AGV
type Manager struct {
	mu          sync.Mutex
	agvs        map[string]*AGVState
	kafkaWriter *kafka.Writer
}

type AGVState struct {
	ID           string
	X, Y         int
	NextX, NextY int    // Vị trí chuẩn bị bước vào
	Status       string // "IDLE", "MOVING", "PICKING", "DROPPING"
}

// Số lần tối đa chờ trước khi phát cảnh báo deadlock
const maxCollisionRetries = 4

func NewManager() *Manager {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "localhost:9092"
	}

	// Khởi tạo Kafka Writer
	w := &kafka.Writer{
		Addr:                   kafka.TCP(broker),
		Topic:                  "agv-telemetry",
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
	}

	return &Manager{
		agvs:        make(map[string]*AGVState),
		kafkaWriter: w,
	}
}

// isPositionOccupied kiểm tra xem có AGV nào khác đang đứng hoặc CHUẨN BỊ bước vào vị trí (targetX, targetY) không.
func (m *Manager) isPositionOccupied(targetX, targetY int, excludeAGV string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, state := range m.agvs {
		if id == excludeAGV {
			continue
		}
		if state.Status != "IDLE" {
			if (state.X == targetX && state.Y == targetY) || (state.NextX == targetX && state.NextY == targetY) {
				return true
			}
		}
	}
	return false
}

// tryClaimPosition kiểm tra và giữ chỗ (NextX, NextY) một cách atom.
func (m *Manager) tryClaimPosition(agvID string, targetX, targetY int) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, state := range m.agvs {
		if id == agvID {
			continue
		}
		if state.Status != "IDLE" {
			if (state.X == targetX && state.Y == targetY) || (state.NextX == targetX && state.NextY == targetY) {
				return false
			}
		}
	}
	
	if state, ok := m.agvs[agvID]; ok {
		state.NextX = targetX
		state.NextY = targetY
	}
	return true
}

// waitForClearance chờ cho đến khi ô tiếp theo trống hoặc hết retry.
// Trả về true nếu ô đã trống và ĐÃ GIỮ CHỖ, false nếu hết retry (nghi ngờ deadlock).
func (m *Manager) waitForClearance(agvID string, targetX, targetY int) bool {
	for retry := 0; retry < maxCollisionRetries; retry++ {
		if m.tryClaimPosition(agvID, targetX, targetY) {
			return true // Ô đã trống và đã được lock
		}
		log.Printf("[AGV %s] ⏳ WAITING — Ô (%d,%d) đang bị AGV khác chiếm. Chờ... (lần %d/%d)",
			agvID, targetX, targetY, retry+1, maxCollisionRetries)
		time.Sleep(1 * time.Second)
	}
	log.Printf("[AGV %s] ⚠️  DEADLOCK WARNING — Không thể vào ô (%d,%d) sau %d lần chờ!",
		agvID, targetX, targetY, maxCollisionRetries)
	return false
}

// RunAGV chạy lộ trình của 1 AGV trong goroutine riêng
func (m *Manager) RunAGV(plan ExecutionPlan) {
	m.mu.Lock()
	if _, exists := m.agvs[plan.AgvID]; !exists {
		m.agvs[plan.AgvID] = &AGVState{ID: plan.AgvID, Status: "MOVING", NextX: -1, NextY: -1}
	} else {
		m.agvs[plan.AgvID].Status = "MOVING"
		m.agvs[plan.AgvID].NextX = -1
		m.agvs[plan.AgvID].NextY = -1
	}
	m.mu.Unlock()

	log.Printf("[AGV %s] ===== BAT DAU THUC THI LENH =====", plan.AgvID)
	log.Printf("[AGV %s] Tong so buoc: %d", plan.AgvID, len(plan.Waypoints))

	for i := 0; i < len(plan.Waypoints); i++ {
		wp := plan.Waypoints[i]

		// ═══ KIỂM TRA VA CHẠM TRƯỚC KHI DI CHUYỂN ═══
		// Chỉ kiểm tra khi action là MOVE hoặc RETURN (các action di chuyển thuần túy)
		if wp.Action == "MOVE" || wp.Action == "RETURN" {
			if !m.waitForClearance(plan.AgvID, wp.Position.X, wp.Position.Y) {
				log.Printf("[AGV %s] Bị kẹt cứng! Gọi RequestReplan...", plan.AgvID)
				
				var milestones []*pbWms.Milestone
				hasReturn := false
				var lastReturn *pbWms.Milestone

				for j := i; j < len(plan.Waypoints); j++ {
					a := plan.Waypoints[j].Action
					if a == "PICK_UP" || a == "DROP_OFF" {
						milestones = append(milestones, &pbWms.Milestone{
							Position: &pbWms.Coordinate{
								X: float32(plan.Waypoints[j].Position.X),
								Y: float32(plan.Waypoints[j].Position.Y),
							},
							Action: a,
						})
					} else if a == "RETURN" {
						hasReturn = true
						lastReturn = &pbWms.Milestone{
							Position: &pbWms.Coordinate{
								X: float32(plan.Waypoints[j].Position.X),
								Y: float32(plan.Waypoints[j].Position.Y),
							},
							Action: "RETURN",
						}
					}
				}

				if hasReturn && lastReturn != nil {
					milestones = append(milestones, lastReturn)
				}

				// Gom tọa độ các AGV đang bị kẹt/đứng im (không phải MOVING) làm obstacles
				m.mu.Lock()
				curX, curY := m.agvs[plan.AgvID].X, m.agvs[plan.AgvID].Y
				var obstacles []*pbWms.Coordinate
				for id, state := range m.agvs {
					if id == plan.AgvID {
						continue
					}
					// Chỉ lấy xe KHÔNG đang di chuyển bình thường (IDLE, WAITING, bị kẹt)
					if state.Status != "MOVING" && state.Status != "RETURN" {
						obstacles = append(obstacles, &pbWms.Coordinate{
							X: float32(state.X),
							Y: float32(state.Y),
						})
					}
				}
				m.mu.Unlock()

				newPlan, err := m.requestReplan(plan, curX, curY, milestones, obstacles)
				if err != nil || len(newPlan) == 0 {
					log.Printf("[AGV %s] Lỗi Replan: %v. Đứng chờ...", plan.AgvID, err)
					time.Sleep(2 * time.Second)
					i-- // Lùi i để thử lại bước hiện tại
					continue
				} else {
					log.Printf("[AGV %s] Replan thành công, nhận %d waypoints mới", plan.AgvID, len(newPlan))
					// Thay thế waypoints từ i trở đi bằng newPlan
					plan.Waypoints = append(plan.Waypoints[:i], newPlan...)
					i-- // Lùi i để vòng lặp kế tiếp xử lý waypoint mới (đang ở vị trí i)
					continue
				}
			}
		}

		time.Sleep(1 * time.Second)

		m.mu.Lock()
		state := m.agvs[plan.AgvID]
		state.X = wp.Position.X
		state.Y = wp.Position.Y
		state.NextX = -1
		state.NextY = -1
		state.Status = wp.Action
		m.mu.Unlock()

		switch wp.Action {
		case "PICK_UP":
			log.Printf("[AGV %s] Buoc %d/%d | TAI: (%d,%d) | [GAP HANG]", plan.AgvID, i+1, len(plan.Waypoints), wp.Position.X, wp.Position.Y)
			time.Sleep(2 * time.Second)
		case "DROP_OFF":
			log.Printf("[AGV %s] Buoc %d/%d | TAI: (%d,%d) | [CAT HANG]", plan.AgvID, i+1, len(plan.Waypoints), wp.Position.X, wp.Position.Y)
			time.Sleep(2 * time.Second)
		case "RETURN":
			log.Printf("[AGV %s] Buoc %d/%d | TAI: (%d,%d) | [QUAY VE DOCK]", plan.AgvID, i+1, len(plan.Waypoints), wp.Position.X, wp.Position.Y)
		default:
			log.Printf("[AGV %s] Buoc %d/%d | TAI: (%d,%d) | [DI CHUYEN]", plan.AgvID, i+1, len(plan.Waypoints), wp.Position.X, wp.Position.Y)
		}

		// Bắn tọa độ lên Kafka
		go m.publishTelemetry(plan.AgvID, wp, plan.InboundOrderID, plan)
	}

	m.mu.Lock()
	m.agvs[plan.AgvID].Status = "IDLE"
	m.mu.Unlock()

	log.Printf("[AGV %s] ===== HOAN THANH NHIEM VU =====", plan.AgvID)

	if plan.WmsGrpcURL != "" {
		go m.reportCompletionToWMS(plan.WmsGrpcURL, plan.AgvID, plan.InboundOrderID)
	}
}

func (m *Manager) publishTelemetry(agvID string, wp Waypoint, orderID string, plan ExecutionPlan) {
	payload := map[string]interface{}{
		"agv_id":           agvID,
		"inbound_order_id": orderID,
		"warehouse_id":     plan.WarehouseID,
		"x":                wp.Position.X,
		"y":                wp.Position.Y,
		"action":           wp.Action,
	}
	body, _ := json.Marshal(payload)

	err := m.kafkaWriter.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(agvID), // Dùng AGV ID làm partition key để đảm bảo thứ tự
			Value: body,
		},
	)
	if err != nil {
		log.Printf("[AGV %s] Loi Publish Kafka: %v", agvID, err)
	}
}

func (m *Manager) reportCompletionToWMS(grpcURL, agvID, orderID string) {
	conn, err := grpc.NewClient(grpcURL, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("[AGV %s] Khong the ket noi toi WMS gRPC server: %v", agvID, err)
		return
	}
	defer conn.Close()

	client := pbWms.NewWMSServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.ReportAGVTaskCompleted(ctx, &pbWms.ReportAGVTaskRequest{
		AgvId:          agvID,
		InboundOrderId: orderID,
		Status:         "COMPLETED",
	})

	if err != nil {
		log.Printf("[AGV %s] Loi bao cao hoan thanh qua gRPC: %v", agvID, err)
		return
	}

	if !resp.Success {
		log.Printf("[AGV %s] WMS tra ve that bai: %s", agvID, resp.Message)
		return
	}

	fmt.Printf("[AGV %s] Da bao cao hoan thanh cho WMS qua gRPC thanh cong.\n", agvID)
}

func (m *Manager) requestReplan(plan ExecutionPlan, curX, curY int, milestones []*pbWms.Milestone, obstacles []*pbWms.Coordinate) ([]Waypoint, error) {
	if plan.WmsGrpcURL == "" {
		return nil, fmt.Errorf("khong co WmsGrpcURL")
	}

	conn, err := grpc.NewClient(plan.WmsGrpcURL, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := pbWms.NewWMSServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req := &pbWms.ReplanRequest{
		AgvId:       plan.AgvID,
		WarehouseId: plan.WarehouseID,
		CurrentPosition: &pbWms.Coordinate{
			X: float32(curX),
			Y: float32(curY),
		},
		Milestones: milestones,
		Obstacles:  obstacles,
	}

	resp, err := client.RequestReplan(ctx, req)
	if err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, fmt.Errorf("wms tu choi replan: %s", resp.Message)
	}

	var newWaypoints []Waypoint
	for _, wp := range resp.Waypoints {
		newWaypoints = append(newWaypoints, Waypoint{
			Position: Point{
				X: int(wp.Position.X),
				Y: int(wp.Position.Y),
			},
			Action: wp.Action,
		})
	}
	return newWaypoints, nil
}
