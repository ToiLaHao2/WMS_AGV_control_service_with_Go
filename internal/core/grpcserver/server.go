package grpcserver

import (
	"context"
	"log"
	"net"

	pb "github.com/devil/wmss/agv/internal/core/contracts/agv_grpc"
	"github.com/devil/wmss/agv/internal/modules/agv"
	"google.golang.org/grpc"
)

type AGVServer struct {
	pb.UnimplementedAGVControlServiceServer
	manager *agv.Manager
}

func NewAGVServer(manager *agv.Manager) *AGVServer {
	return &AGVServer{
		manager: manager,
	}
}

func (s *AGVServer) ExecutePlan(ctx context.Context, req *pb.ExecutePlanRequest) (*pb.ExecutePlanResponse, error) {
	log.Printf("[gRPC] Nhan lenh ExecutePlan cho AGV: %s, Order: %s, Waypoints: %d", req.AgvId, req.InboundOrderId, len(req.Waypoints))

	// Chuyen doi tu protobuf struct sang agv.ExecutionPlan
	var waypoints []agv.Waypoint
	for _, wp := range req.Waypoints {
		waypoints = append(waypoints, agv.Waypoint{
			Position: agv.Point{
				X: int(wp.Position.X),
				Y: int(wp.Position.Y),
			},
			Action: wp.Action,
		})
	}

	plan := agv.ExecutionPlan{
		AgvID:          req.AgvId,
		InboundOrderID: req.InboundOrderId,
		WmsCallbackURL: req.WmsCallbackUrl,
		Waypoints:      waypoints,
	}

	// Chay AGV non-blocking
	go s.manager.RunAGV(plan)

	return &pb.ExecutePlanResponse{
		Status: "dispatched",
		AgvId:  req.AgvId,
		Steps:  int32(len(waypoints)),
	}, nil
}

func StartGRPCServer(port string, manager *agv.Manager) {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("[gRPC] Khong the lang nghe tren port %s: %v", port, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterAGVControlServiceServer(grpcServer, NewAGVServer(manager))

	log.Printf("[gRPC] Server dang lang nghe tai %s", port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("[gRPC] Loi khoi dong server: %v", err)
	}
}
