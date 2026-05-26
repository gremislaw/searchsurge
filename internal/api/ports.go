package api

import pb "searchsurge/internal/pb/proto"

type TrendProvider interface {
	GetSnapshotJSON() []byte
	GetSnapshotProto() *pb.GetTopResponse
	UpdateStopList(words []string)
}
