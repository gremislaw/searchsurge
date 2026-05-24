package api

type TrendProvider interface {
	GetSnapshotJSON() []byte
	UpdateStopList(words []string)
}