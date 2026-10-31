package http

import (
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/device/port/inbound"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/clientip"
)

type DeviceHandler struct {
	service inbound.IDeviceUsecase
	ip      *clientip.Resolver
}

func NewDevice(s inbound.IDeviceUsecase, ip *clientip.Resolver) *DeviceHandler {
	return &DeviceHandler{service: s, ip: ip}
}
