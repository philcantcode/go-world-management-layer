package cuttlefish

import (
	"context"
	"fmt"

	"github.com/philcantcode/go-world-management-layer/internal/drivers/deviceproxy"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

// DeviceProxyGateway adapts the standard loopback ADB protocol gateway to the
// Android target driver's allocation-aware Gateway port.
type DeviceProxyGateway struct{ gateway *deviceproxy.Gateway }

func NewDeviceProxyGateway(config deviceproxy.GatewayConfig) (*DeviceProxyGateway, error) {
	gateway, err := deviceproxy.NewGateway(config)
	if err != nil {
		return nil, err
	}
	return &DeviceProxyGateway{gateway: gateway}, nil
}

func (g *DeviceProxyGateway) Open(ctx context.Context, scope deviceproxy.Scope, allocation Allocation) (ports.ScopedADBEndpoint, error) {
	if g == nil || g.gateway == nil {
		return nil, fmt.Errorf("device proxy gateway is not configured")
	}
	if err := requireExactDeviceScope(scope, allocation); err != nil {
		return nil, err
	}
	return g.gateway.Open(ctx, scope)
}

var _ Gateway = (*DeviceProxyGateway)(nil)
