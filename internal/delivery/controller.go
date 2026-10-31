package delivery

import (
	"database/sql"

	vk "github.com/valkey-io/valkey-go"

	controllerusecase "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/controller/application/usecase"
	controllerentity "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/controller/domain/entity"
	controllerpostgres "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/controller/infrastructure/postgres"
	controllervalkey "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/controller/infrastructure/valkey"
	controllerinbound "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/controller/port/inbound"
	controlleroutbound "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/controller/port/outbound"
)

// ControllerDeps are the admission controller's dependencies. Settings is the
// admin usecase, passed in through the controller's own narrow outbound port.
type ControllerDeps struct {
	Config   controllerentity.Config
	DB       *sql.DB
	Valkey   vk.Client
	Settings controlleroutbound.ISettings
}

// NewController wires the admission controller; the caller runs it with Run(ctx).
// It has no HTTP routes.
func NewController(d ControllerDeps) controllerinbound.IControllerUsecase {
	return controllerusecase.NewController(d.Config, controllervalkey.New(d.Valkey), controllerpostgres.New(d.DB), d.Settings)
}
