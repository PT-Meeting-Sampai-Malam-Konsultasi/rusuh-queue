package delivery

import (
	"database/sql"

	vk "github.com/valkey-io/valkey-go"

	consumerusecase "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/consumer/application/usecase"
	consumerentity "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/consumer/domain/entity"
	consumerpostgres "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/consumer/infrastructure/postgres"
	consumervalkey "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/consumer/infrastructure/valkey"
	consumerinbound "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/consumer/port/inbound"
)

type ConsumerDeps struct {
	Config consumerentity.Config
	DB     *sql.DB
	Valkey vk.Client
}

// NewConsumer wires the stream:booking consumer; the caller runs it with Run(ctx).
func NewConsumer(d ConsumerDeps) consumerinbound.IConsumerUsecase {
	return consumerusecase.NewConsumer(d.Config, consumervalkey.New(d.Valkey, d.Config), consumerpostgres.New(d.DB))
}
