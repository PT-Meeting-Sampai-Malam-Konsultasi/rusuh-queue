package delivery

import (
	"database/sql"
	"net/http"

	vk "github.com/valkey-io/valkey-go"

	adminusecase "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/admin/application/usecase"
	adminhttp "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/admin/infrastructure/http"
	adminpostgres "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/admin/infrastructure/postgres"
	adminvalkey "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/admin/infrastructure/valkey"
	admininbound "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/admin/port/inbound"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/auth"
)

// NewAdmin wires the admin usecase (queue settings in PostgreSQL, applied in
// Valkey). The controller reads settings through it.
func NewAdmin(db *sql.DB, valkey vk.Client) admininbound.IAdminUsecase {
	return adminusecase.NewAdmin(adminpostgres.New(db), adminvalkey.New(valkey))
}

// RegisterAdmin registers adminUpdateQueue on the private admin mux.
func RegisterAdmin(mux *http.ServeMux, uc admininbound.IAdminUsecase, v *auth.AdminVerifier) {
	adminhttp.RegisterRoutes(mux, adminhttp.NewAdmin(uc, v))
}
