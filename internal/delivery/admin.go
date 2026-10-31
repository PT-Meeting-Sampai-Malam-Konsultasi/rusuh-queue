package delivery

import (
	"database/sql"
	"net/http"

	adminusecase "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/admin/application/usecase"
	adminhttp "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/admin/infrastructure/http"
	adminpostgres "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/admin/infrastructure/postgres"
	admininbound "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/admin/port/inbound"
)

// NewAdmin wires the admin usecase (queue settings in PostgreSQL). The
// controller reads settings through it.
func NewAdmin(db *sql.DB) admininbound.IAdminUsecase {
	return adminusecase.NewAdmin(adminpostgres.New(db))
}

// RegisterAdmin registers adminUpdateQueue (a 501 stub until M5).
func RegisterAdmin(mux *http.ServeMux, uc admininbound.IAdminUsecase) {
	adminhttp.RegisterRoutes(mux, adminhttp.NewAdmin(uc))
}
