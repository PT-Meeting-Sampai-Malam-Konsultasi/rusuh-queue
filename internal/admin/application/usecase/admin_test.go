package usecase

import (
	"context"
	"errors"
	"testing"

	"go.uber.org/mock/gomock"

	mock_outbound "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/_mock/admin/port/outbound"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/admin/domain/entity"
)

var errBoom = errors.New("boom")

func newAdmin(t *testing.T) (*AdminUsecase, *mock_outbound.MockIAdminRepository, *mock_outbound.MockIQueueConfig) {
	ctrl := gomock.NewController(t)
	repo, queue := mock_outbound.NewMockIAdminRepository(ctrl), mock_outbound.NewMockIQueueConfig(ctrl)
	return NewAdmin(repo, queue).(*AdminUsecase), repo, queue
}

func TestQueueSettingsDefaultsUntilSaved(t *testing.T) {
	svc, repo, _ := newAdmin(t)
	repo.EXPECT().GetSettings(gomock.Any(), "evt1").Return(nil, nil)
	if s, err := svc.QueueSettings(context.Background(), "evt1"); err != nil || s != entity.DefaultSettings("evt1") {
		t.Fatalf("settings = %+v, err = %v", s, err)
	}
	saved := entity.Settings{EventID: "evt1", MaxSlots: 10, RateCap: 2, State: "paused"}
	repo.EXPECT().GetSettings(gomock.Any(), "evt1").Return(&saved, nil)
	if s, err := svc.QueueSettings(context.Background(), "evt1"); err != nil || s != saved {
		t.Fatalf("settings = %+v, err = %v", s, err)
	}
	repo.EXPECT().GetSettings(gomock.Any(), "evt1").Return(nil, errBoom)
	if _, err := svc.QueueSettings(context.Background(), "evt1"); !errors.Is(err, errBoom) {
		t.Fatalf("err = %v", err)
	}
}

func TestUpdateQueueSavesThenApplies(t *testing.T) {
	svc, repo, queue := newAdmin(t)
	want := entity.DefaultSettings("evt1")
	want.State = "paused"
	queue.EXPECT().Live(gomock.Any(), "evt1").Return(true, nil)
	repo.EXPECT().GetSettings(gomock.Any(), "evt1").Return(nil, nil)
	gomock.InOrder(
		repo.EXPECT().SaveSettings(gomock.Any(), want, "op_1", "incident").Return(nil),
		queue.EXPECT().Apply(gomock.Any(), want).Return([]byte(`{"state":"paused"}`), nil),
	)
	f, err := svc.UpdateQueue(context.Background(), "evt1", entity.Patch{State: entity.Some("paused"), Reason: "incident"}, "op_1")
	if err != nil || string(f) != `{"state":"paused"}` {
		t.Fatalf("frontier %s, err %v", f, err)
	}
}

func TestUpdateQueueRefusals(t *testing.T) {
	ctx := context.Background()
	pause := entity.Patch{State: entity.Some("paused")}

	svc, _, _ := newAdmin(t)
	if _, err := svc.UpdateQueue(ctx, "bad:id", pause, "op"); !errors.Is(err, entity.ErrUnknownEvent) {
		t.Fatalf("bad id: %v", err)
	}

	for name, tc := range map[string]struct {
		setup func(*mock_outbound.MockIAdminRepository, *mock_outbound.MockIQueueConfig)
		patch entity.Patch
		want  error
	}{
		"live check fails": {func(_ *mock_outbound.MockIAdminRepository, q *mock_outbound.MockIQueueConfig) {
			q.EXPECT().Live(gomock.Any(), "evt1").Return(false, errBoom)
		}, pause, errBoom},
		"not live": {func(_ *mock_outbound.MockIAdminRepository, q *mock_outbound.MockIQueueConfig) {
			q.EXPECT().Live(gomock.Any(), "evt1").Return(false, nil)
		}, pause, entity.ErrUnknownEvent},
		"settings fail": {func(r *mock_outbound.MockIAdminRepository, q *mock_outbound.MockIQueueConfig) {
			q.EXPECT().Live(gomock.Any(), "evt1").Return(true, nil)
			r.EXPECT().GetSettings(gomock.Any(), "evt1").Return(nil, errBoom)
		}, pause, errBoom},
		"invalid patch": {func(r *mock_outbound.MockIAdminRepository, q *mock_outbound.MockIQueueConfig) {
			q.EXPECT().Live(gomock.Any(), "evt1").Return(true, nil)
			r.EXPECT().GetSettings(gomock.Any(), "evt1").Return(nil, nil)
		}, entity.Patch{MaxSlots: entity.Some(int64(0))}, entity.ErrInvalidSettings},
		"save fails": {func(r *mock_outbound.MockIAdminRepository, q *mock_outbound.MockIQueueConfig) {
			q.EXPECT().Live(gomock.Any(), "evt1").Return(true, nil)
			r.EXPECT().GetSettings(gomock.Any(), "evt1").Return(nil, nil)
			r.EXPECT().SaveSettings(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(errBoom)
		}, pause, errBoom},
		"apply fails": {func(r *mock_outbound.MockIAdminRepository, q *mock_outbound.MockIQueueConfig) {
			q.EXPECT().Live(gomock.Any(), "evt1").Return(true, nil)
			r.EXPECT().GetSettings(gomock.Any(), "evt1").Return(nil, nil)
			r.EXPECT().SaveSettings(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
			q.EXPECT().Apply(gomock.Any(), gomock.Any()).Return(nil, errBoom)
		}, pause, errBoom},
		"gone meanwhile": {func(r *mock_outbound.MockIAdminRepository, q *mock_outbound.MockIQueueConfig) {
			q.EXPECT().Live(gomock.Any(), "evt1").Return(true, nil)
			r.EXPECT().GetSettings(gomock.Any(), "evt1").Return(nil, nil)
			r.EXPECT().SaveSettings(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
			q.EXPECT().Apply(gomock.Any(), gomock.Any()).Return(nil, nil)
		}, pause, entity.ErrUnknownEvent},
	} {
		t.Run(name, func(t *testing.T) {
			svc, repo, queue := newAdmin(t)
			tc.setup(repo, queue)
			if _, err := svc.UpdateQueue(ctx, "evt1", tc.patch, "op"); !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}
