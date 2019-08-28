package vstate

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/leveldorado/vstate/user"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestChangeHandler_UpdateState(t *testing.T) {
	cfg := ChangeHandlerConfig{HandleVehiclesLimit: 100}
	ctx, cancel := context.WithCancel(context.Background())
	utcLocation := time.UTC
	vehicleID := uuid.New().String()
	vehicleID2 := uuid.New().String()
	vRepo := &mockVehiclesRepo{vehicles: []Vehicle{
		{ID: vehicleID, State: Ready, LastStateUpdatedAt: time.Now(), TimeLocation: *utcLocation},
		{ID: vehicleID2, State: Ready, LastStateUpdatedAt: time.Now(), TimeLocation: *utcLocation},
	}}
	lRepo := &mockVehicleHandlingLockRepo{}
	blChecker := &mockBatterLevelChecker{level: 100}
	queue := &mockSubscriberPublisher{}
	tg := &mockTickGetter{lateTickChan: make(chan time.Time), unknownStateChan: make(chan time.Time)}
	wg := &sync.WaitGroup{}
	h, err := NewChangeHandler(ctx, vRepo, lRepo, blChecker, queue, queue, tg, cfg, wg)
	require.NoError(t, err)
	for _, s := range []State{Riding, Bounty, Collected, Dropped, ServiceMode, Unknown, Terminated, Ready} {
		require.NoError(t, h.UpdateState(context.Background(), vehicleID, s, user.RoleAdmin))
		state, err := h.GetState(context.Background(), vehicleID)
		require.NoError(t, err)
		require.Equal(t, s, state)
	}
	for _, s := range []State{Riding, Ready} {
		require.NoError(t, h.UpdateState(context.Background(), vehicleID, s, user.RoleEndUser))
		state, err := h.GetState(context.Background(), vehicleID)
		require.NoError(t, err)
		require.Equal(t, s, state)
	}
	state, _ := h.GetState(context.Background(), vehicleID)
	for _, s := range []State{Bounty, Collected, Dropped, ServiceMode, Unknown, Terminated} {
		require.IsType(t, ErrNotAllowedStateChange{}, h.UpdateState(context.Background(), vehicleID, s, user.RoleEndUser))
	}
	for _, s := range []State{Riding, Ready} {
		require.NoError(t, h.UpdateState(context.Background(), vehicleID, s, user.RoleHunter))
		state, err := h.GetState(context.Background(), vehicleID)
		require.NoError(t, err)
		require.Equal(t, s, state)
	}
	require.IsType(t, ErrNotAllowedStateChange{}, h.UpdateState(context.Background(), vehicleID, Bounty, user.RoleHunter))
	require.NoError(t, h.UpdateState(context.Background(), vehicleID, Bounty, user.RoleAdmin))
	require.NoError(t, h.UpdateState(context.Background(), vehicleID, Collected, user.RoleHunter))
	require.NoError(t, h.UpdateState(context.Background(), vehicleID, Dropped, user.RoleHunter))
	require.NoError(t, h.UpdateState(context.Background(), vehicleID, Ready, user.RoleHunter))
	require.IsType(t, ErrNotAllowedStateChange{}, h.UpdateState(context.Background(), vehicleID, Collected, user.RoleHunter))
	require.IsType(t, ErrNotAllowedStateChange{}, h.UpdateState(context.Background(), vehicleID, Dropped, user.RoleHunter))
	require.NoError(t, h.UpdateState(context.Background(), vehicleID, Riding, user.RoleEndUser))
	blChecker.level = 10
	require.NoError(t, h.UpdateState(context.Background(), vehicleID, Ready, user.RoleEndUser))
	state, err = h.GetState(context.Background(), vehicleID)
	require.Equal(t, Bounty, state)
	require.NoError(t, h.UpdateState(context.Background(), vehicleID, Ready, user.RoleAdmin))
	for _ = range vRepo.vehicles {
		tg.lateTickChan <- time.Now()
	}
	state, err = h.GetState(context.Background(), vehicleID2)
	require.Equal(t, Bounty, state)
	state, err = h.GetState(context.Background(), vehicleID)
	require.Equal(t, Bounty, state)
	for _ = range vRepo.vehicles {
		tg.unknownStateChan <- time.Now()
	}
	state, err = h.GetState(context.Background(), vehicleID)
	require.Equal(t, Unknown, state)
	state, err = h.GetState(context.Background(), vehicleID2)
	require.Equal(t, Unknown, state)
	cancel()
	wg.Wait()
}

type mockTickGetter struct {
	unknownStateChan chan time.Time
	lateTickChan     chan time.Time
}

func (m mockTickGetter) GetUnknownStateTick(lastUpdatedAt time.Time) <-chan time.Time {
	return m.unknownStateChan
}
func (m mockTickGetter) GetLateTick(location time.Location) <-chan time.Time {
	return m.lateTickChan
}

type mockSubscriberPublisher struct {
	m map[string]chan ChangeRequest
}

func (m *mockSubscriberPublisher) Subscribe(ctx context.Context, topic string) (<-chan ChangeRequest, error) {
	if m.m == nil {
		m.m = map[string]chan ChangeRequest{}
	}
	c := make(chan ChangeRequest, 100)
	m.m[topic] = c
	return c, nil
}

func (m mockSubscriberPublisher) Publish(ctx context.Context, topic string, request ChangeRequest) error {
	if m.m[topic] == nil {
		return errors.New("topic is not initialized")
	}
	m.m[topic] <- request
	return nil
}

type mockBatterLevelChecker struct {
	level float64
}

func (m mockBatterLevelChecker) CheckBatteryLevel(ctx context.Context, vehicleID string) (float64, error) {
	return m.level, nil
}

type mockVehicleHandlingLockRepo struct {
	m map[string]bool
}

func (m *mockVehicleHandlingLockRepo) Create(ctx context.Context, doc vehicleHandlingLock) error {
	if m.m == nil {
		m.m = map[string]bool{doc.VehicleID: true}
		return nil
	}
	if m.m[doc.VehicleID] {
		return ErrPrimaryKeyDuplicated{}
	}
	m.m[doc.VehicleID] = true
	return nil
}
func (m *mockVehicleHandlingLockRepo) Delete(ctx context.Context, id string) error {
	delete(m.m, id)
	return nil
}

type mockVehiclesRepo struct {
	vehicles []Vehicle
}

func (m mockVehiclesRepo) ListNotHandled(ctx context.Context, limit int) ([]Vehicle, error) {
	var res []Vehicle
	for _, v := range m.vehicles {
		if v.Handled {
			continue
		}
		res = append(res, v)
	}
	return res, nil
}
func (m *mockVehiclesRepo) UpdateStateAndLastStateUpdatedAt(ctx context.Context, id string, s State, t time.Time) error {
	for i := range m.vehicles {
		if m.vehicles[i].ID == id {
			m.vehicles[i].State = s
			m.vehicles[i].LastStateUpdatedAt = t
		}
	}
	return nil
}
func (m *mockVehiclesRepo) UpdateState(ctx context.Context, id string, s State) error {
	for i := range m.vehicles {
		if m.vehicles[i].ID == id {
			m.vehicles[i].State = s
		}
	}
	return nil
}
func (m *mockVehiclesRepo) UpdateHandled(ctx context.Context, id string, handled bool) error {
	for i := range m.vehicles {
		if m.vehicles[i].ID == id {
			m.vehicles[i].Handled = handled
		}
	}
	return nil
}
func (m mockVehiclesRepo) GetVehicle(ctx context.Context, id string) (Vehicle, error) {
	for _, v := range m.vehicles {
		if v.ID == id {
			return v, nil
		}
	}
	return Vehicle{}, errors.New("not found")
}
