package controller

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"pentagi/pkg/config"
	"pentagi/pkg/database"
	"pentagi/pkg/graph/subscriptions"
	"pentagi/pkg/providers/provider"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeFlowQuerier implements database.Querier methods needed for flow concurrency tests.
type fakeFlowQuerier struct {
	database.Querier

	mx          sync.Mutex
	nextID      int64
	flows       map[int64]database.Flow
	statusCalls []database.UpdateFlowStatusParams
	createCalls []database.CreateFlowParams
}

func newFakeFlowQuerier() *fakeFlowQuerier {
	return &fakeFlowQuerier{
		nextID: 1,
		flows:  make(map[int64]database.Flow),
	}
}

func (q *fakeFlowQuerier) CreateFlow(ctx context.Context, arg database.CreateFlowParams) (database.Flow, error) {
	q.mx.Lock()
	defer q.mx.Unlock()

	id := q.nextID
	q.nextID++

	flow := database.Flow{
		ID:                id,
		Status:            arg.Status,
		Title:             arg.Title,
		Model:             arg.Model,
		ModelProviderName: arg.ModelProviderName,
		ModelProviderType: arg.ModelProviderType,
		Language:          arg.Language,
		UserID:            arg.UserID,
		CreatedAt:         time.Now(),
		UpdatedAt:         time.Now(),
	}
	q.flows[id] = flow
	q.createCalls = append(q.createCalls, arg)
	return flow, nil
}

func (q *fakeFlowQuerier) GetUserFlow(ctx context.Context, arg database.GetUserFlowParams) (database.Flow, error) {
	q.mx.Lock()
	defer q.mx.Unlock()

	flow, ok := q.flows[arg.ID]
	if !ok {
		return database.Flow{}, fmt.Errorf("flow not found")
	}
	return flow, nil
}

func (q *fakeFlowQuerier) UpdateFlowStatus(ctx context.Context, arg database.UpdateFlowStatusParams) (database.Flow, error) {
	q.mx.Lock()
	defer q.mx.Unlock()

	flow, ok := q.flows[arg.ID]
	if !ok {
		return database.Flow{}, fmt.Errorf("flow not found")
	}
	flow.Status = arg.Status
	flow.UpdatedAt = time.Now()
	q.flows[arg.ID] = flow
	q.statusCalls = append(q.statusCalls, arg)
	return flow, nil
}

func (q *fakeFlowQuerier) GetFlowContainers(ctx context.Context, flowID int64) ([]database.Container, error) {
	return nil, nil
}

func (q *fakeFlowQuerier) GetFlows(ctx context.Context) ([]database.Flow, error) {
	q.mx.Lock()
	defer q.mx.Unlock()

	res := make([]database.Flow, 0, len(q.flows))
	for _, f := range q.flows {
		res = append(res, f)
	}
	return res, nil
}

// fakeFlowWorkerStub is a mock FlowWorker for concurrency tests.
type fakeFlowWorkerStub struct {
	flowID    int64
	userID    int64
	title     string
	status    database.FlowStatus
	mx        sync.Mutex
	onFinish  func()
	onStop    func()
	fcContext *FlowContext
}

func newFakeFlowWorkerStub(flowID, userID int64, status database.FlowStatus) *fakeFlowWorkerStub {
	return &fakeFlowWorkerStub{
		flowID: flowID,
		userID: userID,
		title:  fmt.Sprintf("flow-%d", flowID),
		status: status,
	}
}

func (w *fakeFlowWorkerStub) GetFlowID() int64 { return w.flowID }
func (w *fakeFlowWorkerStub) GetUserID() int64 { return w.userID }
func (w *fakeFlowWorkerStub) GetTitle() string  { return w.title }
func (w *fakeFlowWorkerStub) GetContext() *FlowContext {
	return w.fcContext
}
func (w *fakeFlowWorkerStub) GetStatus(ctx context.Context) (database.FlowStatus, error) {
	w.mx.Lock()
	defer w.mx.Unlock()
	return w.status, nil
}
func (w *fakeFlowWorkerStub) SetStatus(ctx context.Context, status database.FlowStatus) error {
	w.mx.Lock()
	defer w.mx.Unlock()
	w.status = status
	return nil
}
func (w *fakeFlowWorkerStub) AddAssistant(ctx context.Context, aw AssistantWorker) error { return nil }
func (w *fakeFlowWorkerStub) GetAssistant(ctx context.Context, assistantID int64) (AssistantWorker, error) {
	return nil, fmt.Errorf("not found")
}
func (w *fakeFlowWorkerStub) DeleteAssistant(ctx context.Context, assistantID int64) error { return nil }
func (w *fakeFlowWorkerStub) ListAssistants(ctx context.Context) []AssistantWorker           { return nil }
func (w *fakeFlowWorkerStub) ListTasks(ctx context.Context) []TaskWorker                    { return nil }
func (w *fakeFlowWorkerStub) PutInput(ctx context.Context, input string, prv provider.Provider, resources []database.UserResource) error {
	return nil
}
func (w *fakeFlowWorkerStub) PutResources(ctx context.Context, resources []database.UserResource) error {
	return nil
}
func (w *fakeFlowWorkerStub) Finish(ctx context.Context) error {
	w.mx.Lock()
	w.status = database.FlowStatusFinished
	onFin := w.onFinish
	w.mx.Unlock()
	if onFin != nil {
		onFin()
	}
	return nil
}
func (w *fakeFlowWorkerStub) Stop(ctx context.Context) error {
	w.mx.Lock()
	w.status = database.FlowStatusFailed
	onSt := w.onStop
	w.mx.Unlock()
	if onSt != nil {
		onSt()
	}
	return nil
}
func (w *fakeFlowWorkerStub) Rename(ctx context.Context, title string) error {
	w.title = title
	return nil
}
func (w *fakeFlowWorkerStub) WaitTaskCompletion(ctx context.Context) error { return nil }
func (w *fakeFlowWorkerStub) InvalidateTaskSubtasks(ctx context.Context, taskID int64, subtaskIDs []int64) {
}

func newTestConcurrencyFlowController(
	q *fakeFlowQuerier,
	maxConcurrent int,
) (*flowController, *cascadeFakePublisher) {
	pub := &cascadeFakePublisher{}
	cfg := &config.Config{
		MaxConcurrentFlows: maxConcurrent,
	}
	fc := &flowController{
		db:    q,
		mx:    &sync.Mutex{},
		cfg:   cfg,
		flows: make(map[int64]FlowWorker),
		subs:  &cascadeFakeSubscriptions{pub: pub},
		provs: &cascadeFakeProviders{resolvable: map[provider.ProviderName]bool{"openai": true}},
	}
	fc.newWorker = func(ctx context.Context, fwc newFlowWorkerCtx) (FlowWorker, error) {
		flow, err := q.CreateFlow(ctx, database.CreateFlowParams{
			Title:              "untitled",
			Status:             database.FlowStatusRunning,
			Model:              "gpt-4",
			ModelProviderName:  "openai",
			ModelProviderType:  database.ProviderTypeOpenai,
			Language:           "English",
			ToolCallIDTemplate: "template",
			Functions:          []byte("{}"),
			UserID:             fwc.userID,
		})
		if err != nil {
			return nil, err
		}
		fw := newFakeFlowWorkerStub(flow.ID, fwc.userID, database.FlowStatusRunning)
		return fw, nil
	}
	fc.loadWorker = func(ctx context.Context, flow database.Flow, fwc flowWorkerCtx) (FlowWorker, error) {
		return newFakeFlowWorkerStub(flow.ID, flow.UserID, flow.Status), nil
	}
	return fc, pub
}

// -----------------------------------------------------------------------------
// Test A: Unlimited Mode (MAX_CONCURRENT_FLOWS = 0)
// -----------------------------------------------------------------------------
func TestFlowConcurrency_UnlimitedMode(t *testing.T) {
	ctx := context.Background()
	q := newFakeFlowQuerier()
	fc, _ := newTestConcurrencyFlowController(q, 0) // unlimited

	const numFlows = 5
	var created []FlowWorker
	for i := 0; i < numFlows; i++ {
		fw, err := fc.CreateFlow(ctx, 1, "test input", "openai", provider.ProviderOpenai, nil, nil)
		require.NoError(t, err)
		created = append(created, fw)
	}

	require.Len(t, created, numFlows)
	for i, fw := range created {
		status, err := fw.GetStatus(ctx)
		require.NoError(t, err)
		assert.Equal(t, database.FlowStatusRunning, status, "flow %d must be running immediately in unlimited mode", i+1)
	}
}

// -----------------------------------------------------------------------------
// Test B: Limit = 1 (Sequential Execution and Promotion)
// -----------------------------------------------------------------------------
func TestFlowConcurrency_LimitOne_SequentialPromotion(t *testing.T) {
	ctx := context.Background()
	q := newFakeFlowQuerier()
	fc, _ := newTestConcurrencyFlowController(q, 1)

	// Create Flow 1 -> running
	fw1, err := fc.CreateFlow(ctx, 1, "flow 1", "openai", provider.ProviderOpenai, nil, nil)
	require.NoError(t, err)
	st1, err := fw1.GetStatus(ctx)
	require.NoError(t, err)
	assert.Equal(t, database.FlowStatusRunning, st1, "Flow 1 should start running")

	// Create Flow 2 -> queued
	fw2, err := fc.CreateFlow(ctx, 1, "flow 2", "openai", provider.ProviderOpenai, nil, nil)
	require.NoError(t, err)
	st2, err := fw2.GetStatus(ctx)
	require.NoError(t, err)
	assert.Equal(t, database.FlowStatusQueued, st2, "Flow 2 should enter queued state")

	// Create Flow 3 -> queued
	fw3, err := fc.CreateFlow(ctx, 1, "flow 3", "openai", provider.ProviderOpenai, nil, nil)
	require.NoError(t, err)
	st3, err := fw3.GetStatus(ctx)
	require.NoError(t, err)
	assert.Equal(t, database.FlowStatusQueued, st3, "Flow 3 should enter queued state")

	// Finish Flow 1 -> Flow 2 must be promoted to running, Flow 3 must remain queued
	err = fc.FinishFlow(ctx, fw1.GetFlowID())
	require.NoError(t, err)

	st2After, err := fw2.GetStatus(ctx)
	require.NoError(t, err)
	assert.Equal(t, database.FlowStatusRunning, st2After, "Flow 2 should be promoted to running after Flow 1 finishes")

	st3After, err := fw3.GetStatus(ctx)
	require.NoError(t, err)
	assert.Equal(t, database.FlowStatusQueued, st3After, "Flow 3 should remain queued while Flow 2 runs")

	// Stop Flow 2 -> Flow 3 must be promoted to running
	err = fc.StopFlow(ctx, fw2.GetFlowID())
	require.NoError(t, err)

	st3Final, err := fw3.GetStatus(ctx)
	require.NoError(t, err)
	assert.Equal(t, database.FlowStatusRunning, st3Final, "Flow 3 should be promoted to running after Flow 2 stops")
}

// -----------------------------------------------------------------------------
// Test C: Limit = 2 (Enforces Concurrent Active Limit)
// -----------------------------------------------------------------------------
func TestFlowConcurrency_LimitTwo_Capacity(t *testing.T) {
	ctx := context.Background()
	q := newFakeFlowQuerier()
	fc, _ := newTestConcurrencyFlowController(q, 2)

	fw1, err := fc.CreateFlow(ctx, 1, "flow 1", "openai", provider.ProviderOpenai, nil, nil)
	require.NoError(t, err)
	fw2, err := fc.CreateFlow(ctx, 1, "flow 2", "openai", provider.ProviderOpenai, nil, nil)
	require.NoError(t, err)
	fw3, err := fc.CreateFlow(ctx, 1, "flow 3", "openai", provider.ProviderOpenai, nil, nil)
	require.NoError(t, err)
	fw4, err := fc.CreateFlow(ctx, 1, "flow 4", "openai", provider.ProviderOpenai, nil, nil)
	require.NoError(t, err)

	st1, _ := fw1.GetStatus(ctx)
	st2, _ := fw2.GetStatus(ctx)
	st3, _ := fw3.GetStatus(ctx)
	st4, _ := fw4.GetStatus(ctx)

	assert.Equal(t, database.FlowStatusRunning, st1)
	assert.Equal(t, database.FlowStatusRunning, st2)
	assert.Equal(t, database.FlowStatusQueued, st3)
	assert.Equal(t, database.FlowStatusQueued, st4)

	// Finish Flow 1 -> Flow 3 runs, Flow 4 remains queued (2 active: Flow 2 & Flow 3)
	err = fc.FinishFlow(ctx, fw1.GetFlowID())
	require.NoError(t, err)

	st3After, _ := fw3.GetStatus(ctx)
	st4After, _ := fw4.GetStatus(ctx)
	assert.Equal(t, database.FlowStatusRunning, st3After, "Flow 3 must be promoted")
	assert.Equal(t, database.FlowStatusQueued, st4After, "Flow 4 must remain queued")

	// Finish Flow 2 -> Flow 4 runs (2 active: Flow 3 & Flow 4)
	err = fc.FinishFlow(ctx, fw2.GetFlowID())
	require.NoError(t, err)

	st4Final, _ := fw4.GetStatus(ctx)
	assert.Equal(t, database.FlowStatusRunning, st4Final, "Flow 4 must be promoted")
}

// -----------------------------------------------------------------------------
// Test D: FIFO Promotion Order
// -----------------------------------------------------------------------------
func TestFlowConcurrency_FIFOPromotionOrder(t *testing.T) {
	ctx := context.Background()
	q := newFakeFlowQuerier()
	fc, _ := newTestConcurrencyFlowController(q, 1)

	// Create initial active flow
	activeFlow, err := fc.CreateFlow(ctx, 1, "active", "openai", provider.ProviderOpenai, nil, nil)
	require.NoError(t, err)

	// Create 4 queued flows in order A, B, C, D
	fwA, err := fc.CreateFlow(ctx, 1, "A", "openai", provider.ProviderOpenai, nil, nil)
	require.NoError(t, err)
	fwB, err := fc.CreateFlow(ctx, 1, "B", "openai", provider.ProviderOpenai, nil, nil)
	require.NoError(t, err)
	fwC, err := fc.CreateFlow(ctx, 1, "C", "openai", provider.ProviderOpenai, nil, nil)
	require.NoError(t, err)
	fwD, err := fc.CreateFlow(ctx, 1, "D", "openai", provider.ProviderOpenai, nil, nil)
	require.NoError(t, err)

	// Finish active -> A should be promoted
	require.NoError(t, fc.FinishFlow(ctx, activeFlow.GetFlowID()))
	stA, _ := fwA.GetStatus(ctx)
	stB, _ := fwB.GetStatus(ctx)
	assert.Equal(t, database.FlowStatusRunning, stA, "A should be promoted first")
	assert.Equal(t, database.FlowStatusQueued, stB, "B must wait")

	// Finish A -> B should be promoted
	require.NoError(t, fc.FinishFlow(ctx, fwA.GetFlowID()))
	stB2, _ := fwB.GetStatus(ctx)
	stC, _ := fwC.GetStatus(ctx)
	assert.Equal(t, database.FlowStatusRunning, stB2, "B should be promoted second")
	assert.Equal(t, database.FlowStatusQueued, stC, "C must wait")

	// Finish B -> C should be promoted
	require.NoError(t, fc.FinishFlow(ctx, fwB.GetFlowID()))
	stC2, _ := fwC.GetStatus(ctx)
	stD, _ := fwD.GetStatus(ctx)
	assert.Equal(t, database.FlowStatusRunning, stC2, "C should be promoted third")
	assert.Equal(t, database.FlowStatusQueued, stD, "D must wait")

	// Finish C -> D should be promoted
	require.NoError(t, fc.FinishFlow(ctx, fwC.GetFlowID()))
	stD2, _ := fwD.GetStatus(ctx)
	assert.Equal(t, database.FlowStatusRunning, stD2, "D should be promoted fourth")
}

// -----------------------------------------------------------------------------
// Test E: Terminal States Release Capacity (Finished, Failed, Stopped)
// -----------------------------------------------------------------------------
func TestFlowConcurrency_TerminalStatesReleaseCapacity(t *testing.T) {
	ctx := context.Background()
	q := newFakeFlowQuerier()
	fc, _ := newTestConcurrencyFlowController(q, 1)

	// 1. Finished release
	f1, _ := fc.CreateFlow(ctx, 1, "f1", "openai", provider.ProviderOpenai, nil, nil)
	q1, _ := fc.CreateFlow(ctx, 1, "q1", "openai", provider.ProviderOpenai, nil, nil)
	require.NoError(t, fc.FinishFlow(ctx, f1.GetFlowID()))
	stQ1, _ := q1.GetStatus(ctx)
	assert.Equal(t, database.FlowStatusRunning, stQ1)

	// 2. Stopped release
	q2, _ := fc.CreateFlow(ctx, 1, "q2", "openai", provider.ProviderOpenai, nil, nil)
	require.NoError(t, fc.StopFlow(ctx, q1.GetFlowID()))
	stQ2, _ := q2.GetStatus(ctx)
	assert.Equal(t, database.FlowStatusRunning, stQ2)
}

// -----------------------------------------------------------------------------
// Test F: Concurrent Terminal Transitions (Race Condition Safety)
// -----------------------------------------------------------------------------
func TestFlowConcurrency_ConcurrentTerminalTransitions(t *testing.T) {
	ctx := context.Background()
	q := newFakeFlowQuerier()
	const maxConcurrency = 3
	fc, _ := newTestConcurrencyFlowController(q, maxConcurrency)

	const totalFlows = 15
	flows := make([]FlowWorker, totalFlows)
	for i := 0; i < totalFlows; i++ {
		fw, err := fc.CreateFlow(ctx, 1, fmt.Sprintf("concurrent-flow-%d", i), "openai", provider.ProviderOpenai, nil, nil)
		require.NoError(t, err)
		flows[i] = fw
	}

	var activeCounter int64
	var maxObservedActive int64

	// Concurrently finish active flows
	var wg sync.WaitGroup
	for _, fw := range flows {
		wg.Add(1)
		go func(f FlowWorker) {
			defer wg.Done()
			// Check status
			st, err := f.GetStatus(ctx)
			if err == nil && st == database.FlowStatusRunning {
				current := atomic.AddInt64(&activeCounter, 1)
				for {
					max := atomic.LoadInt64(&maxObservedActive)
					if current <= max || atomic.CompareAndSwapInt64(&maxObservedActive, max, current) {
						break
					}
				}
				time.Sleep(10 * time.Millisecond)
				_ = fc.FinishFlow(ctx, f.GetFlowID())
				atomic.AddInt64(&activeCounter, -1)
			}
		}(fw)
	}

	wg.Wait()

	// Finish any remaining flows
	for _, fw := range flows {
		st, _ := fw.GetStatus(ctx)
		if st == database.FlowStatusRunning {
			_ = fc.FinishFlow(ctx, fw.GetFlowID())
		}
	}

	assert.LessOrEqual(t, maxObservedActive, int64(maxConcurrency), "Active flows must never exceed MAX_CONCURRENT_FLOWS")
}

// -----------------------------------------------------------------------------
// Test G: Persistence of Queued Status
// -----------------------------------------------------------------------------
func TestFlowConcurrency_PersistentQueuedStatus(t *testing.T) {
	ctx := context.Background()
	q := newFakeFlowQuerier()
	fc, _ := newTestConcurrencyFlowController(q, 1)

	// Create running flow and queued flow
	f1, err := fc.CreateFlow(ctx, 1, "f1", "openai", provider.ProviderOpenai, nil, nil)
	require.NoError(t, err)
	f2, err := fc.CreateFlow(ctx, 1, "f2", "openai", provider.ProviderOpenai, nil, nil)
	require.NoError(t, err)

	// Verify DB status
	dbFlow1, err := q.GetUserFlow(ctx, database.GetUserFlowParams{ID: f1.GetFlowID(), UserID: 1})
	require.NoError(t, err)
	assert.Equal(t, database.FlowStatusRunning, dbFlow1.Status)

	dbFlow2, err := q.GetUserFlow(ctx, database.GetUserFlowParams{ID: f2.GetFlowID(), UserID: 1})
	require.NoError(t, err)
	assert.Equal(t, database.FlowStatusQueued, dbFlow2.Status, "Queued flow must be persisted with status 'queued' in database")
}
