package controller

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"pentagi/pkg/cast"
	"pentagi/pkg/config"
	"pentagi/pkg/database"
	"pentagi/pkg/docker"
	"pentagi/pkg/graph/subscriptions"
	"pentagi/pkg/providers"
	"pentagi/pkg/providers/provider"
	"pentagi/pkg/tools"

	"github.com/sirupsen/logrus"
)

var (
	ErrFlowNotFound       = fmt.Errorf("flow not found")
	ErrFlowAlreadyStopped = fmt.Errorf("flow already stopped")
)

type FlowController interface {
	CreateFlow(
		ctx context.Context,
		userID int64,
		input string,
		prvname provider.ProviderName,
		prvtype provider.ProviderType,
		functions *tools.Functions,
		resources []database.UserResource,
	) (FlowWorker, error)
	CreateAssistant(
		ctx context.Context,
		userID int64,
		flowID int64,
		input string,
		useAgents bool,
		prvname provider.ProviderName,
		prvtype provider.ProviderType,
		functions *tools.Functions,
		resources []database.UserResource,
	) (AssistantWorker, error)
	LoadFlows(ctx context.Context) error
	ListFlows(ctx context.Context) []FlowWorker
	GetFlow(ctx context.Context, flowID int64) (FlowWorker, error)
	StopFlow(ctx context.Context, flowID int64) error
	FinishFlow(ctx context.Context, flowID int64) error
	RenameFlow(ctx context.Context, flowID int64, title string) error
	RenameFlowsProvider(ctx context.Context, userID int64, oldName, newName provider.ProviderName) error
	ResetFlowsProviderToDefault(
		ctx context.Context,
		userID int64,
		oldName provider.ProviderName,
		prvtype provider.ProviderType,
	) error
}

// reassignProviderTimeout bounds the provider reference sweep. It is generous
// for two indexed UPDATEs and only exists so a stuck database cannot pin the
// goroutine forever once the sweep is detached from the request context.
const reassignProviderTimeout = 30 * time.Second

type queuedFlow struct {
	flowID    int64
	userID    int64
	input     string
	prvname   provider.ProviderName
	prvtype   provider.ProviderType
	functions *tools.Functions
	resources []database.UserResource
	createdAt time.Time
}

type queuedFlowWorker struct {
	flowID   int64
	userID   int64
	title    string
	status   database.FlowStatus
	mx       sync.Mutex
	db       database.Querier
	subs     subscriptions.SubscriptionsController
	onCancel func(flowID int64)
}

func (w *queuedFlowWorker) GetFlowID() int64          { return w.flowID }
func (w *queuedFlowWorker) GetUserID() int64          { return w.userID }
func (w *queuedFlowWorker) GetTitle() string           { return w.title }
func (w *queuedFlowWorker) GetContext() *FlowContext   { return nil }

func (w *queuedFlowWorker) GetStatus(ctx context.Context) (database.FlowStatus, error) {
	w.mx.Lock()
	defer w.mx.Unlock()
	return w.status, nil
}

func (w *queuedFlowWorker) SetStatus(ctx context.Context, status database.FlowStatus) error {
	w.mx.Lock()
	defer w.mx.Unlock()
	w.status = status
	if w.db != nil {
		flow, err := w.db.UpdateFlowStatus(ctx, database.UpdateFlowStatusParams{
			ID:     w.flowID,
			Status: status,
		})
		if err != nil {
			return err
		}
		if w.subs != nil {
			pub := w.subs.NewFlowPublisher(w.userID, w.flowID)
			pub.FlowUpdated(ctx, flow, nil)
		}
	}
	return nil
}

func (w *queuedFlowWorker) AddAssistant(ctx context.Context, aw AssistantWorker) error {
	return fmt.Errorf("cannot add assistant to queued flow %d", w.flowID)
}

func (w *queuedFlowWorker) GetAssistant(ctx context.Context, assistantID int64) (AssistantWorker, error) {
	return nil, fmt.Errorf("assistant %d not found", assistantID)
}

func (w *queuedFlowWorker) DeleteAssistant(ctx context.Context, assistantID int64) error {
	return nil
}

func (w *queuedFlowWorker) ListAssistants(ctx context.Context) []AssistantWorker {
	return nil
}

func (w *queuedFlowWorker) ListTasks(ctx context.Context) []TaskWorker {
	return nil
}

func (w *queuedFlowWorker) PutInput(
	ctx context.Context,
	input string,
	prv provider.Provider,
	resources []database.UserResource,
) error {
	return fmt.Errorf("flow %d is queued, waiting for a free execution slot", w.flowID)
}

func (w *queuedFlowWorker) PutResources(ctx context.Context, resources []database.UserResource) error {
	return nil
}

func (w *queuedFlowWorker) Finish(ctx context.Context) error {
	_ = w.SetStatus(ctx, database.FlowStatusFinished)
	if w.onCancel != nil {
		w.onCancel(w.flowID)
	}
	return nil
}

func (w *queuedFlowWorker) Stop(ctx context.Context) error {
	_ = w.SetStatus(ctx, database.FlowStatusFailed)
	if w.onCancel != nil {
		w.onCancel(w.flowID)
	}
	return nil
}

func (w *queuedFlowWorker) Rename(ctx context.Context, title string) error {
	w.mx.Lock()
	w.title = title
	w.mx.Unlock()
	if w.db != nil {
		flow, err := w.db.UpdateFlowTitle(ctx, database.UpdateFlowTitleParams{
			ID:    w.flowID,
			Title: title,
		})
		if err != nil {
			return err
		}
		if w.subs != nil {
			pub := w.subs.NewFlowPublisher(w.userID, w.flowID)
			pub.FlowUpdated(ctx, flow, nil)
		}
	}
	return nil
}

func (w *queuedFlowWorker) WaitTaskCompletion(ctx context.Context) error {
	return nil
}

func (w *queuedFlowWorker) InvalidateTaskSubtasks(ctx context.Context, taskID int64, subtaskIDs []int64) {
}

type flowController struct {
	db         database.Querier
	mx         *sync.Mutex
	cfg        *config.Config
	flows      map[int64]FlowWorker
	queue      []*queuedFlow
	newWorker  func(ctx context.Context, fwc newFlowWorkerCtx) (FlowWorker, error)
	loadWorker func(ctx context.Context, flow database.Flow, fwc flowWorkerCtx) (FlowWorker, error)
	docker     docker.DockerClient
	provs      providers.ProviderController
	subs       subscriptions.SubscriptionsController
	alc        AgentLogController
	mlc        MsgLogController
	aslc       AssistantLogController
	slc        SearchLogController
	tlc        TermLogController
	vslc       VectorStoreLogController
	tclc       ToolCallLogController
	sc         ScreenshotController
}

func NewFlowController(
	db database.Querier,
	cfg *config.Config,
	docker docker.DockerClient,
	provs providers.ProviderController,
	subs subscriptions.SubscriptionsController,
) FlowController {
	return &flowController{
		db:     db,
		mx:     &sync.Mutex{},
		cfg:    cfg,
		flows:  make(map[int64]FlowWorker),
		docker: docker,
		provs:  provs,
		subs:   subs,
		alc:    NewAgentLogController(db),
		mlc:    NewMsgLogController(db),
		aslc:   NewAssistantLogController(db),
		slc:    NewSearchLogController(db),
		tlc:    NewTermLogController(db),
		vslc:   NewVectorStoreLogController(db),
		tclc:   NewToolCallLogController(db),
		sc:     NewScreenshotController(db),
	}
}

func (fc *flowController) getNewWorker() func(ctx context.Context, fwc newFlowWorkerCtx) (FlowWorker, error) {
	if fc.newWorker != nil {
		return fc.newWorker
	}
	return NewFlowWorker
}

func (fc *flowController) getLoadWorker() func(ctx context.Context, flow database.Flow, fwc flowWorkerCtx) (FlowWorker, error) {
	if fc.loadWorker != nil {
		return fc.loadWorker
	}
	return LoadFlowWorker
}

func (fc *flowController) countActiveFlowsLocked(ctx context.Context) int {
	active := 0
	for _, fw := range fc.flows {
		st, err := fw.GetStatus(ctx)
		if err != nil {
			continue
		}
		if st == database.FlowStatusRunning || st == database.FlowStatusWaiting {
			active++
		}
	}
	return active
}

func (fc *flowController) promoteNextQueuedFlowsLocked(ctx context.Context) {
	if fc.cfg == nil || fc.cfg.MaxConcurrentFlows <= 0 {
		for len(fc.queue) > 0 {
			qf := fc.queue[0]
			fc.queue = fc.queue[1:]
			fc.promoteQueuedFlowLocked(ctx, qf)
		}
		return
	}

	for fc.countActiveFlowsLocked(ctx) < fc.cfg.MaxConcurrentFlows && len(fc.queue) > 0 {
		qf := fc.queue[0]
		fc.queue = fc.queue[1:]
		fc.promoteQueuedFlowLocked(ctx, qf)
	}
}

func (fc *flowController) promoteQueuedFlowLocked(ctx context.Context, qf *queuedFlow) {
	existing, ok := fc.flows[qf.flowID]
	if ok {
		st, err := existing.GetStatus(ctx)
		if err == nil && (st == database.FlowStatusFinished || st == database.FlowStatusFailed) {
			return
		}
	}

	fwWorkerCtx := flowWorkerCtx{
		db:     fc.db,
		cfg:    fc.cfg,
		docker: fc.docker,
		provs:  fc.provs,
		subs:   fc.subs,
		flowProviderControllers: flowProviderControllers{
			mlc:  fc.mlc,
			aslc: fc.aslc,
			alc:  fc.alc,
			slc:  fc.slc,
			tlc:  fc.tlc,
			vslc: fc.vslc,
			tclc: fc.tclc,
			sc:   fc.sc,
		},
	}

	_, _ = fc.db.UpdateFlowStatus(ctx, database.UpdateFlowStatusParams{
		ID:     qf.flowID,
		Status: database.FlowStatusRunning,
	})

	fw, err := fc.getNewWorker()(ctx, newFlowWorkerCtx{
		userID:        qf.userID,
		input:         qf.input,
		prvname:       qf.prvname,
		prvtype:       qf.prvtype,
		functions:     qf.functions,
		resources:     qf.resources,
		flowWorkerCtx: fwWorkerCtx,
	})
	if err != nil {
		logrus.WithContext(ctx).WithError(err).Errorf("failed to promote queued flow %d", qf.flowID)
		_ = fc.db.UpdateFlowStatus(ctx, database.UpdateFlowStatusParams{
			ID:     qf.flowID,
			Status: database.FlowStatusFailed,
		})
		return
	}

	fc.flows[qf.flowID] = fw
}

func (fc *flowController) LoadFlows(ctx context.Context) error {
	fc.mx.Lock()
	defer fc.mx.Unlock()

	flows, err := fc.db.GetFlows(ctx)
	if err != nil {
		return fmt.Errorf("failed to load flows: %w", err)
	}

	for _, flow := range flows {
		if flow.Status == database.FlowStatusQueued {
			qf := &queuedFlow{
				flowID:    flow.ID,
				userID:    flow.UserID,
				input:     "",
				prvname:   provider.ProviderName(flow.ModelProviderName),
				prvtype:   provider.ProviderType(flow.ModelProviderType),
				createdAt: flow.CreatedAt,
			}
			fc.queue = append(fc.queue, qf)
			qw := &queuedFlowWorker{
				flowID: flow.ID,
				userID: flow.UserID,
				title:  flow.Title,
				status: database.FlowStatusQueued,
				db:     fc.db,
				subs:   fc.subs,
			}
			fc.flows[flow.ID] = qw
			continue
		}

		fw, err := fc.getLoadWorker()(ctx, flow, flowWorkerCtx{
			db:     fc.db,
			cfg:    fc.cfg,
			docker: fc.docker,
			provs:  fc.provs,
			subs:   fc.subs,
			flowProviderControllers: flowProviderControllers{
				mlc:  fc.mlc,
				aslc: fc.aslc,
				alc:  fc.alc,
				slc:  fc.slc,
				tlc:  fc.tlc,
				vslc: fc.vslc,
				tclc: fc.tclc,
				sc:   fc.sc,
			},
		})
		if err != nil {
			if errors.Is(err, ErrNothingToLoad) {
				continue
			}

			logrus.WithContext(ctx).WithError(err).Errorf("failed to load flow %d", flow.ID)
			continue
		}

		fc.flows[flow.ID] = fw
	}

	fc.promoteNextQueuedFlowsLocked(ctx)

	return nil
}

func (fc *flowController) CreateFlow(
	ctx context.Context,
	userID int64,
	input string,
	prvname provider.ProviderName,
	prvtype provider.ProviderType,
	functions *tools.Functions,
	resources []database.UserResource,
) (FlowWorker, error) {
	fc.mx.Lock()
	defer fc.mx.Unlock()

	activeCount := fc.countActiveFlowsLocked(ctx)
	if fc.cfg != nil && fc.cfg.MaxConcurrentFlows > 0 && activeCount >= fc.cfg.MaxConcurrentFlows {
		flow, err := fc.db.CreateFlow(ctx, database.CreateFlowParams{
			Title:              "untitled",
			Status:             database.FlowStatusQueued,
			Model:              "unknown",
			ModelProviderName:  prvname.String(),
			ModelProviderType:  database.ProviderType(prvtype),
			Language:           "English",
			ToolCallIDTemplate: cast.ToolCallIDTemplate,
			Functions:          []byte("{}"),
			UserID:             userID,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create queued flow in DB: %w", err)
		}

		qf := &queuedFlow{
			flowID:    flow.ID,
			userID:    userID,
			input:     input,
			prvname:   prvname,
			prvtype:   prvtype,
			functions: functions,
			resources: resources,
			createdAt: time.Now(),
		}
		fc.queue = append(fc.queue, qf)

		qw := &queuedFlowWorker{
			flowID: flow.ID,
			userID: userID,
			title:  "untitled",
			status: database.FlowStatusQueued,
			db:     fc.db,
			subs:   fc.subs,
			onCancel: func(fID int64) {
				fc.mx.Lock()
				defer fc.mx.Unlock()
				for i, item := range fc.queue {
					if item.flowID == fID {
						fc.queue = append(fc.queue[:i], fc.queue[i+1:]...)
						break
					}
				}
				fc.promoteNextQueuedFlowsLocked(context.Background())
			},
		}
		fc.flows[flow.ID] = qw

		if fc.subs != nil {
			pub := fc.subs.NewFlowPublisher(userID, flow.ID)
			pub.FlowCreated(ctx, flow, nil)
		}

		return qw, nil
	}

	fwWorkerCtx := flowWorkerCtx{
		db:     fc.db,
		cfg:    fc.cfg,
		docker: fc.docker,
		provs:  fc.provs,
		subs:   fc.subs,
		flowProviderControllers: flowProviderControllers{
			mlc:  fc.mlc,
			aslc: fc.aslc,
			alc:  fc.alc,
			slc:  fc.slc,
			tlc:  fc.tlc,
			vslc: fc.vslc,
			tclc: fc.tclc,
			sc:   fc.sc,
		},
	}

	fw, err := fc.getNewWorker()(ctx, newFlowWorkerCtx{
		userID:        userID,
		input:         input,
		prvname:       prvname,
		prvtype:       prvtype,
		functions:     functions,
		resources:     resources,
		flowWorkerCtx: fwWorkerCtx,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create flow worker: %w", err)
	}

	fc.flows[fw.GetFlowID()] = fw

	return fw, nil
}

func (fc *flowController) CreateAssistant(
	ctx context.Context,
	userID int64,
	flowID int64,
	input string,
	useAgents bool,
	prvname provider.ProviderName,
	prvtype provider.ProviderType,
	functions *tools.Functions,
	resources []database.UserResource,
) (AssistantWorker, error) {
	fc.mx.Lock()
	defer fc.mx.Unlock()

	var (
		fw  FlowWorker
		ok  bool
		err error
	)

	flowWorkerCtx := flowWorkerCtx{
		db:     fc.db,
		cfg:    fc.cfg,
		docker: fc.docker,
		provs:  fc.provs,
		subs:   fc.subs,
		flowProviderControllers: flowProviderControllers{
			mlc:  fc.mlc,
			aslc: fc.aslc,
			alc:  fc.alc,
			slc:  fc.slc,
			tlc:  fc.tlc,
			vslc: fc.vslc,
			tclc: fc.tclc,
			sc:   fc.sc,
		},
	}

	newFlow := func() error {
		fw, err = NewFlowWorker(ctx, newFlowWorkerCtx{
			userID:        userID,
			input:         input,
			dryRun:        true,
			prvname:       prvname,
			prvtype:       prvtype,
			functions:     functions,
			flowWorkerCtx: flowWorkerCtx,
		})
		if err != nil {
			return fmt.Errorf("failed to create flow worker: %w", err)
		}

		fc.flows[fw.GetFlowID()] = fw
		flowID = fw.GetFlowID()
		fw.SetStatus(ctx, database.FlowStatusWaiting)

		return nil
	}

	loadFlow := func() error {
		flow, err := fc.db.UpdateFlowStatus(ctx, database.UpdateFlowStatusParams{
			ID:     flowID,
			Status: database.FlowStatusWaiting,
		})
		if err != nil {
			return fmt.Errorf("failed to renew flow %d status: %w", flowID, err)
		}

		fw, err = LoadFlowWorker(ctx, flow, flowWorkerCtx)
		if err != nil {
			return fmt.Errorf("failed to load flow %d: %w", flowID, err)
		}

		fc.flows[flowID] = fw

		return nil
	}

	if flowID == 0 {
		if err := newFlow(); err != nil {
			return nil, err
		}
	} else if fw, ok = fc.flows[flowID]; ok {
		status, err := fw.GetStatus(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get flow %d status: %w", flowID, err)
		}

		switch status {
		case database.FlowStatusCreated:
			return nil, fmt.Errorf("flow %d is not completed", flowID)
		case database.FlowStatusFinished, database.FlowStatusFailed:
			if err := loadFlow(); err != nil {
				return nil, err
			}
		case database.FlowStatusRunning, database.FlowStatusWaiting:
			break
		default:
			return nil, fmt.Errorf("flow %d is in unknown status: %s", flowID, status)
		}
	} else {
		if err := loadFlow(); err != nil {
			return nil, err
		}
	}

	if fw == nil { // just double check, this should never happen
		return nil, fmt.Errorf("unexpected error: flow %d not found", flowID)
	}

	aw, err := NewAssistantWorker(ctx, newAssistantWorkerCtx{
		userID:        userID,
		flowID:        flowID,
		input:         input,
		prvname:       prvname,
		prvtype:       prvtype,
		useAgents:     useAgents,
		functions:     functions,
		resources:     resources,
		fw:            fw,
		flowWorkerCtx: flowWorkerCtx,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create assistant: %w", err)
	}

	if err = fw.AddAssistant(ctx, aw); err != nil {
		return nil, fmt.Errorf("failed to add assistant to flow: %w", err)
	}

	return aw, nil
}

func (fc *flowController) ListFlows(ctx context.Context) []FlowWorker {
	fc.mx.Lock()
	defer fc.mx.Unlock()

	flows := make([]FlowWorker, 0)
	for _, flow := range fc.flows {
		flows = append(flows, flow)
	}

	sort.Slice(flows, func(i, j int) bool {
		return flows[i].GetFlowID() < flows[j].GetFlowID()
	})

	return flows
}

func (fc *flowController) GetFlow(ctx context.Context, flowID int64) (FlowWorker, error) {
	fc.mx.Lock()
	defer fc.mx.Unlock()

	flow, ok := fc.flows[flowID]
	if !ok {
		return nil, ErrFlowNotFound
	}

	return flow, nil
}

func (fc *flowController) StopFlow(ctx context.Context, flowID int64) error {
	fc.mx.Lock()
	defer fc.mx.Unlock()

	flow, ok := fc.flows[flowID]
	if !ok {
		return ErrFlowNotFound
	}

	err := flow.Stop(ctx)
	if err != nil {
		return fmt.Errorf("failed to stop flow %d: %w", flowID, err)
	}

	for i, item := range fc.queue {
		if item.flowID == flowID {
			fc.queue = append(fc.queue[:i], fc.queue[i+1:]...)
			break
		}
	}

	fc.promoteNextQueuedFlowsLocked(ctx)

	return nil
}

func (fc *flowController) FinishFlow(ctx context.Context, flowID int64) error {
	fc.mx.Lock()
	defer fc.mx.Unlock()

	flow, ok := fc.flows[flowID]
	if !ok {
		return ErrFlowNotFound
	}

	err := flow.Finish(ctx)
	if err != nil {
		return fmt.Errorf("failed to finish flow %d: %w", flowID, err)
	}

	for i, item := range fc.queue {
		if item.flowID == flowID {
			fc.queue = append(fc.queue[:i], fc.queue[i+1:]...)
			break
		}
	}

	delete(fc.flows, flowID)

	fc.promoteNextQueuedFlowsLocked(ctx)

	return nil
}

func (fc *flowController) RenameFlow(ctx context.Context, flowID int64, title string) error {
	fc.mx.Lock()
	defer fc.mx.Unlock()

	flow, ok := fc.flows[flowID]
	if !ok {
		return ErrFlowNotFound
	}

	return flow.Rename(ctx, title)
}

// RenameFlowsProvider repoints every flow and assistant of userID that still
// refers to oldName at newName, after the user renamed a custom LLM provider.
func (fc *flowController) RenameFlowsProvider(
	ctx context.Context,
	userID int64,
	oldName, newName provider.ProviderName,
) error {
	return fc.reassignFlowsProvider(ctx, userID, oldName, newName)
}

// ResetFlowsProviderToDefault repoints every flow and assistant of userID that
// referred to a just-deleted custom LLM provider at the built-in name for its
// type, which is literally the type string ("qwen", "openai", ...) — see
// provider.DefaultProviderName*. That name always resolves, so the flow stays
// loadable instead of failing with "provider not found by name".
func (fc *flowController) ResetFlowsProviderToDefault(
	ctx context.Context,
	userID int64,
	oldName provider.ProviderName,
	prvtype provider.ProviderType,
) error {
	return fc.reassignFlowsProvider(ctx, userID, oldName, provider.ProviderName(prvtype))
}

// reassignFlowsProvider rewrites the provider reference stored on a user's flow
// and assistant rows. It deliberately does *not* touch loaded workers:
//
//   - Nothing here blocks on an LLM. Building a provider instance probes the
//     upstream API to resolve a tool call ID template, so switching loaded
//     workers inline would tie a "rename provider" click to LLM latency and give
//     the caller time to cancel the request mid-cascade.
//   - Nothing here takes fc.mx or reaches into a worker, so the cascade cannot
//     deadlock against, or stall, any other flow operation.
//
// A running flow picks the change up on the user's next input (which already
// re-resolves the provider by name and calls flowWorker.switchProvider) or on
// the next backend start (which rebuilds the provider from the DB row). Both
// paths compare the provider's raw configuration, so they also catch the case
// where the name did not change but the configuration behind it did.
//
// The two sweeps only match rows still bearing oldName, which makes the whole
// operation idempotent and safe to retry. They are issued independently and
// their errors are joined, so a failure on one table never silently skips the
// other.
func (fc *flowController) reassignFlowsProvider(
	ctx context.Context,
	userID int64,
	oldName, newName provider.ProviderName,
) error {
	logger := logrus.WithContext(ctx).WithFields(logrus.Fields{
		"user_id":  userID,
		"old_name": oldName.String(),
		"new_name": newName.String(),
	})

	if oldName == newName {
		logger.Debug("provider name unchanged, nothing to reassign")
		return nil
	}

	// Only references that would otherwise dangle get rewritten. oldName can
	// still resolve after the provider is gone when it named an override of a
	// built-in — an intentional feature — in which case the built-in answers to
	// that name again and the stored value is already correct. Rewriting it
	// anyway would repoint rows that predate the override, and (when the
	// override's type differed from the built-in it was named after) would send
	// them to the wrong default entirely.
	if _, err := fc.provs.GetProvider(ctx, oldName, userID); err == nil {
		logger.Debug("old provider name still resolves, nothing to reassign")
		return nil
	}

	// Detached from the caller's request context: these are two short statements
	// and the reference must not be left half-rewritten because a browser tab
	// was closed. The timeout keeps a stuck DB from pinning the goroutine.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), reassignProviderTimeout)
	defer cancel()

	flows, flowsErr := fc.db.UpdateFlowsProviderNameByOldName(ctx, database.UpdateFlowsProviderNameByOldNameParams{
		NewName: newName.String(),
		UserID:  userID,
		OldName: oldName.String(),
	})
	if flowsErr != nil {
		logger.WithError(flowsErr).Error("failed to bulk-update flows provider name")
		flowsErr = fmt.Errorf("failed to bulk-update flows provider name: %w", flowsErr)
	}

	assistants, asstErr := fc.db.UpdateAssistantsProviderNameByOldName(
		ctx, database.UpdateAssistantsProviderNameByOldNameParams{
			NewName: newName.String(),
			UserID:  userID,
			OldName: oldName.String(),
		})
	if asstErr != nil {
		logger.WithError(asstErr).Error("failed to bulk-update assistants provider name")
		asstErr = fmt.Errorf("failed to bulk-update assistants provider name: %w", asstErr)
	}

	// Publishing happens only after both writes are done. A subscriber that is
	// not draining its channel makes each publish cost up to the subscription
	// send timeout, so doing it in between would let a wedged websocket client
	// eat the deadline and starve the second UPDATE.
	for _, flow := range flows {
		// Skipped rather than published with no containers: FlowUpdated carries
		// the full terminal list and the client replaces its cached value with
		// whatever arrives, so an empty list would wipe the flow's terminals in
		// the UI. Same handling as flowWorker.switchProvider.
		containers, err := fc.db.GetFlowContainers(ctx, flow.ID)
		if err != nil {
			logger.WithError(err).Warnf("failed to get containers for flow %d, skipping its update event", flow.ID)
			continue
		}
		fc.subs.NewFlowPublisher(userID, flow.ID).FlowUpdated(ctx, flow, containers)
	}

	for _, assistant := range assistants {
		fc.subs.NewFlowPublisher(userID, assistant.FlowID).AssistantUpdated(ctx, assistant)
	}

	logger.WithFields(logrus.Fields{
		"flows_updated":      len(flows),
		"assistants_updated": len(assistants),
	}).Info("provider reference reassigned")

	return errors.Join(flowsErr, asstErr)
}
