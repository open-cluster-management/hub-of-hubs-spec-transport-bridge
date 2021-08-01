package specsyncer

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	datatypes "github.com/open-cluster-management/hub-of-hubs-data-types"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/helpers"
	"github.com/open-cluster-management/hub-of-hubs-spec-transport-bridge/pkg/transport"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

type object interface {
	metav1.Object
	runtime.Object
}

type hubOfHubsObject struct {
	Object  object `json:"object"`
	Deleted bool   `json:"deleted"`
}

type genericSpecToTransportSyncer struct {
	client                        client.Client
	log                           logr.Logger
	transport                     transport.Transport
	syncInterval                  time.Duration
	finalizerName                 string
	createObjFunc                 func() object
	manipulateObjFunc             func(object) object
	HubOfHubsObject               hubOfHubsObject
	lastSentObjectResourceVersion string
	startOnce                     sync.Once
}

const (
	requeuePeriodSeconds = 5
)

func (r *genericSpecToTransportSyncer) init() {
	r.startOnce.Do(func() {
		go r.periodicSync()
	})
}

func (r *genericSpecToTransportSyncer) Reconcile(request ctrl.Request) (ctrl.Result, error) {
	reqLogger := r.log.WithValues("Request.Namespace", request.Namespace, "Request.Name", request.Name)
	ctx := context.Background()

	object := r.createObjFunc()

	err := r.client.Get(ctx, request.NamespacedName, object)
	if apierrors.IsNotFound(err) {
		// the instance was deleted and it had no finalizer on it.
		// this means either HoH removed the finalizer so it was already deleted, or
		// HoH didn't update about this object ever.
		// either way, no need to do anything in this state.
		return ctrl.Result{}, nil
	}

	if err != nil {
		reqLogger.Info(fmt.Sprintf("Reconciliation failed: %s", err))
		return ctrl.Result{Requeue: true, RequeueAfter: requeuePeriodSeconds * time.Second}, err
	}

	if r.isObjectBeingDeleted(object) {
		if err = r.deleteObjectAndFinalizer(ctx, object, reqLogger); err != nil {
			reqLogger.Info(fmt.Sprintf("Reconciliation failed: %s", err))
			return ctrl.Result{Requeue: true, RequeueAfter: requeuePeriodSeconds * time.Second}, err
		}
	} else { // otherwise, the object was not deleted and no error occurred
		if err = r.updateObjectAndFinalizer(ctx, object, reqLogger); err != nil {
			reqLogger.Info(fmt.Sprintf("Reconciliation failed: %s", err))
			return ctrl.Result{Requeue: true, RequeueAfter: requeuePeriodSeconds * time.Second}, err
		}
	}

	reqLogger.Info("Reconciliation complete.")
	return ctrl.Result{}, err
}

func (r *genericSpecToTransportSyncer) isObjectBeingDeleted(object object) bool {
	return !object.GetDeletionTimestamp().IsZero()
}

func (r *genericSpecToTransportSyncer) updateObjectAndFinalizer(ctx context.Context, object object,
	log logr.Logger) error {
	if err := r.addFinalizer(ctx, object, log); err != nil {
		return err
	}

	// put object for sync in next periodic sync cycle
	helpers.SetMetaDataAnnotation(object, datatypes.OriginOwnerReferenceAnnotation, string(object.GetUID()))

	r.HubOfHubsObject.Object = object
	r.HubOfHubsObject.Deleted = false

	return nil
}

func (r *genericSpecToTransportSyncer) addFinalizer(ctx context.Context, object object,
	log logr.Logger) error {
	if helpers.ContainsString(object.GetFinalizers(), r.finalizerName) {
		return nil
	}

	log.Info("adding finalizer")
	controllerutil.AddFinalizer(object, r.finalizerName)

	if err := r.client.Update(ctx, object); err != nil {
		return fmt.Errorf("failed to add finalizer %s, requeue in order to retry", r.finalizerName)
	}

	return nil
}

func (r *genericSpecToTransportSyncer) deleteObjectAndFinalizer(ctx context.Context, object object,
	log logr.Logger) error {
	// put object for sync in next periodic sync cycle
	r.HubOfHubsObject.Object = object
	r.HubOfHubsObject.Deleted = true

	return r.removeFinalizer(ctx, object, log)
}

func (r *genericSpecToTransportSyncer) removeFinalizer(ctx context.Context, object object,
	log logr.Logger) error {
	if !helpers.ContainsString(object.GetFinalizers(), r.finalizerName) {
		return nil // if finalizer is not there, do nothing
	}

	log.Info("removing finalizer")
	controllerutil.RemoveFinalizer(object, r.finalizerName)

	if err := r.client.Update(ctx, object); err != nil {
		return fmt.Errorf("failed to remove finalizer %s, requeue in order to retry", r.finalizerName)
	}

	return nil
}

func (r *genericSpecToTransportSyncer) periodicSync() {
	ticker := time.NewTicker(r.syncInterval)

	for {
		// wait until ticker time expires
		<-ticker.C
		if r.HubOfHubsObject.Object == nil {
			continue
		}

		resourceVersion := r.HubOfHubsObject.Object.GetResourceVersion()

		if resourceVersion > r.lastSentObjectResourceVersion { // send to transport only if object has changed
			cleanObject(r.HubOfHubsObject.Object)
			r.syncToTransport(datatypes.Config, datatypes.Config, resourceVersion, r.HubOfHubsObject)

			r.lastSentObjectResourceVersion = resourceVersion
		}
	}
}

func (r *genericSpecToTransportSyncer) syncToTransport(id string, objType string, version string,
	payload hubOfHubsObject) {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		r.log.Info(fmt.Sprintf("failed to sync object from type %s with id %s- %s", objType, id, err))
		return
	}

	r.transport.SendAsync(id, objType, version, payloadBytes)
}

func cleanObject(object object) {
	object.SetUID("")
	object.SetResourceVersion("")
	object.SetManagedFields(nil)
	object.SetFinalizers(nil)
	object.SetGeneration(0)
	object.SetOwnerReferences(nil)
	object.SetSelfLink("")
	object.SetClusterName("")

	delete(object.GetAnnotations(), "kubectl.kubernetes.io/last-applied-configuration")
}
