/*
Copyright 2014 The Kubernetes Authors All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package userspace

import (
	"errors"
	"fmt"
	"net"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/golang/glog"
	"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/proxy"
	"k8s.io/kubernetes/pkg/types"
	"k8s.io/kubernetes/pkg/util/slice"
)

var (
	ErrMissingServiceEntry = errors.New("missing service entry")
	ErrMissingEndpoints    = errors.New("missing endpoints")
)

// affinityState 如何定义呢?
// <IP, endpoint>
type affinityState struct {
	clientIP string
	//clientProtocol  api.Protocol //not yet used
	//sessionCookie   string       //not yet used
	endpoint string
	lastUsed time.Time
}

type affinityPolicy struct {
	affinityType api.ServiceAffinity
	affinityMap  map[string]*affinityState // map client IP -> affinity info
	ttlMinutes   int
}

// LoadBalancerRR is a round-robin load balancer.
type LoadBalancerRR struct {
	lock     sync.RWMutex
	services map[proxy.ServicePortName]*balancerState
}

// Ensure this implements LoadBalancer.
// 如何确保接口实现呢?
var _ LoadBalancer = &LoadBalancerRR{}

type balancerState struct {
	endpoints []string // a list of "ip:port" style strings
	index     int      // current index into endpoints
	affinity  affinityPolicy
}

func newAffinityPolicy(affinityType api.ServiceAffinity, ttlMinutes int) *affinityPolicy {
	return &affinityPolicy{
		affinityType: affinityType,
		affinityMap:  make(map[string]*affinityState),
		ttlMinutes:   ttlMinutes,
	}
}

// NewLoadBalancerRR returns a new LoadBalancerRR.
func NewLoadBalancerRR() *LoadBalancerRR {
	return &LoadBalancerRR{
		// Service是如何管理的呢？ 和rpc_proxy类似
		services: map[proxy.ServicePortName]*balancerState{},
	}
}

// 添加Service的信息
func (lb *LoadBalancerRR) NewService(svcPort proxy.ServicePortName, affinityType api.ServiceAffinity, ttlMinutes int) error {
	lb.lock.Lock()
	defer lb.lock.Unlock()
	lb.newServiceInternal(svcPort, affinityType, ttlMinutes)
	return nil
}

// This assumes that lb.lock is already held.
func (lb *LoadBalancerRR) newServiceInternal(svcPort proxy.ServicePortName, affinityType api.ServiceAffinity, ttlMinutes int) *balancerState {
	if ttlMinutes == 0 {
		ttlMinutes = 180 //default to 3 hours if not specified.  Should 0 be unlimited instead????
	}

	if _, exists := lb.services[svcPort]; !exists {
		// 创建: Service
		lb.services[svcPort] = &balancerState{affinity: *newAffinityPolicy(affinityType, ttlMinutes)}
		glog.V(4).Infof("LoadBalancerRR service %q did not exist, created", svcPort)
	} else if affinityType != "" {
		lb.services[svcPort].affinity.affinityType = affinityType
	}
	return lb.services[svcPort]
}

// return true if this service is using some form of session affinity.
func isSessionAffinity(affinity *affinityPolicy) bool {
	// Should never be empty string, but checking for it to be safe.
	if affinity.affinityType == "" || affinity.affinityType == api.ServiceAffinityNone {
		return false
	}
	return true
}

// NextEndpoint returns a service endpoint.
// The service endpoint is chosen using the round-robin algorithm.
func (lb *LoadBalancerRR) NextEndpoint(svcPort proxy.ServicePortName, srcAddr net.Addr) (string, error) {
	// Coarse locking is simple.  We can get more fine-grained if/when we
	// can prove it matters.
	lb.lock.Lock()
	defer lb.lock.Unlock()

	// 获取state, 如果state找不到，则报错
	state, exists := lb.services[svcPort]
	if !exists || state == nil {
		return "", ErrMissingServiceEntry
	}
	if len(state.endpoints) == 0 {
		return "", ErrMissingEndpoints
	}
	glog.V(4).Infof("NextEndpoint for service %q, srcAddr=%v: endpoints: %+v", svcPort, srcAddr, state.endpoints)

	sessionAffinityEnabled := isSessionAffinity(&state.affinity)

	// 如何选择一个: Endpoint呢?
	// sessionAffinityEnabled, 则优先考虑之前使用过的Session
	var ipaddr string
	if sessionAffinityEnabled {
		// Caution: don't shadow ipaddr
		var err error
		ipaddr, _, err = net.SplitHostPort(srcAddr.String())
		if err != nil {
			return "", fmt.Errorf("malformed source address %q: %v", srcAddr.String(), err)
		}
		sessionAffinity, exists := state.affinity.affinityMap[ipaddr]
		if exists && int(time.Now().Sub(sessionAffinity.lastUsed).Minutes()) < state.affinity.ttlMinutes {
			// Affinity wins.
			endpoint := sessionAffinity.endpoint
			sessionAffinity.lastUsed = time.Now()
			glog.V(4).Infof("NextEndpoint for service %q from IP %s with sessionAffinity %+v: %s", svcPort, ipaddr, sessionAffinity, endpoint)
			return endpoint, nil
		}
	}


	// 机器和机器之间的关联: Affinity, 似乎没有太大的必要性
	// Take the next endpoint.
	endpoint := state.endpoints[state.index]
	state.index = (state.index + 1) % len(state.endpoints)

	if sessionAffinityEnabled {
		var affinity *affinityState
		affinity = state.affinity.affinityMap[ipaddr]

		// 记录下: affinity
		if affinity == nil {
			affinity = new(affinityState) //&affinityState{ipaddr, "TCP", "", endpoint, time.Now()}
			state.affinity.affinityMap[ipaddr] = affinity
		}
		affinity.lastUsed = time.Now()
		affinity.endpoint = endpoint
		affinity.clientIP = ipaddr
		glog.V(4).Infof("Updated affinity key %s: %+v", ipaddr, state.affinity.affinityMap[ipaddr])
	}

	return endpoint, nil
}

type hostPortPair struct {
	host string
	port int
}

func isValidEndpoint(hpp *hostPortPair) bool {
	return hpp.host != "" && hpp.port > 0
}

func flattenValidEndpoints(endpoints []hostPortPair) []string {
	// Convert Endpoint objects into strings for easier use later.  Ignore
	// the protocol field - we'll get that from the Service objects.
	var result []string
	for i := range endpoints {
		hpp := &endpoints[i]
		if isValidEndpoint(hpp) {
			result = append(result, net.JoinHostPort(hpp.host, strconv.Itoa(hpp.port)))
		}
	}
	return result
}

// Remove any session affinity records associated to a particular endpoint (for example when a pod goes down).
func removeSessionAffinityByEndpoint(state *balancerState, svcPort proxy.ServicePortName, endpoint string) {
	for _, affinity := range state.affinity.affinityMap {
		if affinity.endpoint == endpoint {
			glog.V(4).Infof("Removing client: %s from affinityMap for service %q", affinity.endpoint, svcPort)
			delete(state.affinity.affinityMap, affinity.clientIP)
		}
	}
}

// Loop through the valid endpoints and then the endpoints associated with the Load Balancer.
// Then remove any session affinity records that are not in both lists.
// This assumes the lb.lock is held.
func (lb *LoadBalancerRR) updateAffinityMap(svcPort proxy.ServicePortName, newEndpoints []string) {
	allEndpoints := map[string]int{}
	for _, newEndpoint := range newEndpoints {
		allEndpoints[newEndpoint] = 1
	}
	state, exists := lb.services[svcPort]
	if !exists {
		return
	}
	for _, existingEndpoint := range state.endpoints {
		allEndpoints[existingEndpoint] = allEndpoints[existingEndpoint] + 1
	}
	for mKey, mVal := range allEndpoints {
		if mVal == 1 {
			glog.V(2).Infof("Delete endpoint %s for service %q", mKey, svcPort)
			removeSessionAffinityByEndpoint(state, svcPort, mKey)
		}
	}
}

// OnEndpointsUpdate manages the registered service endpoints.
// Registered endpoints are updated if found in the update set or
// unregistered if missing from the update set.
func (lb *LoadBalancerRR) OnEndpointsUpdate(allEndpoints []api.Endpoints) {
	registeredEndpoints := make(map[proxy.ServicePortName]bool)
	lb.lock.Lock()
	defer lb.lock.Unlock()

	// Update endpoints for services.
	for i := range allEndpoints {
		svcEndpoints := &allEndpoints[i]

		// We need to build a map of portname -> all ip:ports for that
		// portname.  Explode Endpoints.Subsets[*] into this structure.
		portsToEndpoints := map[string][]hostPortPair{}
		for i := range svcEndpoints.Subsets {
			ss := &svcEndpoints.Subsets[i]
			for i := range ss.Ports {
				port := &ss.Ports[i]
				for i := range ss.Addresses {
					addr := &ss.Addresses[i]
					portsToEndpoints[port.Name] = append(portsToEndpoints[port.Name], hostPortPair{addr.IP, port.Port})
					// Ignore the protocol field - we'll get that from the Service objects.
				}
			}
		}

		for portname := range portsToEndpoints {
			svcPort := proxy.ServicePortName{NamespacedName: types.NamespacedName{Namespace: svcEndpoints.Namespace, Name: svcEndpoints.Name}, Port: portname}
			state, exists := lb.services[svcPort]
			curEndpoints := []string{}
			if state != nil {
				curEndpoints = state.endpoints
			}
			newEndpoints := flattenValidEndpoints(portsToEndpoints[portname])

			if !exists || state == nil || len(curEndpoints) != len(newEndpoints) || !slicesEquiv(slice.CopyStrings(curEndpoints), newEndpoints) {
				glog.V(1).Infof("LoadBalancerRR: Setting endpoints for %s to %+v", svcPort, newEndpoints)
				lb.updateAffinityMap(svcPort, newEndpoints)
				// OnEndpointsUpdate can be called without NewService being called externally.
				// To be safe we will call it here.  A new service will only be created
				// if one does not already exist.  The affinity will be updated
				// later, once NewService is called.
				state = lb.newServiceInternal(svcPort, api.ServiceAffinity(""), 0)
				state.endpoints = slice.ShuffleStrings(newEndpoints)

				// Reset the round-robin index.
				state.index = 0
			}
			registeredEndpoints[svcPort] = true
		}
	}

	//
	// Remove endpoints missing from the update.
	for k := range lb.services {
		if _, exists := registeredEndpoints[k]; !exists {
			glog.V(2).Infof("LoadBalancerRR: Removing endpoints for %s", k)
			delete(lb.services, k)
		}
	}
}

// Tests whether two slices are equivalent.  This sorts both slices in-place.
func slicesEquiv(lhs, rhs []string) bool {
	if len(lhs) != len(rhs) {
		return false
	}
	if reflect.DeepEqual(slice.SortStrings(lhs), slice.SortStrings(rhs)) {
		return true
	}
	return false
}

func (lb *LoadBalancerRR) CleanupStaleStickySessions(svcPort proxy.ServicePortName) {
	lb.lock.Lock()
	defer lb.lock.Unlock()

	state, exists := lb.services[svcPort]
	if !exists {
		return
	}
	for ip, affinity := range state.affinity.affinityMap {
		// 如果有一段时间没有使用了，则关闭
		if int(time.Now().Sub(affinity.lastUsed).Minutes()) >= state.affinity.ttlMinutes {
			glog.V(4).Infof("Removing client %s from affinityMap for service %q", affinity.clientIP, svcPort)
			delete(state.affinity.affinityMap, ip)
		}
	}
}
