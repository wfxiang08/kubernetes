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

package json

import (
	"encoding/json"
	"io"
	"testing"
	"time"

	"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/api/testapi"
	"k8s.io/kubernetes/pkg/runtime"
	"k8s.io/kubernetes/pkg/util/wait"
	"k8s.io/kubernetes/pkg/watch"
)

func TestDecoder(t *testing.T) {
	table := []watch.EventType{watch.Added, watch.Deleted, watch.Modified, watch.Error}

	for _, eventType := range table {
		out, in := io.Pipe()
		decoder := NewDecoder(out, testapi.Default.Codec())

		expect := &api.Pod{ObjectMeta: api.ObjectMeta{Name: "foo"}}

		encoder := json.NewEncoder(in)

		// 写入数据到: pipe in中
		go func() {
			data, err := runtime.Encode(testapi.Default.Codec(), expect)
			if err != nil {
				t.Fatalf("Unexpected error %v", err)
			}
			if err := encoder.Encode(&WatchEvent{eventType, runtime.RawExtension{RawJSON: json.RawMessage(data)}}); err != nil {
				t.Errorf("Unexpected error %v", err)
			}

			// 数据输出之后就关闭了
			in.Close()
		}()

		done := make(chan struct{})
		go func() {
			// 解码数据
			action, got, err := decoder.Decode()

			// 首先数据解码不出问题
			if err != nil {
				t.Fatalf("Unexpected error %v", err)
			}

			// 然后eventType和解码出来一致
			if e, a := eventType, action; e != a {
				t.Errorf("Expected %v, got %v", e, a)
			}

			// 所携带的数据一致
			if e, a := expect, got; !api.Semantic.DeepDerivative(e, a) {
				t.Errorf("Expected %v, got %v", e, a)
			}
			t.Logf("Exited read")
			close(done)
		}()

		// 第一个目标完成
		<-done

		// decode结束之后，就不要在读取数据了; 因为input 已经关闭了
		// 如果读取，就可能出现错误
		done = make(chan struct{})
		go func() {
			_, _, err := decoder.Decode()
			if err == nil {
				t.Errorf("Unexpected nil error")
			}
			close(done)
		}()
		<-done

		decoder.Close()
	}
}

func TestDecoder_SourceClose(t *testing.T) {
	out, in := io.Pipe()
	decoder := NewDecoder(out, testapi.Default.Codec())

	done := make(chan struct{})

	go func() {
		_, _, err := decoder.Decode()
		if err == nil {
			t.Errorf("Unexpected nil error")
		}
		close(done)
	}()

	// source close，然后立马关闭
	in.Close()

	// 不应该出现Timeout
	select {
	case <-done:
		break
	case <-time.After(wait.ForeverTestTimeout):
		t.Error("Timeout")
	}
}
