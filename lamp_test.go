package lamp_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/xeitf/lamp"
)

func TestXxx(t *testing.T) {
	close, err := lamp.Init("etcd://127.0.0.1:2379/services")
	if err != nil {
		return
	}
	defer close()

	cancel, err := lamp.Register("user-svr",
		lamp.WithTTL(5),
		lamp.WithPublic("127.0.0.1:8999"),
		lamp.WithPublic("127.0.0.1:80", "http"),
	)
	if err != nil {
		return
	}
	defer cancel()

	cancel2, err := lamp.Register("user-svr",
		lamp.WithPublic("127.0.0.1:8990"),
	)
	if err != nil {
		return
	}
	defer cancel2()

	closeWatch, err := lamp.Watch("user-svr", "http", func(addrs []string, closed bool) {
		fmt.Printf("Watch: %+v %+v\n", addrs, closed)
	})
	if err != nil {
		return
	}
	defer closeWatch()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		wg.Done()

		for range time.Tick(time.Second * 2) {
			addrs, err := lamp.Discover("user-svr", "tcp")
			if err != nil {
				fmt.Printf("Discover: %s\n", err.Error())
			} else {
				fmt.Printf("Discover: %+v\n", addrs)
			}
		}
	}()

	wg.Wait()

	time.Sleep(20 * time.Second)

}
